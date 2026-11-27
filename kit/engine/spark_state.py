from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from shapestream.common import RDF_TYPE
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from engine.contracts import TouchedSlice
from pathlib import Path
import shutil

class SparkState:
    def __init__(self, spark: SparkSession, state_dir: str) -> None:        
        if not isinstance(spark, SparkSession):
            raise TypeError("Expected valid instance of SparkSession")
        
        self.session = spark
        self.state_dir = state_dir
        self.state_version = 0
        self.num_buckets = 64

        self.bucket_paths = {
            "entity_types": {},
            "data_triples": {},
            "reverse_refs": {}
        }

        state_path = Path(self.state_dir)
        state_path.mkdir(parents=True, exist_ok=True)

        for folder in state_path.glob("batch_*"):
            if folder.is_dir() and folder.name[6:].isdigit():
                shutil.rmtree(folder)
        # SCHEMAS
        self.entity_types_schema = StructType([
                                        StructField("s", StringType(), False),
                                        StructField("cls", StringType(), False)
        ])

        self.data_triples_schema = StructType([
                                        StructField("s", StringType(), False),
                                        StructField("p", StringType(), False),
                                        StructField("o", StringType(), False),
                                        StructField("okind", StringType(), False),
                                        StructField("dt", StringType(), True),
                                        StructField("lang", StringType(), True),
        ])

        self.reverse_refs_schema = StructType([
                                        StructField("o", StringType(), False),
                                        StructField("s", StringType(), False),
                                        StructField("p", StringType(), False),
                                        StructField("dt", StringType(), True),
                                        StructField("lang", StringType(), True)
        ])

        
    @property
    def entity_types(self) -> DataFrame:
        return self._read_buckets(
            "entity_types",
            self.bucket_paths["entity_types"].keys(),
            self.entity_types_schema
        )

    @property
    def data_triples(self) -> DataFrame:
        return self._read_buckets(
            "data_triples",
            self.bucket_paths["data_triples"].keys(),
            self.data_triples_schema
        )

    @property
    def reverse_refs(self) -> DataFrame:
        return self._read_buckets(
            "reverse_refs",
            self.bucket_paths["reverse_refs"].keys(),
            self.reverse_refs_schema
        )


    def apply_batch(self, batch_df:DataFrame) -> tuple[TouchedSlice, TouchedSlice]:

        # i find same triples to keep the latest change 
        latest_win = Window.partitionBy("s", "p", "o", "okind", "dt", "lang").orderBy(F.col("seq").desc())
        latest_events = batch_df.withColumn("rn", F.row_number().over(latest_win)).filter(F.col("rn") == 1).drop("rn")
        
        # first splitting the input for changes in temp buffers 
        type_events = latest_events.filter((F.col("p") == RDF_TYPE) & (F.col("okind") == "iri"))
        triple_events = latest_events.filter(F.col("p") != RDF_TYPE)

        # i have both direct and indirect changes on the graph after getting the info from the batch 
        direct_sub = latest_events.select("s").distinct()
        affected_buckets = self._get_bucket_ids(type_events, "s")
        reverse_subset = self._read_buckets("reverse_refs", affected_buckets, self.reverse_refs_schema)
        indir_sub = reverse_subset.join(type_events, reverse_subset["o"] == type_events["s"], how="inner").select(reverse_subset["s"]).distinct()
        touched = direct_sub.union(indir_sub).distinct()

        touched_buckets = self._get_bucket_ids(touched, "s")
        data_subset = self._read_buckets("data_triples", touched_buckets, self.data_triples_schema)
        types_subset = self._read_buckets("entity_types", touched_buckets, self.entity_types_schema)
        triples_bef = touched.join(data_subset, on="s", how="inner")
        types_bef = touched.join(types_subset, on="s", how="inner")

        iri_triples_bef = triples_bef.filter(F.col("okind") == "iri")
        obj_buckets_bef = self._get_bucket_ids(iri_triples_bef, "o")
        obj_types_subset = self._read_buckets("entity_types", obj_buckets_bef, self.entity_types_schema)
        obj_types_bef = iri_triples_bef.join(obj_types_subset, iri_triples_bef["o"] == obj_types_subset["s"], how="inner").select(iri_triples_bef["o"], obj_types_subset["cls"]).distinct()
        
        bef_slice = TouchedSlice(types_df=types_bef, data_df=triples_bef, obj_types_df=obj_types_bef)

        # now i add/ del on kg first on types
        type_add = type_events.filter(type_events["op"] == "+").select(F.col("s"), F.col("o").alias("cls"))
        type_del = type_events.filter(type_events["op"] == "-").select(F.col("s"), F.col("o").alias("cls"))

        self._update_buckets("entity_types", "s", type_add, type_del, self.entity_types_schema)

        # here i do the same in data_triples
        triple_add = triple_events.filter(triple_events["op"] == "+").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))
        triple_del = triple_events.filter(triple_events["op"] == "-").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))

        reverse_add = triple_add.filter(F.col("okind") == "iri").select("o", "s", "p", "dt", "lang")
        reverse_del = triple_del.filter(F.col("okind") == "iri").select("o", "s", "p", "dt", "lang")
        
        self._update_buckets(
            "reverse_refs",
            "o",
            reverse_add,
            reverse_del,
            self.reverse_refs_schema
        )

        self._update_buckets(
            "data_triples",
            "s",
            triple_add,
            triple_del,
            self.data_triples_schema
        )
        
        data_subset_aft = self._read_buckets("data_triples", touched_buckets, self.data_triples_schema)

        types_subset_aft = self._read_buckets("entity_types", touched_buckets, self.entity_types_schema)

        triples_aft = touched.join(data_subset_aft, on="s", how="inner")
        types_aft = touched.join(types_subset_aft, on="s", how="inner")

        iri_triples_aft = triples_aft.filter(F.col("okind") == "iri")

        obj_buckets_aft = self._get_bucket_ids(iri_triples_aft, "o")

        obj_types_subset_aft = self._read_buckets("entity_types", obj_buckets_aft, self.entity_types_schema)

        obj_types_aft = iri_triples_aft.join(obj_types_subset_aft, iri_triples_aft["o"] == obj_types_subset_aft["s"], how="inner").select(iri_triples_aft["o"], obj_types_subset_aft["cls"]).distinct()

        self.state_version += 1
        
        aft_slice = TouchedSlice(
                                types_df= types_aft,
                                data_df= triples_aft,
                                obj_types_df= obj_types_aft
        )

        result = (bef_slice, aft_slice)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(x, TouchedSlice) for x in result)
        
        return result


    
    ###################
    # psilo prive
    ##################
    def _with_bucket(self, df: DataFrame, key: str) -> DataFrame:
        return df.withColumn(
            "bucket",
            F.pmod(
                F.xxhash64(F.col(key)),
                F.lit(self.num_buckets)
            ).cast("int")
        )

    
    def _read_buckets(self, table: str, bucket_ids, schema: StructType) -> DataFrame:
        paths = [
            self.bucket_paths[table][b]
            for b in bucket_ids
            if b in self.bucket_paths[table]
        ]

        if not paths:
            return self.session.createDataFrame([], schema)

        return self.session.read.schema(schema).parquet(*paths)


    def _get_bucket_ids(self, df: DataFrame, key: str) -> list[int]:
        return [
            row["bucket"]
            for row in self._with_bucket(df, key)
            .select("bucket")
            .distinct()
            .collect()
        ]


    def _update_buckets(self, table: str, key: str, additions: DataFrame,deletions: DataFrame, schema: StructType) -> None:

        columns = schema.fieldNames()

        affected = self._get_bucket_ids(additions.select(key).unionByName(deletions.select(key)),key)

        if not affected:
            return

        current = self._read_buckets(table, affected, schema).select(*columns)

        updated = current.subtract(deletions.select(*columns))
        updated = updated.unionByName(additions.select(*columns)).distinct()

        path = f"{self.state_dir.rstrip('/')}/batch_{self.state_version}/{table}"

        self._with_bucket(updated, key).write.mode("overwrite").partitionBy("bucket").parquet(path)

        for bucket_id in affected:
            bucket_path = f"{path}/bucket={bucket_id}"

            if Path(bucket_path).is_dir():
                self.bucket_paths[table][bucket_id] = bucket_path
            else:
                self.bucket_paths[table].pop(bucket_id, None)


    ###################
    # public
    ###################
    def cleanup(self) -> None:
        active_paths = {
            Path(path)
            for table in self.bucket_paths.values()
            for path in table.values()
        }

        state_path = Path(self.state_dir)

        for folder in state_path.glob("batch_*"):
            if folder.is_dir() and folder.name[6:].isdigit():

                for bucket in folder.glob("*/bucket=*"):
                    if bucket.is_dir() and bucket not in active_paths:
                        shutil.rmtree(bucket)

                if not any(folder.glob("*/bucket=*")):
                    shutil.rmtree(folder)


    def state_bytes(self) -> int:
        return sum(
            file.stat().st_size
            for table in self.bucket_paths.values()
            for path in table.values()
            for file in Path(path).rglob("*.parquet")
            if file.is_file()
        )
    