from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from shapestream.common import RDF_TYPE
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from engine.contracts import TouchedSlice
from pathlib import Path
import shutil
import tempfile

class SparkState:
    def __init__(self, spark: SparkSession, state_dir: str | None = None, num_buckets: int = 16) -> None:
        if not isinstance(spark, SparkSession):
            raise TypeError("Expected valid instance of SparkSession")
        if num_buckets < 1:
            raise ValueError("num_buckets must be >= 1")

        self.session = spark
        self._temp_dir = None

        if state_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="shapestream_")
            state_dir = self._temp_dir.name

        self.state_dir = state_dir

        self.state_version = 0
        self.num_buckets = num_buckets

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
        latest_events = latest_events.persist()

        try:
        
            # first splitting the input for changes in temp buffers 
            type_events = latest_events.filter((F.col("p") == RDF_TYPE) & (F.col("okind") == "iri"))
            triple_events = latest_events.filter(F.col("p") != RDF_TYPE)

            type_s_buckets, triple_s_buckets, triple_o_buckets = self._event_buckets(latest_events)

            # i have both direct and indirect changes on the graph after getting the info from the batch
            direct_sub = latest_events.select("s").distinct()
            reverse_subset = self._read_buckets("reverse_refs", type_s_buckets, self.reverse_refs_schema)
            indir_sub = reverse_subset.join(type_events, reverse_subset["o"] == type_events["s"], how="inner").select(reverse_subset["s"]).distinct()
            touched = direct_sub.union(indir_sub).distinct()
            touched = touched.localCheckpoint(eager=True) 

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

            self._update_buckets("entity_types", "s", type_s_buckets, type_add, type_del, self.entity_types_schema)

            # here i do the same in data_triples
            triple_add = triple_events.filter(triple_events["op"] == "+").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))
            triple_del = triple_events.filter(triple_events["op"] == "-").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))

            reverse_add = triple_add.filter(F.col("okind") == "iri").select("o", "s", "p", "dt", "lang")
            reverse_del = triple_del.filter(F.col("okind") == "iri").select("o", "s", "p", "dt", "lang")
            
            self._update_buckets(
                "reverse_refs",
                "o",
                triple_o_buckets,
                reverse_add,
                reverse_del,
                self.reverse_refs_schema
            )

            self._update_buckets(
                "data_triples",
                "s",
                triple_s_buckets,
                triple_add,
                triple_del,
                self.data_triples_schema
            )
            
            data_subset_aft = self._read_buckets("data_triples", touched_buckets, self.data_triples_schema)

            types_subset_aft = self._read_buckets("entity_types", touched_buckets, self.entity_types_schema)

            triples_aft = touched.join(data_subset_aft, on="s", how="inner")
            types_aft = touched.join(types_subset_aft, on="s", how="inner")

            iri_triples_aft = triples_aft.filter(F.col("okind") == "iri")

            # objects after the batch = objects before + objects of inserted IRI triples, so this
            # superset is correct without another collect (the join on o filters the extras)
            obj_buckets_aft = sorted(set(obj_buckets_bef) | set(triple_o_buckets))

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

        finally:
            latest_events.unpersist()


    
    ###################
    # psilo prive
    ##################
    def _bucket_expr(self, col):
        return F.pmod(F.xxhash64(col), F.lit(self.num_buckets)).cast("int")


    def _with_bucket(self, df: DataFrame, key: str) -> DataFrame:
        return df.withColumn("bucket", self._bucket_expr(F.col(key)))


    def _event_buckets(self, events: DataFrame) -> tuple[list[int], list[int], list[int]]:
        is_type = (F.col("p") == RDF_TYPE) & (F.col("okind") == "iri")
        is_triple = F.col("p") != RDF_TYPE
        rows = events.select(
            F.when(is_type, self._bucket_expr(F.col("s"))).alias("type_s"),
            F.when(is_triple, self._bucket_expr(F.col("s"))).alias("triple_s"),
            F.when(is_triple & (F.col("okind") == "iri"), self._bucket_expr(F.col("o"))).alias("triple_o"),
        ).distinct().collect()

        def pick(name: str) -> list[int]:
            return sorted({r[name] for r in rows if r[name] is not None})

        return pick("type_s"), pick("triple_s"), pick("triple_o")

    
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


    def _update_buckets(self, table: str, key: str, affected: list[int], additions: DataFrame, deletions: DataFrame, schema: StructType) -> None:
        columns = schema.fieldNames()

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
        """Remove obsolete bucket versions.

        Must be called after aggregator.update(before_slice, after_slice)
        has completed successfully and the old slices are no longer needed.
        """
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
    