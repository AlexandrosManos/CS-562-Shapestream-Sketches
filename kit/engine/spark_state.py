from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from shapestream.common import RDF_TYPE
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from engine.contracts import TouchedSlice

class SparkState:
    def __init__(self, spark:SparkSession) -> None:
        if not isinstance(spark, SparkSession):
            raise TypeError("Expected valid instance of SparkSession")
        
        self.session = spark
        
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
        
        # empty dataframes from schemas
        self.entity_types = self.session.createDataFrame([], self.entity_types_schema)
        self.data_triples = self.session.createDataFrame([], self.data_triples_schema)


    def apply_batch(self, batch_df:DataFrame) -> tuple[TouchedSlice, TouchedSlice]:

        # i find same triples to keep the latest change 
        latest_win = Window.partitionBy("s", "p", "o", "okind", "dt", "lang").orderBy(F.col("seq").desc())
        latest_events = batch_df.withColumn("rn", F.row_number().over(latest_win)).filter(F.col("rn") == 1).drop("rn")
        
        # first splitting the input for changes in temp buffers 
        type_events = latest_events.filter(latest_events["p"] == RDF_TYPE)
        triple_events = latest_events.filter(latest_events["p"] != RDF_TYPE)

        # i have both direct and indirect changes on the graph after getting the info from the batch 
        direct_sub = latest_events.select("s").distinct()
        indir_sub = self.data_triples.join(type_events, (self.data_triples["o"] == type_events["s"]) & 
                                                        (self.data_triples["okind"] == "iri"), "inner").select(self.data_triples["s"]).distinct()

        touched = direct_sub.union(indir_sub).distinct()

        triples_bef = touched.join(self.data_triples, on="s", how="inner")
        types_bef = touched.join(self.entity_types, on="s", how="inner")
        obj_types_bef = triples_bef.filter(triples_bef["okind"] == "iri").join(self.entity_types, triples_bef["o"] == self.entity_types["s"], how="inner").select(triples_bef["o"], self.entity_types["cls"]).distinct()

        bef_slice = TouchedSlice(types_df=types_bef, data_df=triples_bef, obj_types_df=obj_types_bef)

        # now i add/ del on kg first on types
        type_add = type_events.filter(type_events["op"] == "+").select(F.col("s"), F.col("o").alias("cls"))
        type_del = type_events.filter(type_events["op"] == "-").select(F.col("s"), F.col("o").alias("cls"))

        # anti left join to remove everything in type_del from self.entity_types
        self.entity_types = self.entity_types.join(type_del, (self.entity_types["s"] == type_del["s"]) & (self.entity_types["cls"] == type_del["cls"]), how="left_anti") 

        # unionbyname to add everything in type_del to self.entity_types
        self.entity_types = self.entity_types.unionByName(type_add).distinct()

        # here i do the same in data_triples
        triple_add = triple_events.filter(triple_events["op"] == "+").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))
        triple_del = triple_events.filter(triple_events["op"] == "-").select(F.col("s"), F.col("p"), F.col("o"), F.col("okind"), F.col("dt"), F.col("lang"))

        self.data_triples = self.data_triples.join(triple_del,    (self.data_triples["s"] == triple_del["s"]) & 
                                                                (self.data_triples["p"] == triple_del["p"]) &
                                                                (self.data_triples["o"] == triple_del["o"]) &
                                                                (self.data_triples["okind"] == triple_del["okind"]) &
                                                                (self.data_triples["dt"].eqNullSafe(triple_del["dt"])) &
                                                                (self.data_triples["lang"].eqNullSafe(triple_del["lang"])), how="left_anti")  
        
        
        self.data_triples = self.data_triples.unionByName(triple_add).distinct()

        triples_aft = touched.join(self.data_triples, on="s", how="inner")
        types_aft = touched.join(self.entity_types, on="s", how="inner")
        obj_types_aft = triples_aft.filter(triples_aft["okind"] == "iri").join(self.entity_types, triples_aft["o"] == self.entity_types["s"], how="inner").select(triples_aft["o"], self.entity_types["cls"]).distinct()

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