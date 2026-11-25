from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from shapestream.common import RDF_TYPE
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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


    def apply_batch(self, batch_df:DataFrame) -> tuple[DataFrame, DataFrame]:

        # i find same triples to keep the latest change 
        latest_win = Window.partitionBy("s", "p", "o", "okind", "dt", "lang").orderBy(F.col("seq").desc())
        latest_events = batch_df.withColumn("rn", F.row_number().over(latest_win)).filter(F.col("rn") == 1).drop("rn")
        
        # first splitting the input for changes in temp buffers 
        type_events = latest_events.filter(latest_events["p"] == RDF_TYPE)
        triple_events = latest_events.filter(latest_events["p"] != RDF_TYPE)

        # i have both direct and indirect changes on the graph after getting the info from the batch 
        direct_sub = latest_events.select("s").distinct()
        indir_sub = self.data_triples.join(type_events, self.data_triples["o"] == type_events["s"], "inner").select(self.data_triples["s"]).distinct()

        touched = direct_sub.union(indir_sub).distinct()

        triples_bef = self.data_triples.join(touched, self.data_triples["s"] == touched["s"])
        types_bef = self.entity_types.join(touched, self.entity_types["s"] == touched["s"])
