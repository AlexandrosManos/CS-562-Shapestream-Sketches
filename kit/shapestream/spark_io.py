"""PySpark helpers: session, N-Triples loading, snapshot schema."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .common import NT_REGEX, OBJ_LIT, RDF_LANGSTRING, XSD_STRING

TRIPLE_SCHEMA = T.StructType([
    T.StructField("s", T.StringType()),
    T.StructField("p", T.StringType()),
    T.StructField("o", T.StringType()),
    T.StructField("okind", T.StringType()),
    T.StructField("dt", T.StringType()),
    T.StructField("lang", T.StringType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("seq", T.LongType()),
    T.StructField("epoch", T.IntegerType()),
    T.StructField("op", T.StringType()),
    T.StructField("s", T.StringType()),
    T.StructField("p", T.StringType()),
    T.StructField("o", T.StringType()),
    T.StructField("okind", T.StringType()),
    T.StructField("dt", T.StringType()),
    T.StructField("lang", T.StringType()),
])


def get_spark(app: str = "shapestream", driver_memory: str | None = None) -> SparkSession:
    b = SparkSession.builder.appName(app)
    if driver_memory:
        b = b.config("spark.driver.memory", driver_memory)
    # the course image sets 8 shuffle partitions; keep that default elsewhere too
    b = b.config("spark.sql.shuffle.partitions", "8")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_nt(df_lines: DataFrame, col: str = "value") -> DataFrame:
    """Lines of N-Triples -> (s, p, o, okind, dt, lang). Malformed lines get s = ''."""
    x = df_lines.select(
        F.regexp_extract(col, NT_REGEX, 1).alias("s_raw"),
        F.regexp_extract(col, NT_REGEX, 2).alias("p"),
        F.regexp_extract(col, NT_REGEX, 3).alias("o_raw"),
    )
    s = F.when(F.col("s_raw").startswith("<"), F.expr("substring(s_raw, 2, length(s_raw) - 2)")) \
         .otherwise(F.col("s_raw"))
    is_iri = F.col("o_raw").startswith("<")
    is_bn = F.col("o_raw").startswith("_:")
    lit_val = F.regexp_extract("o_raw", OBJ_LIT, 1)
    lit_lang = F.regexp_extract("o_raw", OBJ_LIT, 2)
    lit_dt = F.regexp_extract("o_raw", OBJ_LIT, 3)
    return x.select(
        s.alias("s"),
        F.col("p"),
        F.when(is_iri, F.expr("substring(o_raw, 2, length(o_raw) - 2)"))
         .when(is_bn, F.col("o_raw")).otherwise(lit_val).alias("o"),
        F.when(is_iri, F.lit("iri")).when(is_bn, F.lit("bnode")).otherwise(F.lit("literal")).alias("okind"),
        F.when(is_iri | is_bn, F.lit(None).cast("string"))
         .when(lit_lang != "", F.lit(RDF_LANGSTRING))
         .when(lit_dt != "", lit_dt).otherwise(F.lit(XSD_STRING)).alias("dt"),
        F.when(~is_iri & ~is_bn & (lit_lang != ""), lit_lang).otherwise(F.lit(None).cast("string")).alias("lang"),
    )


def load_nt(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read N-Triples files (.nt, .ttl, also .bz2/.gz) into the triple schema.

    Comment and empty lines are skipped. Malformed lines come back with s == ''
    so that the caller can count them; filter them with F.col('s') != ''.
    """
    lines = spark.read.text(paths)
    lines = lines.filter(~F.col("value").rlike(r"^\s*(#.*)?$"))
    return parse_nt(lines)
