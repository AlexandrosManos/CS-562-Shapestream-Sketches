"""Shared schemas and interfaces across ShapeStream engine modules."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Protocol, Tuple, Union

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import types as T

    # Active rdf:type pairs for touched subjects: (s, cls)
    TYPES_SCHEMA = T.StructType([
        T.StructField("s", T.StringType(), nullable=False),
        T.StructField("cls", T.StringType(), nullable=False),
    ])

    # Active non-type triples for touched subjects: matches shapestream.spark_io.TRIPLE_SCHEMA
    # Note: dt and lang are nullable; use eqNullSafe (<=>) when joining on triple identity.
    DATA_SCHEMA = T.StructType([
        T.StructField("s", T.StringType(), nullable=False),
        T.StructField("p", T.StringType(), nullable=False),
        T.StructField("o", T.StringType(), nullable=False),
        T.StructField("okind", T.StringType(), nullable=False),
        T.StructField("dt", T.StringType(), nullable=True),
        T.StructField("lang", T.StringType(), nullable=True),
    ])

    # Active rdf:type pairs for IRI objects referenced in DATA_SCHEMA: (o, cls)
    OBJ_TYPES_SCHEMA = T.StructType([
        T.StructField("o", T.StringType(), nullable=False),
        T.StructField("cls", T.StringType(), nullable=False),
    ])
except ImportError:
    DataFrame = object
    TYPES_SCHEMA = DATA_SCHEMA = OBJ_TYPES_SCHEMA = None

# Output types matching shapestream.common.write_stats
Number = Union[int, float]
ClassesDict = Dict[str, Number]
PropertiesDict = Dict[Tuple[str, str], Tuple[Number, int, int]]
ConstraintsDict = Dict[Tuple[str, str, str], Number]
StatsTuple = Tuple[ClassesDict, PropertiesDict, ConstraintsDict]


@dataclass
class TouchedSlice:
    """Subgraph view (before or after a batch) restricted to touched_subjects.

    touched_subjects MUST be the union of:
      1. Direct subjects s appearing in batch_df, AND
      2. Reverse-lookup subjects s with an active (s, p, o) triple whose IRI
         object o gained or lost an rdf:type in batch_df (Hard Case 3).
    """
    types_df: DataFrame
    data_df: DataFrame
    obj_types_df: DataFrame


class BaseEngine(Protocol):
    """Common engine interface used by streaming_runner.py."""

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Apply one single-epoch micro-batch of EVENT_SCHEMA events (op in {'+', '-'})."""
        ...

    def on_epoch_end(self, epoch: int) -> None:
        """Optional hook called once all events of `epoch` have been processed."""
        ...

    def current_stats(self) -> StatsTuple:
        """Return (classes, properties, constraints) for write_stats()."""
        ...

    def state_mb(self) -> float:
        """Return current state size in MB (Spark tables + driver summaries)."""
        ...
