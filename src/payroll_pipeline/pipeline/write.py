"""Shared Iceberg write-sink logic for every ETL job in this package."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    write_mode: str,
) -> None:
    """Write the final transformed, validated output to its Iceberg sink.

    The table must already exist (created by the generated catalog DDL, see
    infra/catalog/). This function never creates it: `writeTo(...).create()`
    would infer the table's shape from whatever the transformation emitted on
    its first run, making the table a function of generated code rather than
    of the declared contract.

    Args:
        spark: Active SparkSession from the scaffold-owned entrypoint.
        df: The final transformed DataFrame to persist.
        table: Two-part `{database}.{dataset}` identity, composed at runtime by
            `JobContext.table_identity` (never rendered at generation time,
            because the database name carries the deploy environment).
        write_mode: One of "append", "overwrite_partitions", "overwrite_all".
            Resolved at generation time from the job's Sink settings; this is a
            dispatch on an already-decided value, not a policy branch.

    Raises:
        ValueError: If `write_mode` is not one of the three accepted values.
    """
    writer = df.writeTo(table)
    if write_mode == "append":
        writer.append()
    elif write_mode == "overwrite_partitions":
        writer.overwritePartitions()
    elif write_mode == "overwrite_all":
        # overwrite(F.lit(True)) rather than replace(): replace() redefines the
        # table from the DataFrame's schema, which is the create-on-write failure
        # mode this function exists to avoid, by another name.
        writer.overwrite(F.lit(True))
    else:
        raise ValueError(f"unknown write_mode {write_mode!r}")
