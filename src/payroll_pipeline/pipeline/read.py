"""Shared per-source DataFrame reader for every ETL job in this package."""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from ..runtime.job_settings import SourceConfig


def _window_bounds(
    column_type: str | None, start_date: str, end_date: str
) -> tuple[Column, Column]:
    """Build the window filter's [start, end) bounds, cast to the column's declared type.

    A bare string comparison is only correct against a `yyyy-MM-dd` string column --
    against a `date` column Spark coerces, and against a `timestamp` it truncates, so
    the comparison must be built with the same type the column actually holds.

    Args:
        column_type: The partition column's declared type -- `"date"`, `"timestamp"`,
            `"string"`, or `None` (treated the same as `"string"`; `read()` only calls
            this when a partition column is set, and generation time never bakes in a
            type this function doesn't handle -- see `render.py`'s ambiguity check).
        start_date: Inclusive lower bound (ISO date string).
        end_date: Exclusive upper bound (ISO date string).

    Returns:
        The (start, end) bound expressions to compare the partition column against.
    """
    if column_type == "date":
        return F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date))
    if column_type == "timestamp":
        return F.to_timestamp(F.lit(start_date)), F.to_timestamp(F.lit(end_date))
    return F.lit(start_date), F.lit(end_date)


def _check_and_prune_columns(df: DataFrame, src: SourceConfig) -> DataFrame:
    """Compare the read frame's schema against the source's declared columns, then prune.

    Strictness is asymmetric, and not the same as `pipeline/validate.py`'s check on this
    job's own output: for an input, a missing declared column is fatal (a transformation
    may depend on it) and a retyped declared column is fatal, but an extra undeclared
    column is fine -- upstream added something this job doesn't consume. Pruning to
    exactly the declared columns runs only once no drift is found, so a missing column is
    reported as drift here rather than surfacing later as a `.select()` failure.

    Args:
        df: The DataFrame just read from `src.table_identity`, before any window filter
            has narrowed its row set (schema is unaffected by the filter either way).
        src: The source whose `columns` (declared as `"name:type"` entries) to check
            against. Only called when `src.columns` is non-empty.

    Returns:
        `df` pruned to exactly `src.columns`' column names, in declared order.

    Raises:
        ValueError: If a declared column is missing from `df`, or a shared column's
            observed type doesn't match its declared type. Names the source contract's
            contact for an external source; points at the regenerating job for an owned
            one (`src.layer is not None`, since only an owned source resolves through
            `JobContext.table_identity`).
    """
    declared = dict(entry.split(":", 1) for entry in src.columns)
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    missing = sorted(set(declared) - set(actual))
    mismatched = sorted(
        f"{name}: declared={declared[name]} observed={actual[name]}"
        for name in set(declared) & set(actual)
        if declared[name] != actual[name]
    )

    if missing or mismatched:
        detail = f"missing {missing}, type mismatches {mismatched}"
        if src.layer is not None:
            remedy = (
                f"which is generated from job '{src.dataset}'. "
                f"Re-run 'generate create-job {src.dataset}'."
            )
        else:
            remedy = f"Contact {src.contact}."
        raise ValueError(
            f"source '{src.table_identity}' (alias '{src.alias}') does not match its "
            f"contract -- {detail}. {remedy}"
        )

    return df.select(*declared)


def read(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    sources_config: tuple[SourceConfig, ...],
) -> dict[str, DataFrame]:
    """Read source DataFrames, apply a half-open [start, end) window filter, and fail
    fast on an empty windowed read unless allow_empty is set.

    `sources_config` is baked in at generation time by the scaffold render step and
    carries one entry per source; `run()` resolves each entry's final
    `table_identity` before calling this function (see `SourceConfig`).

    Args:
        spark: Active SparkSession from the scaffold-owned entrypoint.
        start_date: Inclusive lower bound of the processing window (ISO date string).
        end_date: Exclusive upper bound of the processing window (ISO date string).
        sources_config: One `SourceConfig` per source; see that class for the keys.

    Returns:
        Dict mapping each source alias to its filtered, column-pruned DataFrame.

    Raises:
        ValueError: If any source returns no rows and allow_empty is False; if a source's
            `table_identity` was never resolved before reaching this function; or if a
            source's declared columns don't match what was actually read (see
            `_check_and_prune_columns`).
    """
    sources: dict[str, DataFrame] = {}
    for src in sources_config:
        if src.table_identity is None:
            raise ValueError(
                f"source '{src.alias}' has no resolved table_identity; "
                "run() must resolve it before calling read()"
            )
        df = spark.read.table(src.table_identity)
        if src.partition_column is not None:
            start_bound, end_bound = _window_bounds(src.partition_column_type, start_date, end_date)
            df = df.filter(
                (F.col(src.partition_column) >= start_bound)
                & (F.col(src.partition_column) < end_bound)
            )
        if src.columns:
            df = _check_and_prune_columns(df, src)
        if src.partition_column is not None:
            # Only worth caching when a window filter narrowed the read: the emptiness
            # probe below and the later real use would otherwise each rescan the same
            # one window of data. Without a filter this would cache a full table read,
            # which costs more Glue-worker memory than re-reading a pruned Parquet scan.
            df = df.cache()
        if not src.allow_empty and not df.take(1):
            raise ValueError(
                f"source '{src.alias}' returned no rows for window "
                f"[{start_date}, {end_date}); set allow_empty: true to permit this"
            )
        sources[src.alias] = df
    return sources
