"""Shared ETL job orchestrator: read → transform → write for every job in this package.

Spark is lazy: an analysis failure such as an unknown column surfaces during
the `transform` phase because Spark analyzes the plan eagerly, but a
data-dependent failure (a bad value, a type mismatch in the actual data) only
surfaces at the first action, inside `validate` or `write`. A phase marker on
the emitted run record names where Spark *noticed* the problem, which is not
always where the problem was actually written.
"""

import dataclasses
from collections.abc import Callable
from typing import cast

from pyspark.sql import DataFrame

from ..pipeline.read import read as _read
from ..pipeline.validate import ValidationFailed, validate
from ..pipeline.write import write as _write
from .job_settings import JobConfig, JobContext, RunRecord, phase


def run(
    ctx: JobContext,
    record: RunRecord,
    config: JobConfig,
    *,
    transform_fn: Callable[[dict[str, DataFrame]], DataFrame],
) -> None:
    """Orchestrate read → transform → validate → write for one ETL job.

    Args:
        ctx: Resolved runtime inputs (SparkSession, processing window, Glue
            identifiers) for this execution, built by `job_run()`.
        record: The mutable run record for this execution; each stage below is
            wrapped in `phase()` so the record carries a per-stage timing
            breakdown and names the stage executing if one of them fails.
        config: The job's generation-time-fixed configuration (sources, sink,
            contract package/name) -- everything about the job that is
            identical across every run of it.
        transform_fn: The per-job transform function (injected at call time by
            the per-job run() wrapper so this shared function stays job-agnostic).

    Raises:
        ValidationFailed: If `validate()` reports any check as failed. Raised only
            after the result is recorded on `record.metrics["validation"]`, so a
            failed run still produces a structured record naming every check
            rather than a stack trace naming one.
    """
    table = ctx.table_identity(config.sink.layer, config.sink.dataset)
    with phase(record, "read"):
        # An owned source's table_identity is unresolved at generation time (its
        # database name carries the deploy environment, a runtime value) -- resolve it
        # here via JobContext before read() ever sees it. An external source's
        # table_identity is already resolved verbatim; leave it untouched.
        resolved_sources = tuple(
            dataclasses.replace(
                src,
                table_identity=src.table_identity
                or ctx.table_identity(cast(str, src.layer), cast(str, src.dataset)),
            )
            for src in config.sources
        )
        sources = _read(ctx.spark, ctx.start_date, ctx.end_date, resolved_sources)
    with phase(record, "transform"):
        output = transform_fn(sources)
    with phase(record, "validate"):
        output = output.cache()
        result = validate(
            ctx.spark,
            output,
            table=table,
            contract_package=config.contract_package,
            contract_name=config.contract_name,
        )
        record.metrics["validation"] = result.as_record()
        if not result.ok:
            raise ValidationFailed(result)
    with phase(record, "write"):
        _write(ctx.spark, output, table=table, write_mode=config.sink.write_mode)
    output.unpersist()
    # A source is only actually cached (by read()) when a window filter narrowed it;
    # unpersist() is a safe no-op on a frame that was never cached, so this cleans up
    # whichever sources were cached without read() needing to report which ones back.
    for source_df in sources.values():
        source_df.unpersist()
