"""Shared pytest fixtures for the payroll_pipeline test suite.

Provides the single session-scoped `spark` fixture every tier (`unit`, `integration`,
`end_to_end`) runs against, so the JVM starts once per pytest run rather than once per test.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator["SparkSession"]:
    """Session-scoped local SparkSession shared by every test in this suite.

    Configures a local, `hadoop`-type Iceberg catalog against a session-scoped temp
    warehouse -- not the REST catalog Terraform configures for the deployed job. The
    catalog *name* (`glue_catalog`) matches production exactly, so a table identity
    written by a test is textually identical to one written by the deployed job; only
    `type` and `warehouse` differ between the two.

    `spark.jars.packages` pulls in the Iceberg Spark runtime JAR that defines the
    extension/catalog classes configured below -- without it, Spark cannot resolve
    `IcebergSparkSessionExtensions` and every test that requests this fixture fails at
    session creation, not at the assertion it's actually testing.

    The pyspark import is deferred to inside this fixture body (mirroring
    `pipeline/types.py`'s `to_spark_type`), so collecting this file -- and running
    any test that doesn't request `spark` -- never requires pyspark to be
    installed. `importorskip` skips, rather than errors, a test that does request
    it when pyspark is genuinely absent.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession as _SparkSession

    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    session = (
        _SparkSession.builder.appName("payroll_pipeline-tests")
        .master("local[2]")
        .enableHiveSupport()
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.type", "hadoop")
        .config("spark.sql.catalog.glue_catalog.warehouse", str(warehouse))
        .config("spark.sql.defaultCatalog", "glue_catalog")
        .getOrCreate()
    )
    yield session
    session.stop()
