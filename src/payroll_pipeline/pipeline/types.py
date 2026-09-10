"""Canonical column-type vocabulary for this project's generated code.

Vendored from engineering-automation's `core/shared/types.py` so this project's own
test fixtures (`tests/_schema_fixtures.py`) don't need a dependency on the scaffolding
tool itself. Do not edit by hand; edit the source in engineering-automation and re-run
`pipeline-automator scaffold` to pick up the change. Kept byte-for-byte identical to the
source below the module docstring -- see engineering-automation's
`tests/unit/test_vendored_types_matches_source.py`.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import DataType

_ALIASES: dict[str, str] = {"integer": "int", "long": "bigint"}
_SIMPLE_TYPES: frozenset[str] = frozenset(
    {"string", "boolean", "int", "bigint", "double", "float", "date", "timestamp"}
)
_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

_VOCABULARY_DESCRIPTION = (
    "string, boolean, int (or integer), bigint (or long), double, float, date, "
    "timestamp, decimal(p,s)"
)


def normalize(declared: str) -> str:
    """Canonical form for comparison -- 'bigint' and 'long' are the same type.

    Args:
        declared: A YAML-declared column type string, e.g. "long" or "decimal(14, 2)".

    Returns:
        The lowercased, alias-resolved, whitespace-collapsed canonical form. Simple types
        return their canonical name; decimal returns "decimal(p,s)" with no inner spaces.

    Raises:
        ValueError: If `declared` matches neither a known simple type nor the
            `decimal(p,s)` pattern. Names the declared type and the accepted vocabulary;
            callers with column context (e.g. a future schema-artefact renderer) should
            wrap this error to add the column name -- this function has no column-name
            parameter by design.
    """
    token = declared.strip().lower()
    match = _DECIMAL_RE.match(token)
    if match:
        return f"decimal({match.group(1)},{match.group(2)})"
    token = _ALIASES.get(token, token)
    if token in _SIMPLE_TYPES:
        return token
    raise ValueError(
        f"Unknown declared type {declared!r}. Accepted vocabulary: {_VOCABULARY_DESCRIPTION}."
    )


def to_iceberg_sql_type(declared: str) -> str:
    """Map a declared type string to its Iceberg SQL DDL type name.

    Args:
        declared: A YAML-declared column type string.

    Returns:
        The uppercase Iceberg SQL type, e.g. "BIGINT", "DECIMAL(14,2)".

    Raises:
        ValueError: See `normalize`.
    """
    canonical = normalize(declared)
    match = _DECIMAL_RE.match(canonical)
    if match:
        return f"DECIMAL({match.group(1)},{match.group(2)})"
    return canonical.upper()


def to_spark_type(declared: str) -> DataType:
    """Map a declared type string to its `pyspark.sql.types.DataType` instance.

    Requires pyspark to be installed at call time (see module docstring); rejection of an
    unknown `declared` happens before the pyspark import, so calling this with an invalid
    type string never requires pyspark to be present.

    Args:
        declared: A YAML-declared column type string.

    Returns:
        The corresponding `DataType` instance (a `DecimalType(p, s)` for `decimal(p,s)`).

    Raises:
        ValueError: See `normalize`.
        ModuleNotFoundError: If pyspark is not installed.
    """
    canonical = normalize(declared)  # validates and rejects before importing pyspark

    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    match = _DECIMAL_RE.match(canonical)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))

    simple_map: dict[str, DataType] = {
        "string": StringType(),
        "boolean": BooleanType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    return simple_map[canonical]
