"""Scaffold-owned smoke test: the package imports and declares a version.

Replaced by real transformation tests as jobs are generated. Its job is to keep
`pytest -m unit` from exiting 5 (no tests collected) on a freshly scaffolded
project, and to catch a broken package __init__ or a failed editable install.
"""

import pytest

import payroll_pipeline


@pytest.mark.unit
def test_package_imports_and_declares_a_version() -> None:
    """The package is importable and exposes a non-empty __version__."""
    assert payroll_pipeline.__version__
