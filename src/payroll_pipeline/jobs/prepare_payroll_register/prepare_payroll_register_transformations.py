from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_unique_key(payroll_register_raw: DataFrame) -> DataFrame:
    """Creates an MD5 hashed unique key from employee_id, pay_run_id, and lohnart_code.

    Adds a payroll_register_key column to the DataFrame by concatenating
    employee_id, pay_run_id, and lohnart_code and hashing the result with MD5,
    intended for use in deduplication of the payroll register dataset.

    Args:
        payroll_register_raw: Raw payroll register DataFrame containing employee
            and pay run information.

    Returns:
        DataFrame with the same schema as the input plus a payroll_register_key
        column containing the MD5 hash of the concatenated key fields.
    """
    return payroll_register_raw.withColumn(
        "payroll_register_key",
        F.md5(F.concat(F.col("employee_id"), F.col("pay_run_id"), F.col("lohnart_code"))),
    )


def cast_data_types(create_unique_key_result: DataFrame) -> DataFrame:
    """Cast specific columns to their target data types.

    Casts gl_account and lohnart_code to integer type and pay_date to date type,
    while preserving all other columns and their existing types.

    Args:
        create_unique_key_result: Input DataFrame containing payroll data with
            gl_account, lohnart_code as strings and pay_date as a string.

    Returns:
        DataFrame with gl_account and lohnart_code cast to integer (as string),
        and pay_date cast to date type, with all other columns unchanged.
    """
    return (
        create_unique_key_result.withColumn(
            "gl_account", F.col("gl_account").cast("integer").cast("string")
        )
        .withColumn("lohnart_code", F.col("lohnart_code").cast("integer").cast("string"))
        .withColumn("pay_date", F.col("pay_date").cast("date").cast("string"))
    )


def filter_retro_runs(cast_data_types_result: DataFrame) -> DataFrame:
    """Filter rows to keep only retro pay runs.

    Args:
        cast_data_types_result: DataFrame containing payroll data with run_type
            and other pay run attributes.

    Returns:
        DataFrame containing only rows where run_type equals 'retro', with the
        same schema as the input.
    """
    return cast_data_types_result.filter(F.col("run_type") == "retro")


def filter_regular_runs(cast_data_types_result: DataFrame) -> DataFrame:
    """Filter payroll register rows to only include regular pay runs.

    Args:
        cast_data_types_result: DataFrame containing deduplicated payroll register
            data with typed columns including run_type.

    Returns:
        DataFrame containing only rows where run_type equals 'regular', with
        the same schema as the input.
    """
    return cast_data_types_result.filter(F.col("run_type") == "regular")


def sum_retro_amounts(filter_retro_runs_result: DataFrame) -> DataFrame:
    """Sum retro amounts grouped by employee, pay period, and lohnart code.

    Args:
        filter_retro_runs_result: DataFrame containing retro run records with
            employee, pay period, lohnart code, and amount columns.

    Returns:
        DataFrame with employee_id, pay_period, lohnart_code (as integer), and
        retro_amount representing the sum of amount_eur per group.
    """
    return (
        filter_retro_runs_result.groupBy("employee_id", "pay_period", "lohnart_code")
        .agg(F.sum("amount_eur").alias("retro_amount"))
        .select(
            F.col("employee_id").cast("string"),
            F.col("pay_period").cast("string"),
            F.col("lohnart_code").cast("integer"),
            F.col("retro_amount").cast("decimal(14,2)"),
        )
    )


def join_regular_and_retro_data(
    filter_regular_runs_result: DataFrame, sum_retro_amounts_result: DataFrame
) -> DataFrame:
    """Join regular pay data with aggregated retro pay amounts.

    Performs a left join between the filtered regular pay runs and the summed
    retro amounts on employee_id, pay_period, and lohnart_code, preserving all
    regular pay rows and attaching any matching retro_amount values.

    Args:
        filter_regular_runs_result: DataFrame containing filtered regular pay
            run records with employee, period, and pay component details.
        sum_retro_amounts_result: DataFrame containing summed retro amounts
            grouped by employee_id, pay_period, and lohnart_code.

    Returns:
        DataFrame with all columns from filter_regular_runs_result plus the
        retro_amount column from sum_retro_amounts_result, joined on
        employee_id, pay_period, and lohnart_code.
    """
    retro_prepared = (
        sum_retro_amounts_result.withColumn(
            "lohnart_code_str", F.col("lohnart_code").cast("string")
        )
        .drop("lohnart_code")
        .withColumnRenamed("lohnart_code_str", "lohnart_code")
    )
    joined = filter_regular_runs_result.join(
        retro_prepared, on=["employee_id", "pay_period", "lohnart_code"], how="left"
    )
    return joined.select(
        F.col("employee_id"),
        F.col("pay_run_id"),
        F.col("pay_period"),
        F.col("pay_date"),
        F.col("run_type"),
        F.col("lohnart_code"),
        F.col("component_name"),
        F.col("gl_account"),
        F.col("cost_center_code"),
        F.col("location_code"),
        F.col("bearer"),
        F.col("amount_eur"),
        F.col("assessment_base_eur"),
        F.col("quantity"),
        F.col("rate"),
        F.col("retro_amount"),
    )


def combine_regular_and_retro_pay(join_regular_and_retro_data_result: DataFrame) -> DataFrame:
    """Combines regular pay amounts with retro pay amounts into a single amount column.

    Adds the retro_amount to amount_eur for each row, treating null retro_amount
    values as zero, then drops the retro_amount column from the result.

    Args:
        join_regular_and_retro_data_result: DataFrame containing regular pay data
            joined with retro pay data, including both amount_eur and retro_amount columns.

    Returns:
        DataFrame with the same schema as the input minus the retro_amount column,
        where amount_eur reflects the sum of regular and retro pay amounts.
    """
    return join_regular_and_retro_data_result.withColumn(
        "amount_eur",
        F.col("amount_eur") + F.coalesce(F.col("retro_amount"), F.lit(0).cast("decimal(14,2)")),
    ).drop("retro_amount")
