"""PySpark Schemas — SentinelStream Streaming.

Defines the PySpark StructType schema matching the canonical TransactionEvent specification.
"""

# Attempt PySpark import; fallback gracefully if PySpark is not installed
try:
    from pyspark.sql.types import (  # type: ignore
        StructType,
        StructField,
        StringType,
        DoubleType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


def get_spark_transaction_schema():
    """Return PySpark StructType schema for parsing raw transaction JSON strings."""
    if not PYSPARK_AVAILABLE:
        return None

    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("user_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("ip_address", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
    ])
