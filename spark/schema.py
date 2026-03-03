<<<<<<< HEAD
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

"""
This file defines the expected schema of the streaming JSON events.
"""

def get_iot_schema():
    return StructType([
        StructField("device_id", StringType(), True),
        StructField("event_time", StringType(), True), # Read as string first to handle formats
        StructField("temperature", DoubleType(), True),
        StructField("country", StringType(), True)
    ])
=======
"""
This file defines the expected schema of the streaming JSON events.

You will use this schema in streaming_app.py to:
- Parse raw JSON messages
- Enable event-time processing
- Detect malformed or incomplete records
"""
>>>>>>> 98a843ef9356e74eaa4bf0fea8e8c9beafb55213
