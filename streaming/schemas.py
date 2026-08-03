"""Schemas for the streaming layer.

Bronze is intentionally schema-loose: every PPR CSV column is captured as a
string so that upstream schema drift (renamed/added/removed columns) never
breaks ingestion. Typing and cleaning happen in Silver.
"""

from pyspark.sql.types import StringType, StructField, StructType

PPR_RAW_SCHEMA = StructType(
    [
        StructField("Date of Sale (dd/mm/yyyy)", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("County", StringType(), True),
        StructField("Eircode", StringType(), True),
        StructField("Price (€)", StringType(), True),
        StructField("Not Full Market Price", StringType(), True),
        StructField("VAT Exclusive", StringType(), True),
        StructField("Description of Property", StringType(), True),
        StructField("Property Size Description", StringType(), True),
    ]
)

# Delta Lake column names can't contain spaces, parentheses, or other special
# characters. Values are stored unmodified; only these headers are normalized
# so the columns are usable in Delta (and BigQuery, later on).
COLUMN_NAME_MAP = {
    "Date of Sale (dd/mm/yyyy)": "date_of_sale",
    "Address": "address",
    "County": "county",
    "Eircode": "eircode",
    "Price (€)": "price_eur",
    "Not Full Market Price": "not_full_market_price",
    "VAT Exclusive": "vat_exclusive",
    "Description of Property": "description_of_property",
    "Property Size Description": "property_size_description",
}
