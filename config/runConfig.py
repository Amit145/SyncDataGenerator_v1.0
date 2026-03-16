import os
from dotenv import load_dotenv

load_dotenv()

DDL_FILE_PATH = os.getenv("DDL_FILE_PATH")
COLUMN_ATTRIBUTE_FILE = os.getenv("COLUMN_ATTRIBUTE_FILE")

PARSED_DDL_PATH = os.getenv("PARSED_DDL_PATH")
TABLE_METADATA_PATH = os.getenv("TABLE_METADATA_PATH")
ORDERED_TABLE_METADATA_PATH = os.getenv("ORDERED_TABLE_METADATA_PATH")
SYNTHETIC_DATA_JSON_PATH = os.getenv("SYNTHETIC_DATA_JSON_PATH")
CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR")

RAW_DDL = r"metadata/fixed_C360-DV.ddl"
DDL_JSON_PATH = r"metadata/table_metadata_ordered.json"
OUTPUT_BASE = r"output"
SYNTHETIC_DATA = r"synthetic_data"
