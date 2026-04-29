import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(ROOT, "data")

INPUT_ROOT = os.path.join(DATA_ROOT, "input")
KAGGLE_INPUT_ROOT = os.path.join(INPUT_ROOT, "kaggle")

RAW_ROOT = os.path.join(DATA_ROOT, "raw")
RAW_CRM_ROOT = os.path.join(RAW_ROOT, "crm")
RAW_CRM_CANONICAL_ROOT = os.path.join(RAW_ROOT, "crm_canonical")
RAW_API_ROOT = os.path.join(RAW_ROOT, "api")
RAW_CLAIMS_ROOT = os.path.join(RAW_ROOT, "claims")
RAW_CLAIMS_CANONICAL_ROOT = os.path.join(RAW_ROOT, "claims_canonical")
RAW_KAGGLE_ROOT = os.path.join(RAW_ROOT, "kaggle")
RAW_DATA_SOURCE_ROOT = os.path.join(RAW_ROOT, "data_source")
RAW_DATA_SOURCE_CANONICAL_ROOT = os.path.join(RAW_ROOT, "data_source_canonical")

OUTPUT_ROOT = os.path.join(DATA_ROOT, "output")

SYNTHETIC_ROOT = os.path.join(DATA_ROOT, "synthetic")
SYNTHETIC_BASE_ROOT = os.path.join(SYNTHETIC_ROOT, "base")

SILVER_ROOT = os.path.join(DATA_ROOT, "silver")
SILVER_REBUILT_ROOT = os.path.join(SILVER_ROOT, "rebuild")
SILVER_API_ROOT = os.path.join(SILVER_ROOT, "api")
SILVER_CLAIMS_ROOT = os.path.join(SILVER_ROOT, "claims")
SILVER_KAGGLE_ROOT = os.path.join(SILVER_ROOT, "kaggle")
SILVER_DATA_SOURCE_ROOT = os.path.join(SILVER_ROOT, "data_source")

SCD2_ROOT = os.path.join(DATA_ROOT, "scd2")
SCD2_BASE_ROOT = os.path.join(SCD2_ROOT, "base")
SCD2_UPDATED_ROOT = os.path.join(SCD2_ROOT, "updated")
SCD2_RAW_ROOT = os.path.join(SCD2_ROOT, "raw")
SCD2_REPORT_ROOT = os.path.join(SCD2_ROOT, "reports")


def ensure_data_roots():
    for path in [
        DATA_ROOT,
        INPUT_ROOT,
        KAGGLE_INPUT_ROOT,
        RAW_ROOT,
        RAW_CRM_ROOT,
        RAW_CRM_CANONICAL_ROOT,
        RAW_API_ROOT,
        RAW_CLAIMS_ROOT,
        RAW_CLAIMS_CANONICAL_ROOT,
        RAW_KAGGLE_ROOT,
        RAW_DATA_SOURCE_ROOT,
        RAW_DATA_SOURCE_CANONICAL_ROOT,
        OUTPUT_ROOT,
        SYNTHETIC_ROOT,
        SYNTHETIC_BASE_ROOT,
        SILVER_ROOT,
        SILVER_REBUILT_ROOT,
        SILVER_API_ROOT,
        SILVER_CLAIMS_ROOT,
        SILVER_KAGGLE_ROOT,
        SILVER_DATA_SOURCE_ROOT,
        SCD2_ROOT,
        SCD2_BASE_ROOT,
        SCD2_UPDATED_ROOT,
        SCD2_RAW_ROOT,
        SCD2_REPORT_ROOT,
    ]:
        os.makedirs(path, exist_ok=True)
