import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(ROOT, "data")

INPUT_ROOT = os.path.join(DATA_ROOT, "input")

RAW_ROOT = os.path.join(DATA_ROOT, "raw")
RAW_CRM_ROOT = os.path.join(RAW_ROOT, "crm")
RAW_CRM_CANONICAL_ROOT = os.path.join(RAW_ROOT, "crm_canonical")
RAW_PRD1_ROOT = os.path.join(RAW_ROOT, "prd_01")
RAW_PRD2_ROOT = os.path.join(RAW_ROOT, "prd_02")
RAW_API_ROOT = os.path.join(RAW_ROOT, "api")
RAW_CLAIMS_ROOT = os.path.join(RAW_ROOT, "claims")
RAW_CLAIMS_CANONICAL_ROOT = os.path.join(RAW_ROOT, "claims_canonical")
RAW_DATA_SOURCE_ROOT = os.path.join(RAW_ROOT, "data_source")
RAW_DATA_SOURCE_CANONICAL_ROOT = os.path.join(RAW_ROOT, "data_source_canonical")
NEW_OUTPUTS_SRC_ROOT = os.path.join(DATA_ROOT, "new_outputs_src")
PRODUCT_COMBINED_ROOT = os.path.join(DATA_ROOT, "product_combined")

OUTPUT_ROOT = os.path.join(DATA_ROOT, "output")

SYNTHETIC_ROOT = os.path.join(DATA_ROOT, "synthetic")
SYNTHETIC_BASE_ROOT = os.path.join(SYNTHETIC_ROOT, "base")
SYNTHETIC_ENHANCED_ROOT = os.path.join(SYNTHETIC_ROOT, "enhanced")
SYNTHETIC_MLOPS_ROOT = os.path.join(SYNTHETIC_ROOT, "mlops")
MLOPS_ROOT = SYNTHETIC_MLOPS_ROOT

SILVER_ROOT = os.path.join(DATA_ROOT, "silver")
SILVER_BASE_ROOT = os.path.join(SILVER_ROOT, "base")
SILVER_ENHANCED_ROOT = os.path.join(SILVER_ROOT, "enhanced")
SILVER_MLOPS_ROOT = os.path.join(SILVER_ROOT, "mlops")
SILVER_REBUILT_ROOT = os.path.join(SILVER_ROOT, "rebuild")
SILVER_API_ROOT = os.path.join(SILVER_ROOT, "api")
SILVER_CLAIMS_ROOT = os.path.join(SILVER_ROOT, "claims")
SILVER_DATA_SOURCE_ROOT = os.path.join(SILVER_ROOT, "data_source")

SCD2_ROOT = os.path.join(DATA_ROOT, "scd2")
SCD2_BASE_ROOT = os.path.join(SCD2_ROOT, "base")
SCD2_ENHANCED_ROOT = os.path.join(SCD2_ROOT, "enhanced")
SCD2_MLOPS_ROOT = os.path.join(SCD2_ROOT, "mlops")
SCD2_UPDATED_ROOT = os.path.join(SCD2_ROOT, "updated")
SCD2_RAW_ROOT = os.path.join(SCD2_ROOT, "raw")
SCD2_REPORT_ROOT = os.path.join(SCD2_ROOT, "reports")


def ensure_data_roots(
    include_optional_raw_silver=True,
    include_new_outputs_src=True,
    include_product_combined=True,
    include_legacy_global_prd=True,
    include_scd2_reports=True,
):
    paths = [
        DATA_ROOT,
        INPUT_ROOT,
        RAW_ROOT,
        OUTPUT_ROOT,
        SYNTHETIC_ROOT,
        SYNTHETIC_BASE_ROOT,
        SYNTHETIC_ENHANCED_ROOT,
        SYNTHETIC_MLOPS_ROOT,
        MLOPS_ROOT,
        SCD2_ROOT,
        SCD2_BASE_ROOT,
        SCD2_ENHANCED_ROOT,
        SCD2_MLOPS_ROOT,
    ]

    if include_optional_raw_silver:
        paths.extend(
            [
                RAW_CRM_ROOT,
                RAW_CRM_CANONICAL_ROOT,
                RAW_API_ROOT,
                RAW_CLAIMS_ROOT,
                RAW_CLAIMS_CANONICAL_ROOT,
                RAW_DATA_SOURCE_ROOT,
                RAW_DATA_SOURCE_CANONICAL_ROOT,
                SILVER_ROOT,
                SILVER_BASE_ROOT,
                SILVER_ENHANCED_ROOT,
                SILVER_MLOPS_ROOT,
                SILVER_REBUILT_ROOT,
                SILVER_API_ROOT,
                SILVER_CLAIMS_ROOT,
                SILVER_DATA_SOURCE_ROOT,
                SCD2_UPDATED_ROOT,
                SCD2_RAW_ROOT,
            ]
        )

    if include_new_outputs_src:
        paths.append(NEW_OUTPUTS_SRC_ROOT)

    if include_product_combined:
        paths.append(PRODUCT_COMBINED_ROOT)

    if include_legacy_global_prd:
        paths.extend([RAW_PRD1_ROOT, RAW_PRD2_ROOT])

    if include_scd2_reports:
        paths.append(SCD2_REPORT_ROOT)

    for path in paths:
        os.makedirs(path, exist_ok=True)
