from __future__ import annotations

from pathlib import Path


OUT = Path("e_360/sql/enhanced_raw_vault_source_split_load.sql")
ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_SAT_COLUMNS = {"batch_ref", "pull_ts", "origin_sys"}


def crm(name: str) -> str:
    return f"crm_{name}_csv"


def sap(name: str) -> str:
    return f"sap_{name}_db"


def nz(col: str) -> str:
    return f"NULLIF(TRIM(CAST({col} AS CHAR)), '')"


def hk(source: str, col: str) -> str:
    return f"MD5(CONCAT('{source}|', {nz(col)}))"


def lhk(*cols: str) -> str:
    return "MD5(CONCAT_WS('|', " + ", ".join(cols) + "))"


def load_date(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"COALESCE({nz(prefix + 'pull_ts')}, CURRENT_TIMESTAMP)"


def record_source(source: str) -> str:
    return f"'{source}'"


def create_table(name: str, columns: list[str]) -> str:
    cols = ",\n    ".join(f"{col} TEXT" for col in columns)
    return f"CREATE TABLE IF NOT EXISTS {name} (\n    {cols}\n);\n"


def insert(name: str, columns: list[str], selects: list[str]) -> str:
    body = "\nUNION ALL\n".join(selects)
    return (
        f"TRUNCATE TABLE {name};\n"
        f"INSERT INTO {name} ({', '.join(columns)})\n"
        f"{body};\n"
    )


def select_distinct(exprs: list[str], table: str, where: str) -> str:
    return f"SELECT DISTINCT\n    " + ",\n    ".join(exprs) + f"\nFROM {table}\nWHERE {where}"


hub_defs = {
    "hub_person": ("person_hash_key", "person_id"),
    "hub_natural_person": ("natural_person_hash_key", "natural_person_id"),
    "hub_legal_person": ("legal_person_hash_key", "legal_person_id"),
    "hub_contact": ("contact_hash_key", "contact_id"),
    "hub_identities": ("identities_hash_key", "identities_id"),
    "hub_address": ("address_hash_key", "address_id"),
    "hub_consent": ("consent_hash_key", "consent_id"),
    "hub_marketing_preference": ("marketing_preference_hash_key", "marketing_preference_id"),
    "hub_marketing_engagement": ("marketing_engagement_hash_key", "marketing_engagement_id"),
    "hub_lead": ("lead_hash_key", "lead_id"),
    "hub_quote": ("quote_hash_key", "quote_id"),
    "hub_policy": ("policy_hash_key", "policy_id"),
    "hub_account": ("account_hash_key", "account_id"),
    "hub_customer": ("customer_hash_key", "customer_id"),
    "hub_product": ("product_hash_key", "product_id"),
    "hub_motor": ("motor_hash_key", "motor_id"),
    "hub_home": ("home_hash_key", "home_id"),
    "hub_broker": ("broker_hash_key", "agent_id"),
    "hub_campaign": ("campaign_hash_key", "campaign_id"),
    "hub_channel": ("channel_hash_key", "channel_id"),
    "hub_claim": ("claim_hash_key", "claim_id"),
    "hub_complaint": ("complaint_hash_key", "complaint_id"),
    "hub_insured_object": ("insured_object_hash_key", "insured_object_id"),
    "hub_override": ("override_hash_key", "override_id"),
    "hub_regulation": ("regulation_hash_key", "regulation_id"),
}

hub_sources = {
    "hub_person": [("CRM", crm("party_master"), "party_ref"), ("SAP", sap("person"), "person_id")],
    "hub_natural_person": [("CRM", crm("party_master"), "natural_ref"), ("SAP", sap("person"), "person_id", "UPPER(person_type) = 'NATURAL'")],
    "hub_legal_person": [("CRM", crm("party_master"), "legal_ref"), ("SAP", sap("person"), "person_id", "UPPER(person_type) = 'LEGAL'")],
    "hub_contact": [("CRM", crm("contact_point"), "contact_ref")],
    "hub_identities": [("CRM", crm("identity_registry"), "identity_ref")],
    "hub_address": [("CRM", crm("address_book"), "address_ref"), ("CRM", crm("enhanced_address_book"), "src_address_id"), ("SAP", sap("address"), "address_id")],
    "hub_consent": [("CRM", crm("consent_snapshot"), "consent_ref")],
    "hub_marketing_preference": [("CRM", crm("comm_preference"), "preference_ref")],
    "hub_marketing_engagement": [("CRM", crm("campaign_touch"), "engagement_ref")],
    "hub_lead": [("CRM", crm("lead_register"), "lead_ref")],
    "hub_quote": [("CRM", crm("quote_register"), "quote_ref")],
    "hub_policy": [("CRM", crm("policy_register"), "policy_ref"), ("SAP", sap("home"), "policy_id"), ("SAP", sap("motor"), "policy_id")],
    "hub_account": [("CRM", crm("account_book"), "account_ref")],
    "hub_customer": [("CRM", crm("customer_portfolio"), "customer_ref")],
    "hub_product": [("CRM", crm("product_catalog"), "product_ref"), ("SAP", sap("product"), "product_id"), ("SAP", sap("home"), "product_id"), ("SAP", sap("motor"), "product_id")],
    "hub_motor": [("CRM", crm("vehicle_asset"), "vehicle_ref"), ("SAP", sap("motor"), "motor_id")],
    "hub_home": [("CRM", crm("property_asset"), "property_ref"), ("SAP", sap("home"), "home_id")],
    "hub_broker": [("CRM", crm("broker_book"), "src_agent_id")],
    "hub_campaign": [("CRM", crm("campaign_register"), "src_campaign_id")],
    "hub_channel": [("CRM", crm("channel_catalog"), "src_channel_id")],
    "hub_claim": [("CRM", crm("claim_register"), "src_claim_id")],
    "hub_complaint": [("CRM", crm("complaint_register"), "src_complaint_id")],
    "hub_insured_object": [("CRM", crm("property_asset"), "insured_object_id"), ("CRM", crm("vehicle_asset"), "insured_object_id")],
    "hub_override": [("CRM", crm("override_register"), "src_override_id")],
    "hub_regulation": [("CRM", crm("regulation_register"), "src_regulation_id")],
}

link_defs = {
    "link_person_natural_person": ("person_natural_person_hash_key", "person_hash_key", "natural_person_hash_key"),
    "link_person_legal_person": ("person_legal_person_hash_key", "person_hash_key", "legal_person_hash_key"),
    "link_person_contact": ("person_contact_hash_key", "person_hash_key", "contact_hash_key"),
    "link_person_identities": ("person_identities_hash_key", "person_hash_key", "identities_hash_key"),
    "link_person_address": ("person_address_hash_key", "person_hash_key", "address_hash_key"),
    "link_person_consent": ("person_consent_hash_key", "person_hash_key", "consent_hash_key"),
    "link_person_marketing_preference": ("person_marketing_preference_hash_key", "person_hash_key", "marketing_preference_hash_key"),
    "link_person_marketing_engagement": ("person_marketing_engagement_hash_key", "person_hash_key", "marketing_engagement_hash_key"),
    "link_person_lead": ("person_lead_hash_key", "person_hash_key", "lead_hash_key"),
    "link_person_account": ("person_account_hash_key", "person_hash_key", "account_hash_key"),
    "link_customer_person": ("customer_person_hash_key", "customer_hash_key", "person_hash_key"),
    "link_customer_lead": ("customer_lead_hash_key", "customer_hash_key", "lead_hash_key"),
    "link_quote_person": ("quote_person_hash_key", "quote_hash_key", "person_hash_key"),
    "link_quote_product": ("quote_product_hash_key", "quote_hash_key", "product_hash_key"),
    "link_policy_customer": ("policy_customer_hash_key", "policy_hash_key", "customer_hash_key"),
    "link_policy_product": ("policy_product_hash_key", "policy_hash_key", "product_hash_key"),
    "link_policy_quote": ("policy_quote_hash_key", "policy_hash_key", "quote_hash_key"),
    "link_broker_person": ("broker_person_hash_key", "broker_hash_key", "person_hash_key"),
    "link_person_campaign": ("person_campaign_hash_key", "person_hash_key", "campaign_hash_key"),
    "link_claim_policy": ("claim_policy_hash_key", "claim_hash_key", "policy_hash_key"),
    "link_complaint_policy": ("complaint_policy_hash_key", "complaint_hash_key", "policy_hash_key"),
    "link_complaint_regulation": ("complaint_regulation_hash_key", "complaint_hash_key", "regulation_hash_key"),
    "link_insured_object_home": ("insured_object_home_hash_key", "insured_object_hash_key", "home_hash_key"),
    "link_insured_object_motor": ("insured_object_motor_hash_key", "insured_object_hash_key", "motor_hash_key"),
    "link_policy_broker": ("policy_broker_hash_key", "policy_hash_key", "broker_hash_key"),
    "link_policy_channel": ("policy_channel_hash_key", "policy_hash_key", "channel_hash_key"),
    "link_policy_insured_object": ("policy_insured_object_hash_key", "policy_hash_key", "insured_object_hash_key"),
    "link_policy_override": ("policy_override_hash_key", "policy_hash_key", "override_hash_key"),
    "link_quote_broker": ("quote_broker_hash_key", "quote_hash_key", "broker_hash_key"),
    "link_quote_channel": ("quote_channel_hash_key", "quote_hash_key", "channel_hash_key"),
}


def link_select(source: str, table: str, left_col: str, right_col: str, left_source: str | None = None, right_source: str | None = None, where_extra: str = "1=1") -> tuple[str, list[str]]:
    left_source = left_source or source
    right_source = right_source or source
    left_hash = hk(left_source, left_col)
    right_hash = hk(right_source, right_col)
    return table, [lhk(left_hash, right_hash), load_date(), record_source(source), left_hash, right_hash, f"{nz(left_col)} IS NOT NULL AND {nz(right_col)} IS NOT NULL AND {where_extra}"]


link_sources = {
    "link_person_natural_person": [link_select("CRM", crm("party_master"), "party_ref", "natural_ref"), link_select("SAP", sap("person"), "person_id", "person_id", where_extra="UPPER(person_type) = 'NATURAL'")],
    "link_person_legal_person": [link_select("CRM", crm("party_master"), "party_ref", "legal_ref"), link_select("SAP", sap("person"), "person_id", "person_id", where_extra="UPPER(person_type) = 'LEGAL'")],
    "link_person_contact": [link_select("CRM", crm("contact_point"), "party_ref", "contact_ref")],
    "link_person_identities": [link_select("CRM", crm("identity_registry"), "party_ref", "identity_ref")],
    "link_person_address": [link_select("CRM", crm("address_book"), "party_ref", "address_ref"), link_select("CRM", crm("enhanced_person_relationships"), "src_person_id", "src_address_id", where_extra="source_extract = 'person_address_bridge.csv'"), link_select("SAP", sap("address"), "person_id", "address_id")],
    "link_person_consent": [link_select("CRM", crm("consent_snapshot"), "party_ref", "consent_ref")],
    "link_person_marketing_preference": [link_select("CRM", crm("comm_preference"), "party_ref", "preference_ref")],
    "link_person_marketing_engagement": [link_select("CRM", crm("campaign_touch"), "party_ref", "engagement_ref")],
    "link_person_lead": [link_select("CRM", crm("lead_register"), "party_ref", "lead_ref")],
    "link_person_account": [link_select("CRM", crm("account_book"), "party_ref", "account_ref")],
    "link_customer_person": [link_select("CRM", crm("customer_portfolio"), "customer_ref", "party_ref")],
    "link_customer_lead": [link_select("CRM", crm("customer_lead_bridge"), "customer_ref", "lead_ref")],
    "link_quote_person": [link_select("CRM", crm("quote_register"), "quote_ref", "party_ref")],
    "link_quote_product": [link_select("CRM", crm("quote_register"), "quote_ref", "product_ref")],
    "link_policy_customer": [link_select("CRM", crm("policy_register"), "policy_ref", "customer_ref")],
    "link_policy_product": [link_select("CRM", crm("policy_register"), "policy_ref", "product_ref"), link_select("SAP", sap("home"), "policy_id", "product_id"), link_select("SAP", sap("motor"), "policy_id", "product_id")],
    "link_policy_quote": [link_select("CRM", crm("policy_register"), "policy_ref", "quote_ref"), link_select("CRM", crm("enhanced_policy_relationships"), "src_policy_id", "src_quote_id", where_extra="source_extract = 'policy_quote_bridge.csv'")],
    "link_broker_person": [link_select("CRM", crm("enhanced_person_relationships"), "src_agent_id", "src_person_id", where_extra="source_extract = 'broker_person_bridge.csv'")],
    "link_person_campaign": [link_select("CRM", crm("enhanced_person_relationships"), "src_person_id", "src_campaign_id", where_extra="source_extract = 'person_campaign_bridge.csv'")],
    "link_claim_policy": [link_select("CRM", crm("enhanced_policy_relationships"), "src_claim_id", "src_policy_id", where_extra="source_extract = 'claim_policy_bridge.csv'")],
    "link_complaint_policy": [link_select("CRM", crm("enhanced_policy_relationships"), "src_complaint_id", "src_policy_id", where_extra="source_extract = 'complaint_policy_bridge.csv'")],
    "link_complaint_regulation": [link_select("CRM", crm("enhanced_policy_relationships"), "src_complaint_id", "src_regulation_id", where_extra="source_extract = 'complaint_regulation_bridge.csv'")],
    "link_insured_object_home": [link_select("CRM", crm("enhanced_policy_relationships"), "src_insured_object_id", "src_home_id", where_extra="source_extract = 'insured_object_home_bridge.csv'")],
    "link_insured_object_motor": [link_select("CRM", crm("enhanced_policy_relationships"), "src_insured_object_id", "src_motor_id", where_extra="source_extract = 'insured_object_motor_bridge.csv'")],
    "link_policy_broker": [link_select("CRM", crm("enhanced_policy_relationships"), "src_policy_id", "src_agent_id", where_extra="source_extract = 'policy_broker_bridge.csv'")],
    "link_policy_channel": [link_select("CRM", crm("enhanced_policy_relationships"), "src_policy_id", "src_channel_id", where_extra="source_extract = 'policy_channel_bridge.csv'")],
    "link_policy_insured_object": [link_select("CRM", crm("enhanced_policy_relationships"), "src_policy_id", "src_insured_object_id", where_extra="source_extract = 'policy_insured_object_bridge.csv'")],
    "link_policy_override": [link_select("CRM", crm("enhanced_policy_relationships"), "src_policy_id", "src_override_id", where_extra="source_extract = 'policy_override_bridge.csv'")],
    "link_quote_broker": [link_select("CRM", crm("enhanced_policy_relationships"), "src_quote_id", "src_agent_id", where_extra="source_extract = 'quote_broker_bridge.csv'")],
    "link_quote_channel": [link_select("CRM", crm("enhanced_policy_relationships"), "src_quote_id", "src_channel_id", where_extra="source_extract = 'quote_channel_bridge.csv'")],
}

sat_sources = {
    "sat_person_crm": ("CRM", crm("party_master"), "person_hash_key", "party_ref"),
    "sat_person_sap": ("SAP", sap("person"), "person_hash_key", "person_id"),
    "sat_address_crm": ("CRM", crm("address_book"), "address_hash_key", "address_ref"),
    "sat_address_sap": ("SAP", sap("address"), "address_hash_key", "address_id"),
    "sat_product_crm": ("CRM", crm("product_catalog"), "product_hash_key", "product_ref"),
    "sat_product_sap": ("SAP", sap("product"), "product_hash_key", "product_id"),
    "sat_home_crm": ("CRM", crm("property_asset"), "home_hash_key", "property_ref"),
    "sat_home_sap": ("SAP", sap("home"), "home_hash_key", "home_id"),
    "sat_motor_crm": ("CRM", crm("vehicle_asset"), "motor_hash_key", "vehicle_ref"),
    "sat_motor_sap": ("SAP", sap("motor"), "motor_hash_key", "motor_id"),
    "sat_contact": ("CRM", crm("contact_point"), "contact_hash_key", "contact_ref"),
    "sat_identities": ("CRM", crm("identity_registry"), "identities_hash_key", "identity_ref"),
    "sat_consent": ("CRM", crm("consent_snapshot"), "consent_hash_key", "consent_ref"),
    "sat_marketing_preference": ("CRM", crm("comm_preference"), "marketing_preference_hash_key", "preference_ref"),
    "sat_marketing_engagement": ("CRM", crm("campaign_touch"), "marketing_engagement_hash_key", "engagement_ref"),
    "sat_lead": ("CRM", crm("lead_register"), "lead_hash_key", "lead_ref"),
    "sat_quote": ("CRM", crm("quote_register"), "quote_hash_key", "quote_ref"),
    "sat_policy": ("CRM", crm("policy_register"), "policy_hash_key", "policy_ref"),
    "sat_account": ("CRM", crm("account_book"), "account_hash_key", "account_ref"),
    "sat_customer": ("CRM", crm("customer_portfolio"), "customer_hash_key", "customer_ref"),
    "sat_broker": ("CRM", crm("broker_book"), "broker_hash_key", "src_agent_id"),
    "sat_campaign": ("CRM", crm("campaign_register"), "campaign_hash_key", "src_campaign_id"),
    "sat_channel": ("CRM", crm("channel_catalog"), "channel_hash_key", "src_channel_id"),
    "sat_claim": ("CRM", crm("claim_register"), "claim_hash_key", "src_claim_id"),
    "sat_complaint": ("CRM", crm("complaint_register"), "complaint_hash_key", "src_complaint_id"),
    "sat_insured_object": ("CRM", crm("property_asset"), "insured_object_hash_key", "insured_object_id"),
    "sat_override": ("CRM", crm("override_register"), "override_hash_key", "src_override_id"),
    "sat_regulation": ("CRM", crm("regulation_register"), "regulation_hash_key", "src_regulation_id"),
    "sat_natural_person": ("CRM", crm("party_master"), "natural_person_hash_key", "natural_ref"),
    "sat_legal_person": ("CRM", crm("party_master"), "legal_person_hash_key", "legal_ref"),
}

source_columns = {
    crm("party_master"): ["party_kind", "tenant_cd", "lead_ind", "paperless_ind", "src_party_ref", "src_party_type", "natural_ref", "given_nm", "family_nm", "display_nm", "title_txt", "occupation_txt", "dob", "birth_yr", "nationality_txt", "gender_txt", "marital_txt", "disability_degree", "language_pref", "role_txt", "job_title_txt", "legal_ref", "legal_name", "legal_score_no", "legal_status_txt", "legal_job_title_txt", "legal_src_ref", "legal_src_type", "constitution_dt", "lead_conv_dt"],
    sap("person"): ["person_type", "organization", "org_establishment_date", "first_name", "middle_name", "last_name", "date_of_birth", "gender", "occupation", "email_address", "phone_number"],
    crm("address_book"): ["party_ref", "street_txt", "postal_cd", "city_nm", "state_cd", "country_cd", "address_type_txt", "region_txt"],
    sap("address"): ["person_id", "address_line_1", "address_line_2", "city", "state", "country", "zipcode"],
    crm("product_catalog"): ["product_cd", "product_line", "product_type_txt", "underwriting_group_txt", "regulatory_approval_cd", "product_status_txt", "product_lob_cd", "product_launch_dt", "product_variant"],
    sap("product"): ["product_type", "product_sub_type", "product_name", "product_start_date", "line_of_business"],
    crm("property_asset"): ["policy_ref", "product_ref", "product_cd", "wall_material_txt", "risk_address_txt", "roof_material_txt", "property_type_txt", "property_state_cd", "existing_home_ind", "street_txt", "postal_cd", "city_nm", "state_cd", "country_cd", "insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_description", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status"],
    sap("home"): ["policy_id", "product_id", "home_type", "home_location", "wall_type", "roof_material"],
    crm("vehicle_asset"): ["policy_ref", "product_ref", "product_cd", "auto_decline_ind", "body_style_txt", "fuel_type_txt", "license_status_txt", "existing_motor_ind", "motor_lapse_cnt", "garage_address_txt", "risk_class_cd", "variant_nm", "owner_type_txt", "registration_state_cd", "vehicle_class_txt", "model_nm", "vehicle_type_txt", "insured_value_amt", "manufacture_yr", "vehicle_age_yrs", "driver_experience_years", "insured_object_id", "insured_object_type", "insured_object_sub_type", "insured_object_description", "insured_value", "currency_code", "insured_object_start_date", "insured_object_end_date", "insured_object_current_status"],
    sap("motor"): ["policy_id", "product_id", "motor_class", "motor_model", "motor_type", "manufacturing_date", "body_colour", "fuel_type", "gear_type", "motor_parked_location"],
}


def _latest_run(folder: Path) -> str | None:
    runs = sorted([path.name for path in folder.iterdir() if path.is_dir()], reverse=True) if folder.exists() else []
    return runs[0] if runs else None


def _source_file_for_table(table: str) -> tuple[str, str] | None:
    if table.startswith("crm_") and table.endswith("_csv"):
        return "crm", table.removeprefix("crm_").removesuffix("_csv") + ".csv"
    if table.startswith("sap_") and table.endswith("_db"):
        return "sap", table.removeprefix("sap_").removesuffix("_db") + ".csv"
    return None


def _actual_header(table: str) -> list[str]:
    resolved = _source_file_for_table(table)
    if not resolved:
        return []
    kind, file_name = resolved
    run = _latest_run(ROOT / "data" / "raw" / "enhanced" / "prd_01")
    if not run:
        return []
    if kind == "crm":
        path = ROOT / "data" / "raw" / "enhanced" / "prd_01" / run / "vault_ready_28" / file_name
    else:
        path = ROOT / "data" / "raw" / "enhanced" / "prd_02" / run / file_name
    if not path.exists():
        return []
    import csv

    with path.open("r", newline="", encoding="utf-8") as f:
        return next(csv.reader(f), [])


def generic_source_columns(table: str, id_col: str) -> list[str]:
    header = _actual_header(table)
    if header:
        return [col for col in header if col not in EXCLUDED_SAT_COLUMNS and col != id_col]
    if table in source_columns:
        return [col for col in source_columns[table] if col != id_col]
    return ["source_payload_note"]


def sat_sql(name: str, source: str, table: str, hash_col: str, id_col: str) -> tuple[str, str]:
    attrs = generic_source_columns(table, id_col)
    if attrs == ["source_payload_note"]:
        select_attrs = [f"'{table}: add target-specific source columns here' AS source_payload_note"]
    else:
        select_attrs = [f"{col} AS {col}" for col in attrs]
    columns = [hash_col, "load_date", "record_source", *attrs]
    ddl = create_table(name, columns)
    stmt = insert(
        name,
        columns,
        [
            select_distinct(
                [f"{hk(source, id_col)} AS {hash_col}", f"{load_date()} AS load_date", f"{record_source(source)} AS record_source", *select_attrs],
                table,
                f"{nz(id_col)} IS NOT NULL",
            )
        ],
    )
    return ddl, stmt


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    sections: list[str] = []
    sections.append(
        """-- Enhanced raw vault source-split load SQL.
-- Source assumptions:
--   PRD1/CRM raw tables are named crm_<raw_file_stem>_csv, e.g. crm_party_master_csv.
--   PRD2/SAP raw tables are named sap_<raw_file_stem>_db, e.g. sap_person_db.
-- Design:
--   25 union hubs, 30 union links, 30 source-split satellites.
--   SAP has five source entities: person, address, product, home, motor.
--   SAP natural/legal attributes are kept in sat_person_sap for the 30-satellite design.
--   If you want sat_natural_person_sap and sat_legal_person_sap as separate raw-vault sats,
--   expand this model to 32 satellites / 87 total tables.
"""
    )

    sections.append("-- ============================================================\n-- HUBS\n-- ============================================================\n")
    for table, (hk_col, id_col) in hub_defs.items():
        columns = [hk_col, "load_date", "record_source", id_col]
        sections.append(create_table(table, columns))
        selects = []
        for source_def in hub_sources[table]:
            source, src_table, src_col, *where_extra = source_def
            where = f"{nz(src_col)} IS NOT NULL"
            if where_extra:
                where += f" AND {where_extra[0]}"
            selects.append(
                select_distinct(
                    [f"{hk(source, src_col)} AS {hk_col}", f"{load_date()} AS load_date", f"{record_source(source)} AS record_source", f"{nz(src_col)} AS {id_col}"],
                    src_table,
                    where,
                )
            )
        sections.append(insert(table, columns, selects))

    sections.append("-- ============================================================\n-- LINKS\n-- ============================================================\n")
    for table, (link_hk, left_hk, right_hk) in link_defs.items():
        columns = [link_hk, "load_date", "record_source", left_hk, right_hk]
        sections.append(create_table(table, columns))
        selects = []
        for src_table, exprs in link_sources[table]:
            link_expr, ld_expr, rs_expr, left_expr, right_expr, where = exprs
            selects.append(select_distinct([f"{link_expr} AS {link_hk}", f"{ld_expr} AS load_date", f"{rs_expr} AS record_source", f"{left_expr} AS {left_hk}", f"{right_expr} AS {right_hk}"], src_table, where))
        sections.append(insert(table, columns, selects))

    sections.append("-- ============================================================\n-- SATELLITES\n-- ============================================================\n")
    for table, (source, src_table, hash_col, id_col) in sat_sources.items():
        ddl, stmt = sat_sql(table, source, src_table, hash_col, id_col)
        sections.append(ddl)
        sections.append(stmt)

    OUT.write_text("\n".join(sections), encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
