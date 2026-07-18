import psycopg

from db import connection_details


SCHEMA = "business_vault"


SQL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

DROP VIEW IF EXISTS {SCHEMA}.vw_motor_fallback_match_best;
DROP VIEW IF EXISTS {SCHEMA}.vw_motor_fallback_match_candidates;
DROP VIEW IF EXISTS {SCHEMA}.vw_home_fallback_match_best;
DROP VIEW IF EXISTS {SCHEMA}.vw_home_fallback_match_candidates;

CREATE OR REPLACE VIEW {SCHEMA}.vw_motor_fallback_match_candidates AS
WITH crm_motor AS (
    SELECT DISTINCT
        hm.motor_hash_key AS crm_motor_hash_key,
        hm.insured_object_motor_id AS crm_motor_id,
        hp.policy_id AS crm_policy_id,
        smc.vehicle_model,
        smc.vehicle_type,
        smc.vehicle_class,
        smc.fuel_type,
        smc.vehicle_year,
        smc.body_type,
        smc.variant,
        smc.risk_class_code,
        smc.vehicle_regstate,
        smc.motor_sum_insrd
    FROM raw_vault.hub_motor hm
    JOIN raw_vault.link_insured_object_motor liom
      ON liom.motor_hash_key = hm.motor_hash_key
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.insured_object_hash_key = liom.insured_object_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpio.policy_hash_key
     AND hp.record_source = 'CRM'
    JOIN raw_vault.sat_motor_crm smc
      ON smc.motor_hash_key = hm.motor_hash_key
    WHERE hm.record_source = 'CRM'
),
sap_motor AS (
    SELECT DISTINCT
        hm.motor_hash_key AS sap_motor_hash_key,
        hm.insured_object_motor_id AS sap_motor_id,
        sms.policy_id AS sap_policy_id,
        sms.product_id AS sap_product_id,
        sms.motor_model,
        sms.motor_type,
        sms.motor_class,
        sms.fuel_type,
        sms.manufacturing_date,
        sms.body_colour,
        sms.gear_type,
        sms.motor_parked_location
    FROM raw_vault.hub_motor hm
    JOIN raw_vault.sat_motor_sap sms
      ON sms.motor_hash_key = hm.motor_hash_key
    WHERE hm.record_source = 'SAP'
),
candidates AS (
    SELECT
        crm.crm_motor_hash_key,
        crm.crm_motor_id,
        crm.crm_policy_id,
        sap.sap_motor_hash_key,
        sap.sap_motor_id,
        sap.sap_policy_id,
        CASE WHEN upper(trim(crm.vehicle_model)) = upper(trim(sap.motor_model)) THEN 1 ELSE 0 END AS model_match,
        CASE WHEN upper(trim(crm.fuel_type)) = upper(trim(sap.fuel_type)) THEN 1 ELSE 0 END AS fuel_match,
        CASE WHEN nullif(trim(crm.vehicle_year), '') = nullif(trim(sap.manufacturing_date), '') THEN 1 ELSE 0 END AS manufacture_year_match,
        CASE WHEN upper(trim(crm.vehicle_type)) = upper(trim(sap.motor_type)) THEN 1 ELSE 0 END AS type_match,
        CASE WHEN upper(trim(crm.vehicle_class)) = upper(trim(sap.motor_class)) THEN 1 ELSE 0 END AS class_match,
        CASE WHEN upper(trim(crm.body_type)) = upper(trim(sap.body_colour)) THEN 1 ELSE 0 END AS body_match,
        crm.vehicle_model AS crm_vehicle_model,
        sap.motor_model AS sap_motor_model,
        crm.fuel_type AS crm_fuel_type,
        sap.fuel_type AS sap_fuel_type,
        crm.vehicle_year AS crm_vehicle_year,
        sap.manufacturing_date AS sap_manufacturing_date,
        crm.vehicle_type AS crm_vehicle_type,
        sap.motor_type AS sap_motor_type,
        crm.vehicle_class AS crm_vehicle_class,
        sap.motor_class AS sap_motor_class
    FROM crm_motor crm
    JOIN sap_motor sap
      ON upper(trim(crm.vehicle_model)) = upper(trim(sap.motor_model))
      OR (
            upper(trim(crm.fuel_type)) = upper(trim(sap.fuel_type))
        AND nullif(trim(crm.vehicle_year), '') = nullif(trim(sap.manufacturing_date), '')
      )
)
SELECT
    *,
    model_match
    + fuel_match
    + manufacture_year_match
    + type_match
    + class_match
    + body_match AS match_score,
    CASE
        WHEN model_match + fuel_match + manufacture_year_match + type_match + class_match + body_match >= 5 THEN 'HIGH'
        WHEN model_match + fuel_match + manufacture_year_match + type_match + class_match + body_match >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS match_confidence
FROM candidates;

CREATE OR REPLACE VIEW {SCHEMA}.vw_motor_fallback_match_best AS
SELECT *
FROM (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY crm_motor_hash_key
            ORDER BY match_score DESC, sap_motor_id
        ) AS rn
    FROM {SCHEMA}.vw_motor_fallback_match_candidates c
    WHERE match_score >= 4
) ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW {SCHEMA}.vw_home_fallback_match_candidates AS
WITH crm_home AS (
    SELECT DISTINCT
        hh.home_hash_key AS crm_home_hash_key,
        hh.insured_object_home_id AS crm_home_id,
        hp.policy_id AS crm_policy_id,
        shc.home_type,
        shc.home_risk_address,
        shc.home_state,
        shc.wall_construction,
        shc.roof_construction,
        shc.is_existing_home_customer
    FROM raw_vault.hub_home hh
    JOIN raw_vault.link_insured_object_home lioh
      ON lioh.home_hash_key = hh.home_hash_key
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.insured_object_hash_key = lioh.insured_object_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpio.policy_hash_key
     AND hp.record_source = 'CRM'
    JOIN raw_vault.sat_home_crm shc
      ON shc.home_hash_key = hh.home_hash_key
    WHERE hh.record_source = 'CRM'
),
sap_home AS (
    SELECT DISTINCT
        hh.home_hash_key AS sap_home_hash_key,
        hh.insured_object_home_id AS sap_home_id,
        shs.policy_id AS sap_policy_id,
        shs.product_id AS sap_product_id,
        shs.home_type,
        shs.home_location,
        shs.wall_type,
        shs.roof_material
    FROM raw_vault.hub_home hh
    JOIN raw_vault.sat_home_sap shs
      ON shs.home_hash_key = hh.home_hash_key
    WHERE hh.record_source = 'SAP'
),
candidates AS (
    SELECT
        crm.crm_home_hash_key,
        crm.crm_home_id,
        crm.crm_policy_id,
        sap.sap_home_hash_key,
        sap.sap_home_id,
        sap.sap_policy_id,
        CASE WHEN upper(trim(crm.home_type)) = upper(trim(sap.home_type)) THEN 1 ELSE 0 END AS home_type_match,
        CASE WHEN upper(trim(crm.home_risk_address)) = upper(trim(sap.home_location)) THEN 1 ELSE 0 END AS location_match,
        CASE WHEN upper(trim(crm.wall_construction)) = upper(trim(sap.wall_type)) THEN 1 ELSE 0 END AS wall_match,
        CASE WHEN upper(trim(crm.roof_construction)) = upper(trim(sap.roof_material)) THEN 1 ELSE 0 END AS roof_match,
        crm.home_type AS crm_home_type,
        sap.home_type AS sap_home_type,
        crm.home_risk_address AS crm_home_location,
        sap.home_location AS sap_home_location,
        crm.wall_construction AS crm_wall_construction,
        sap.wall_type AS sap_wall_type,
        crm.roof_construction AS crm_roof_construction,
        sap.roof_material AS sap_roof_material
    FROM crm_home crm
    JOIN sap_home sap
      ON upper(trim(crm.home_risk_address)) = upper(trim(sap.home_location))
      OR (
            upper(trim(crm.home_type)) = upper(trim(sap.home_type))
        AND upper(trim(crm.wall_construction)) = upper(trim(sap.wall_type))
        AND upper(trim(crm.roof_construction)) = upper(trim(sap.roof_material))
      )
)
SELECT
    *,
    home_type_match
    + location_match
    + wall_match
    + roof_match AS match_score,
    CASE
        WHEN home_type_match + location_match + wall_match + roof_match >= 4 THEN 'HIGH'
        WHEN home_type_match + location_match + wall_match + roof_match >= 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS match_confidence
FROM candidates;

CREATE OR REPLACE VIEW {SCHEMA}.vw_home_fallback_match_best AS
SELECT *
FROM (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY crm_home_hash_key
            ORDER BY match_score DESC, sap_home_id
        ) AS rn
    FROM {SCHEMA}.vw_home_fallback_match_candidates c
    WHERE match_score >= 3
) ranked
WHERE rn = 1;
"""


def main() -> None:
    with psycopg.connect(**connection_details) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL)
            for view in [
                "vw_motor_fallback_match_candidates",
                "vw_motor_fallback_match_best",
                "vw_home_fallback_match_candidates",
                "vw_home_fallback_match_best",
            ]:
                cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{view}")
                print(f"{SCHEMA}.{view}: {cursor.fetchone()[0]}")
        connection.commit()


if __name__ == "__main__":
    main()
