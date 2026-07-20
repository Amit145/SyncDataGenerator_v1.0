-- Databricks SQL: CTE-only BV Home and Motor load.
-- Source: raw_vault. Target: business_vault.

-- Home and motor use CTE-only person matching, CRM anchor, and SAP enrichment.
INSERT INTO business_vault.bv_home_master
WITH
natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'NATURAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hn.natural_person_hash_key AS crm_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.birth_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_crm sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hn.natural_person_hash_key AS sap_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.date_of_birth
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_sap sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.first_name)) = upper(trim(sap.first_name))
     AND upper(trim(crm.last_name)) = upper(trim(sap.last_name))
     AND crm.birth_date = sap.date_of_birth
),
legal_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_legal_person_hash_key,
        sap.sap_legal_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'LEGAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hl.legal_person_hash_key AS crm_legal_person_hash_key,
            sl.company_name,
            sl.date_of_constitution
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_crm sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hl.legal_person_hash_key AS sap_legal_person_hash_key,
            sl.organization,
            sl.org_establishment_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_sap sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.company_name)) = upper(trim(sap.organization))
     AND crm.date_of_constitution = sap.org_establishment_date
),
person_match AS (
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM legal_match
),
crm_policy_person_anchor AS (
    SELECT DISTINCT
        pm.global_person_identifier,
        pm.match_type,
        pm.crm_person_id,
        pm.sap_person_id,
        pm.crm_person_hash_key,
        pm.sap_person_hash_key,
        hc.customer_id AS crm_customer_id,
        hp.policy_id AS crm_policy_id,
        hp.policy_hash_key AS crm_policy_hash_key
    FROM person_match pm
    JOIN raw_vault.link_customer_person lcp
      ON lcp.person_hash_key = pm.crm_person_hash_key
    JOIN raw_vault.hub_customer hc
      ON hc.customer_hash_key = lcp.customer_hash_key
     AND hc.record_source = 'CRM'
    JOIN raw_vault.link_policy_customer lpc
      ON lpc.customer_hash_key = hc.customer_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpc.policy_hash_key
     AND hp.record_source = 'CRM'
),
latest_policy_product AS (
    SELECT *
    FROM raw_vault.link_policy_product
    QUALIFY row_number() OVER (
        PARTITION BY policy_hash_key
        ORDER BY load_date DESC
    ) = 1
),
crm_home_anchor AS (
    SELECT DISTINCT
        ppa.global_person_identifier,
        ppa.match_type,
        ppa.crm_person_id,
        ppa.sap_person_id,
        ppa.crm_customer_id,
        ppa.crm_policy_id,
        hh.insured_object_home_id AS crm_home_id,
        hh.home_hash_key AS crm_home_hash_key,
        hh.load_date AS crm_home_load_date,
        hp.product_id AS crm_product_id,
        shc.wall_construction,
        shc.home_risk_address,
        shc.roof_construction,
        shc.home_type,
        shc.home_state,
        shc.is_existing_home_customer
    FROM crm_policy_person_anchor ppa
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.policy_hash_key = ppa.crm_policy_hash_key
    JOIN raw_vault.link_insured_object_home lioh
      ON lioh.insured_object_hash_key = lpio.insured_object_hash_key
    JOIN raw_vault.hub_home hh
      ON hh.home_hash_key = lioh.home_hash_key
     AND hh.record_source = 'CRM'
    JOIN raw_vault.sat_home_crm shc
      ON shc.home_hash_key = hh.home_hash_key
    LEFT JOIN latest_policy_product lpp
      ON lpp.policy_hash_key = ppa.crm_policy_hash_key
    LEFT JOIN raw_vault.hub_product hp
      ON hp.product_hash_key = lpp.product_hash_key
),
home_rows AS (
    SELECT
        concat(ch.crm_home_id, '||', hsap.insured_object_home_id) AS global_home_identifier,
        concat(ch.crm_home_hash_key, '||', hsap.home_hash_key) AS master_home_hash_key,
        concat(ch.crm_policy_id, '||', shs.policy_id) AS policy_identifier,
        coalesce(nullif(shs.home_type, ''), nullif(ch.home_type, '')) AS home_type,
        coalesce(nullif(shs.home_location, ''), nullif(ch.home_risk_address, '')) AS home_location,
        ch.home_state,
        coalesce(nullif(shs.wall_type, ''), nullif(ch.wall_construction, '')) AS wall_construction_type,
        coalesce(nullif(shs.roof_material, ''), nullif(ch.roof_construction, '')) AS roof_construction_type,
        ch.is_existing_home_customer,
        concat(ch.crm_product_id, '||', shs.product_id) AS global_product_identifier
    FROM crm_home_anchor ch
    JOIN raw_vault.sat_home_sap shs
      ON regexp_replace(shs.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(ch.crm_policy_id, '^(SAP_|CRM_)', '')
    JOIN raw_vault.hub_home hsap
      ON hsap.home_hash_key = shs.home_hash_key
     AND hsap.record_source = 'SAP'
    QUALIFY row_number() OVER (
        PARTITION BY concat(ch.crm_home_id, '||', hsap.insured_object_home_id)
        ORDER BY ch.crm_home_hash_key, hsap.home_hash_key
    ) = 1
)
SELECT
    global_home_identifier,
    master_home_hash_key,
    policy_identifier,
    home_type,
    home_location,
    home_state,
    wall_construction_type,
    roof_construction_type,
    is_existing_home_customer,
    current_date() AS effective_from,
    DATE '9999-12-31' AS effective_to,
    'Y' AS current_active_flag,
    global_product_identifier
FROM home_rows;

INSERT INTO business_vault.bv_home_master_xref
WITH
natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'NATURAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hn.natural_person_hash_key AS crm_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.birth_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_crm sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hn.natural_person_hash_key AS sap_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.date_of_birth
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_sap sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.first_name)) = upper(trim(sap.first_name))
     AND upper(trim(crm.last_name)) = upper(trim(sap.last_name))
     AND crm.birth_date = sap.date_of_birth
),
legal_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_legal_person_hash_key,
        sap.sap_legal_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'LEGAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hl.legal_person_hash_key AS crm_legal_person_hash_key,
            sl.company_name,
            sl.date_of_constitution
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_crm sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hl.legal_person_hash_key AS sap_legal_person_hash_key,
            sl.organization,
            sl.org_establishment_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_sap sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.company_name)) = upper(trim(sap.organization))
     AND crm.date_of_constitution = sap.org_establishment_date
),
person_match AS (
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM legal_match
),
crm_policy_person_anchor AS (
    SELECT DISTINCT
        pm.global_person_identifier,
        pm.match_type,
        pm.crm_person_id,
        pm.sap_person_id,
        pm.crm_person_hash_key,
        pm.sap_person_hash_key,
        hc.customer_id AS crm_customer_id,
        hp.policy_id AS crm_policy_id,
        hp.policy_hash_key AS crm_policy_hash_key
    FROM person_match pm
    JOIN raw_vault.link_customer_person lcp
      ON lcp.person_hash_key = pm.crm_person_hash_key
    JOIN raw_vault.hub_customer hc
      ON hc.customer_hash_key = lcp.customer_hash_key
     AND hc.record_source = 'CRM'
    JOIN raw_vault.link_policy_customer lpc
      ON lpc.customer_hash_key = hc.customer_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpc.policy_hash_key
     AND hp.record_source = 'CRM'
),
latest_policy_product AS (
    SELECT *
    FROM raw_vault.link_policy_product
    QUALIFY row_number() OVER (
        PARTITION BY policy_hash_key
        ORDER BY load_date DESC
    ) = 1
),
crm_home_anchor AS (
    SELECT DISTINCT
        ppa.global_person_identifier,
        ppa.match_type,
        ppa.crm_person_id,
        ppa.sap_person_id,
        ppa.crm_customer_id,
        ppa.crm_policy_id,
        hh.insured_object_home_id AS crm_home_id,
        hh.home_hash_key AS crm_home_hash_key,
        hh.load_date AS crm_home_load_date,
        hp.product_id AS crm_product_id,
        shc.wall_construction,
        shc.home_risk_address,
        shc.roof_construction,
        shc.home_type,
        shc.home_state,
        shc.is_existing_home_customer
    FROM crm_policy_person_anchor ppa
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.policy_hash_key = ppa.crm_policy_hash_key
    JOIN raw_vault.link_insured_object_home lioh
      ON lioh.insured_object_hash_key = lpio.insured_object_hash_key
    JOIN raw_vault.hub_home hh
      ON hh.home_hash_key = lioh.home_hash_key
     AND hh.record_source = 'CRM'
    JOIN raw_vault.sat_home_crm shc
      ON shc.home_hash_key = hh.home_hash_key
    LEFT JOIN latest_policy_product lpp
      ON lpp.policy_hash_key = ppa.crm_policy_hash_key
    LEFT JOIN raw_vault.hub_product hp
      ON hp.product_hash_key = lpp.product_hash_key
),
home_rows AS (
    SELECT
        concat(ch.crm_home_id, '||', hsap.insured_object_home_id) AS global_home_identifier,
        ch.crm_home_id,
        ch.crm_home_hash_key,
        ch.crm_home_load_date,
        hsap.insured_object_home_id AS sap_home_id,
        hsap.home_hash_key AS sap_home_hash_key,
        hsap.load_date AS sap_home_load_date
    FROM crm_home_anchor ch
    JOIN raw_vault.sat_home_sap shs
      ON regexp_replace(shs.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(ch.crm_policy_id, '^(SAP_|CRM_)', '')
    JOIN raw_vault.hub_home hsap
      ON hsap.home_hash_key = shs.home_hash_key
     AND hsap.record_source = 'SAP'
    QUALIFY row_number() OVER (
        PARTITION BY concat(ch.crm_home_id, '||', hsap.insured_object_home_id)
        ORDER BY ch.crm_home_hash_key, hsap.home_hash_key
    ) = 1
)
SELECT
    global_home_identifier,
    'CRM' AS rawdv_source_name,
    crm_home_id AS rawdv_source_business_key,
    crm_home_hash_key AS rawdv_hashkey,
    crm_home_load_date AS load_timestamp
FROM home_rows
UNION ALL
SELECT
    global_home_identifier,
    'SAP' AS rawdv_source_name,
    sap_home_id AS rawdv_source_business_key,
    sap_home_hash_key AS rawdv_hashkey,
    sap_home_load_date AS load_timestamp
FROM home_rows;

INSERT INTO business_vault.bv_motor_master
WITH
natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'NATURAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hn.natural_person_hash_key AS crm_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.birth_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_crm sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hn.natural_person_hash_key AS sap_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.date_of_birth
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_sap sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.first_name)) = upper(trim(sap.first_name))
     AND upper(trim(crm.last_name)) = upper(trim(sap.last_name))
     AND crm.birth_date = sap.date_of_birth
),
legal_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_legal_person_hash_key,
        sap.sap_legal_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'LEGAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hl.legal_person_hash_key AS crm_legal_person_hash_key,
            sl.company_name,
            sl.date_of_constitution
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_crm sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hl.legal_person_hash_key AS sap_legal_person_hash_key,
            sl.organization,
            sl.org_establishment_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_sap sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.company_name)) = upper(trim(sap.organization))
     AND crm.date_of_constitution = sap.org_establishment_date
),
person_match AS (
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM legal_match
),
crm_policy_person_anchor AS (
    SELECT DISTINCT
        pm.global_person_identifier,
        pm.match_type,
        pm.crm_person_id,
        pm.sap_person_id,
        pm.crm_person_hash_key,
        pm.sap_person_hash_key,
        hc.customer_id AS crm_customer_id,
        hp.policy_id AS crm_policy_id,
        hp.policy_hash_key AS crm_policy_hash_key
    FROM person_match pm
    JOIN raw_vault.link_customer_person lcp
      ON lcp.person_hash_key = pm.crm_person_hash_key
    JOIN raw_vault.hub_customer hc
      ON hc.customer_hash_key = lcp.customer_hash_key
     AND hc.record_source = 'CRM'
    JOIN raw_vault.link_policy_customer lpc
      ON lpc.customer_hash_key = hc.customer_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpc.policy_hash_key
     AND hp.record_source = 'CRM'
),
latest_policy_product AS (
    SELECT *
    FROM raw_vault.link_policy_product
    QUALIFY row_number() OVER (
        PARTITION BY policy_hash_key
        ORDER BY load_date DESC
    ) = 1
),
crm_motor_anchor AS (
    SELECT DISTINCT
        ppa.global_person_identifier,
        ppa.match_type,
        ppa.crm_person_id,
        ppa.sap_person_id,
        ppa.crm_customer_id,
        ppa.crm_policy_id,
        hm.insured_object_motor_id AS crm_motor_id,
        hm.motor_hash_key AS crm_motor_hash_key,
        hm.load_date AS crm_motor_load_date,
        hp.product_id AS crm_product_id,
        smc.auto_decline_vehicle,
        smc.body_type,
        smc.fuel_type,
        smc.license_status,
        smc.is_existing_motor_customer,
        smc.motor_lapsed_policies,
        smc.motor_risk_address,
        smc.risk_class_code,
        smc.variant,
        smc.vehicle_owner_type,
        smc.vehicle_regstate,
        smc.vehicle_class,
        smc.vehicle_model,
        smc.vehicle_type,
        smc.motor_sum_insrd,
        smc.vehicle_year,
        smc.vehicle_age,
        smc.driver_experience_years
    FROM crm_policy_person_anchor ppa
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.policy_hash_key = ppa.crm_policy_hash_key
    JOIN raw_vault.link_insured_object_motor liom
      ON liom.insured_object_hash_key = lpio.insured_object_hash_key
    JOIN raw_vault.hub_motor hm
      ON hm.motor_hash_key = liom.motor_hash_key
     AND hm.record_source = 'CRM'
    JOIN raw_vault.sat_motor_crm smc
      ON smc.motor_hash_key = hm.motor_hash_key
    LEFT JOIN latest_policy_product lpp
      ON lpp.policy_hash_key = ppa.crm_policy_hash_key
    LEFT JOIN raw_vault.hub_product hp
      ON hp.product_hash_key = lpp.product_hash_key
),
motor_rows AS (
    SELECT
        concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id) AS global_motor_identifier,
        concat(cm.crm_motor_hash_key, '||', hsap.motor_hash_key) AS master_motor_hash_key,
        concat(cm.crm_policy_id, '||', sms.policy_id) AS policy_identifier,
        coalesce(nullif(sms.motor_class, ''), nullif(cm.vehicle_class, '')) AS motor_class,
        coalesce(nullif(sms.motor_model, ''), nullif(cm.vehicle_model, '')) AS motor_model,
        coalesce(nullif(sms.motor_type, ''), nullif(cm.vehicle_type, '')) AS motor_type,
        cm.variant AS motor_variant,
        cm.body_type,
        coalesce(nullif(sms.fuel_type, ''), nullif(cm.fuel_type, '')) AS fuel_type,
        sms.gear_type,
        sms.body_colour,
        sms.motor_parked_location,
        cm.vehicle_regstate AS motor_registration_state,
        to_date(nullif(coalesce(sms.manufacturing_date, cm.vehicle_year), ''), 'yyyy') AS motor_manufacturing_date,
        cast(nullif(cm.vehicle_age, '') AS int) AS motor_age,
        cm.risk_class_code AS motor_risk_class_code,
        cm.vehicle_owner_type AS motor_owner_type,
        cast(nullif(cm.driver_experience_years, '') AS int) AS driver_experience_years,
        cm.license_status,
        cast(nullif(cm.motor_sum_insrd, '') AS double) AS motor_sum_insured,
        cm.is_existing_motor_customer,
        cm.motor_lapsed_policies,
        concat(cm.crm_product_id, '||', sms.product_id) AS global_product_identifier
    FROM crm_motor_anchor cm
    JOIN raw_vault.sat_motor_sap sms
      ON regexp_replace(sms.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(cm.crm_policy_id, '^(SAP_|CRM_)', '')
    JOIN raw_vault.hub_motor hsap
      ON hsap.motor_hash_key = sms.motor_hash_key
     AND hsap.record_source = 'SAP'
    QUALIFY row_number() OVER (
        PARTITION BY concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id)
        ORDER BY cm.crm_motor_hash_key, hsap.motor_hash_key
    ) = 1
)
SELECT
    global_motor_identifier,
    master_motor_hash_key,
    policy_identifier,
    motor_class,
    motor_model,
    motor_type,
    motor_variant,
    body_type,
    fuel_type,
    gear_type,
    body_colour,
    motor_parked_location,
    motor_registration_state,
    motor_manufacturing_date,
    motor_age,
    motor_risk_class_code,
    motor_owner_type,
    driver_experience_years,
    license_status,
    motor_sum_insured,
    is_existing_motor_customer,
    motor_lapsed_policies,
    current_date() AS effective_from,
    DATE '9999-12-31' AS effective_to,
    'Y' AS current_active_flag,
    global_product_identifier
FROM motor_rows;

INSERT INTO business_vault.bv_motor_master_xref
WITH
natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'NATURAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hn.natural_person_hash_key AS crm_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.birth_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_crm sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hn.natural_person_hash_key AS sap_natural_person_hash_key,
            sn.first_name,
            sn.last_name,
            sn.date_of_birth
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN raw_vault.sat_natural_person_sap sn
          ON sn.natural_person_hash_key = hn.natural_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.first_name)) = upper(trim(sap.first_name))
     AND upper(trim(crm.last_name)) = upper(trim(sap.last_name))
     AND crm.birth_date = sap.date_of_birth
),
legal_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_legal_person_hash_key,
        sap.sap_legal_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier,
        'LEGAL' AS match_type
    FROM (
        SELECT
            hp.person_id AS crm_person_id,
            hp.person_hash_key AS crm_person_hash_key,
            hl.legal_person_hash_key AS crm_legal_person_hash_key,
            sl.company_name,
            sl.date_of_constitution
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_crm sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'CRM'
    ) crm
    JOIN (
        SELECT
            hp.person_id AS sap_person_id,
            hp.person_hash_key AS sap_person_hash_key,
            hl.legal_person_hash_key AS sap_legal_person_hash_key,
            sl.organization,
            sl.org_establishment_date
        FROM raw_vault.hub_person hp
        JOIN raw_vault.link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN raw_vault.hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN raw_vault.sat_legal_person_sap sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.company_name)) = upper(trim(sap.organization))
     AND crm.date_of_constitution = sap.org_establishment_date
),
person_match AS (
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key,
        match_type
    FROM legal_match
),
crm_policy_person_anchor AS (
    SELECT DISTINCT
        pm.global_person_identifier,
        pm.match_type,
        pm.crm_person_id,
        pm.sap_person_id,
        pm.crm_person_hash_key,
        pm.sap_person_hash_key,
        hc.customer_id AS crm_customer_id,
        hp.policy_id AS crm_policy_id,
        hp.policy_hash_key AS crm_policy_hash_key
    FROM person_match pm
    JOIN raw_vault.link_customer_person lcp
      ON lcp.person_hash_key = pm.crm_person_hash_key
    JOIN raw_vault.hub_customer hc
      ON hc.customer_hash_key = lcp.customer_hash_key
     AND hc.record_source = 'CRM'
    JOIN raw_vault.link_policy_customer lpc
      ON lpc.customer_hash_key = hc.customer_hash_key
    JOIN raw_vault.hub_policy hp
      ON hp.policy_hash_key = lpc.policy_hash_key
     AND hp.record_source = 'CRM'
),
latest_policy_product AS (
    SELECT *
    FROM raw_vault.link_policy_product
    QUALIFY row_number() OVER (
        PARTITION BY policy_hash_key
        ORDER BY load_date DESC
    ) = 1
),
crm_motor_anchor AS (
    SELECT DISTINCT
        ppa.global_person_identifier,
        ppa.match_type,
        ppa.crm_person_id,
        ppa.sap_person_id,
        ppa.crm_customer_id,
        ppa.crm_policy_id,
        hm.insured_object_motor_id AS crm_motor_id,
        hm.motor_hash_key AS crm_motor_hash_key,
        hm.load_date AS crm_motor_load_date,
        hp.product_id AS crm_product_id,
        smc.auto_decline_vehicle,
        smc.body_type,
        smc.fuel_type,
        smc.license_status,
        smc.is_existing_motor_customer,
        smc.motor_lapsed_policies,
        smc.motor_risk_address,
        smc.risk_class_code,
        smc.variant,
        smc.vehicle_owner_type,
        smc.vehicle_regstate,
        smc.vehicle_class,
        smc.vehicle_model,
        smc.vehicle_type,
        smc.motor_sum_insrd,
        smc.vehicle_year,
        smc.vehicle_age,
        smc.driver_experience_years
    FROM crm_policy_person_anchor ppa
    JOIN raw_vault.link_policy_insured_object lpio
      ON lpio.policy_hash_key = ppa.crm_policy_hash_key
    JOIN raw_vault.link_insured_object_motor liom
      ON liom.insured_object_hash_key = lpio.insured_object_hash_key
    JOIN raw_vault.hub_motor hm
      ON hm.motor_hash_key = liom.motor_hash_key
     AND hm.record_source = 'CRM'
    JOIN raw_vault.sat_motor_crm smc
      ON smc.motor_hash_key = hm.motor_hash_key
    LEFT JOIN latest_policy_product lpp
      ON lpp.policy_hash_key = ppa.crm_policy_hash_key
    LEFT JOIN raw_vault.hub_product hp
      ON hp.product_hash_key = lpp.product_hash_key
),
motor_rows AS (
    SELECT
        concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id) AS global_motor_identifier,
        cm.crm_motor_id,
        cm.crm_motor_hash_key,
        cm.crm_motor_load_date,
        hsap.insured_object_motor_id AS sap_motor_id,
        hsap.motor_hash_key AS sap_motor_hash_key,
        hsap.load_date AS sap_motor_load_date
    FROM crm_motor_anchor cm
    JOIN raw_vault.sat_motor_sap sms
      ON regexp_replace(sms.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(cm.crm_policy_id, '^(SAP_|CRM_)', '')
    JOIN raw_vault.hub_motor hsap
      ON hsap.motor_hash_key = sms.motor_hash_key
     AND hsap.record_source = 'SAP'
    QUALIFY row_number() OVER (
        PARTITION BY concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id)
        ORDER BY cm.crm_motor_hash_key, hsap.motor_hash_key
    ) = 1
)
SELECT
    global_motor_identifier,
    'CRM' AS rawdv_source_name,
    crm_motor_id AS rawdv_source_business_key,
    crm_motor_hash_key AS rawdv_hashkey,
    crm_motor_load_date AS load_timestamp
FROM motor_rows
UNION ALL
SELECT
    global_motor_identifier,
    'SAP' AS rawdv_source_name,
    sap_motor_id AS rawdv_source_business_key,
    sap_motor_hash_key AS rawdv_hashkey,
    sap_motor_load_date AS load_timestamp
FROM motor_rows;

-- Validation counts for Databricks SQL.
SELECT '01_bv_home_master' AS check_name, count(*) AS row_count FROM business_vault.bv_home_master
UNION ALL
SELECT '02_bv_home_master_xref', count(*) FROM business_vault.bv_home_master_xref
UNION ALL
SELECT '03_bv_motor_master', count(*) FROM business_vault.bv_motor_master
UNION ALL
SELECT '04_bv_motor_master_xref', count(*) FROM business_vault.bv_motor_master_xref
ORDER BY check_name;
