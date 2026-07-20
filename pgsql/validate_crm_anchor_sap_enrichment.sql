-- Validate CRM-anchor / SAP-enrichment flow for BV home and motor.
--
-- Intended logic:
--   1. Match CRM and SAP persons using BV natural/legal person rules.
--   2. Use CRM-only person -> customer -> policy -> insured object links as the ownership anchor.
--   3. Use SAP home/motor rows as enrichment.
--   4. Compare the generated home/motor master keys with the current Business Vault masters.

SET search_path TO raw_vault, business_vault, public;

DROP VIEW IF EXISTS tmp_bv_person_match;
CREATE TEMP VIEW tmp_bv_person_match AS
WITH natural_match AS (
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
        FROM hub_person hp
        JOIN link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN sat_natural_person_crm sn
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
        FROM hub_person hp
        JOIN link_person_natural_person lpn
          ON lpn.person_hash_key = hp.person_hash_key
        JOIN hub_natural_person hn
          ON hn.natural_person_hash_key = lpn.natural_person_hash_key
        JOIN sat_natural_person_sap sn
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
        FROM hub_person hp
        JOIN link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN sat_legal_person_crm sl
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
        FROM hub_person hp
        JOIN link_person_legal_person lpl
          ON lpl.person_hash_key = hp.person_hash_key
        JOIN hub_legal_person hl
          ON hl.legal_person_hash_key = lpl.legal_person_hash_key
        JOIN sat_legal_person_sap sl
          ON sl.legal_person_hash_key = hl.legal_person_hash_key
        WHERE hp.record_source = 'SAP'
    ) sap
      ON upper(trim(crm.company_name)) = upper(trim(sap.organization))
     AND crm.date_of_constitution = sap.org_establishment_date
)
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
FROM legal_match;

DROP VIEW IF EXISTS tmp_crm_policy_person_anchor;
CREATE TEMP VIEW tmp_crm_policy_person_anchor AS
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
FROM tmp_bv_person_match pm
JOIN link_customer_person lcp
  ON lcp.person_hash_key = pm.crm_person_hash_key
JOIN hub_customer hc
  ON hc.customer_hash_key = lcp.customer_hash_key
 AND hc.record_source = 'CRM'
JOIN link_policy_customer lpc
  ON lpc.customer_hash_key = hc.customer_hash_key
JOIN hub_policy hp
  ON hp.policy_hash_key = lpc.policy_hash_key
 AND hp.record_source = 'CRM';

DROP VIEW IF EXISTS tmp_crm_home_anchor;
CREATE TEMP VIEW tmp_crm_home_anchor AS
WITH latest_policy_product AS (
    SELECT *
    FROM (
        SELECT
            lpp.*,
            row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
        FROM link_policy_product lpp
    ) x
    WHERE rn = 1
)
SELECT DISTINCT
    ppa.global_person_identifier,
    ppa.match_type,
    ppa.crm_person_id,
    ppa.sap_person_id,
    ppa.crm_customer_id,
    ppa.crm_policy_id,
    hh.insured_object_home_id AS crm_home_id,
    hh.home_hash_key AS crm_home_hash_key,
    hp.product_id AS crm_product_id,
    shc.home_type AS crm_home_type,
    shc.home_state,
    shc.home_risk_address
FROM tmp_crm_policy_person_anchor ppa
JOIN link_policy_insured_object lpio
  ON lpio.policy_hash_key = ppa.crm_policy_hash_key
JOIN link_insured_object_home lioh
  ON lioh.insured_object_hash_key = lpio.insured_object_hash_key
JOIN hub_home hh
  ON hh.home_hash_key = lioh.home_hash_key
 AND hh.record_source = 'CRM'
JOIN sat_home_crm shc
  ON shc.home_hash_key = hh.home_hash_key
LEFT JOIN latest_policy_product lpp
  ON lpp.policy_hash_key = ppa.crm_policy_hash_key
LEFT JOIN hub_product hp
  ON hp.product_hash_key = lpp.product_hash_key;

DROP VIEW IF EXISTS tmp_crm_motor_anchor;
CREATE TEMP VIEW tmp_crm_motor_anchor AS
WITH latest_policy_product AS (
    SELECT *
    FROM (
        SELECT
            lpp.*,
            row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
        FROM link_policy_product lpp
    ) x
    WHERE rn = 1
)
SELECT DISTINCT
    ppa.global_person_identifier,
    ppa.match_type,
    ppa.crm_person_id,
    ppa.sap_person_id,
    ppa.crm_customer_id,
    ppa.crm_policy_id,
    hm.insured_object_motor_id AS crm_motor_id,
    hm.motor_hash_key AS crm_motor_hash_key,
    hp.product_id AS crm_product_id,
    smc.vehicle_class,
    smc.vehicle_model,
    smc.vehicle_type,
    smc.vehicle_regstate
FROM tmp_crm_policy_person_anchor ppa
JOIN link_policy_insured_object lpio
  ON lpio.policy_hash_key = ppa.crm_policy_hash_key
JOIN link_insured_object_motor liom
  ON liom.insured_object_hash_key = lpio.insured_object_hash_key
JOIN hub_motor hm
  ON hm.motor_hash_key = liom.motor_hash_key
 AND hm.record_source = 'CRM'
JOIN sat_motor_crm smc
  ON smc.motor_hash_key = hm.motor_hash_key
LEFT JOIN latest_policy_product lpp
  ON lpp.policy_hash_key = ppa.crm_policy_hash_key
LEFT JOIN hub_product hp
  ON hp.product_hash_key = lpp.product_hash_key;

DROP VIEW IF EXISTS tmp_proposed_bv_home;
CREATE TEMP VIEW tmp_proposed_bv_home AS
SELECT DISTINCT ON (concat(ch.crm_home_id, '||', hsap.insured_object_home_id))
    ch.global_person_identifier,
    ch.match_type,
    ch.crm_person_id,
    ch.sap_person_id,
    ch.crm_customer_id,
    ch.crm_policy_id,
    shs.policy_id AS sap_policy_id,
    concat(ch.crm_home_id, '||', hsap.insured_object_home_id) AS global_home_identifier,
    concat(ch.crm_home_hash_key, '||', hsap.home_hash_key) AS master_home_hash_key,
    concat(ch.crm_policy_id, '||', shs.policy_id) AS policy_identifier,
    concat(ch.crm_product_id, '||', shs.product_id) AS global_product_identifier,
    ch.crm_home_id,
    hsap.insured_object_home_id AS sap_home_id
FROM tmp_crm_home_anchor ch
JOIN sat_home_sap shs
  ON regexp_replace(shs.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(ch.crm_policy_id, '^(SAP_|CRM_)', '')
JOIN hub_home hsap
  ON hsap.home_hash_key = shs.home_hash_key
 AND hsap.record_source = 'SAP';

DROP VIEW IF EXISTS tmp_proposed_bv_motor;
CREATE TEMP VIEW tmp_proposed_bv_motor AS
SELECT DISTINCT ON (concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id))
    cm.global_person_identifier,
    cm.match_type,
    cm.crm_person_id,
    cm.sap_person_id,
    cm.crm_customer_id,
    cm.crm_policy_id,
    sms.policy_id AS sap_policy_id,
    concat(cm.crm_motor_id, '||', hsap.insured_object_motor_id) AS global_motor_identifier,
    concat(cm.crm_motor_hash_key, '||', hsap.motor_hash_key) AS master_motor_hash_key,
    concat(cm.crm_policy_id, '||', sms.policy_id) AS policy_identifier,
    concat(cm.crm_product_id, '||', sms.product_id) AS global_product_identifier,
    cm.crm_motor_id,
    hsap.insured_object_motor_id AS sap_motor_id
FROM tmp_crm_motor_anchor cm
JOIN sat_motor_sap sms
  ON regexp_replace(sms.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(cm.crm_policy_id, '^(SAP_|CRM_)', '')
JOIN hub_motor hsap
  ON hsap.motor_hash_key = sms.motor_hash_key
 AND hsap.record_source = 'SAP';

-- Summary: proposed CRM-anchor/SAP-enrichment flow.
SELECT '01_person_matches_total' AS check_name, count(*) AS row_count FROM tmp_bv_person_match
UNION ALL
SELECT '02_person_matches_natural', count(*) FROM tmp_bv_person_match WHERE match_type = 'NATURAL'
UNION ALL
SELECT '03_person_matches_legal', count(*) FROM tmp_bv_person_match WHERE match_type = 'LEGAL'
UNION ALL
SELECT '04_crm_policy_person_anchors', count(*) FROM tmp_crm_policy_person_anchor
UNION ALL
SELECT '05_crm_home_anchors', count(*) FROM tmp_crm_home_anchor
UNION ALL
SELECT '06_crm_motor_anchors', count(*) FROM tmp_crm_motor_anchor
UNION ALL
SELECT '07_proposed_home_rows', count(*) FROM tmp_proposed_bv_home
UNION ALL
SELECT '08_current_bv_home_rows', count(*) FROM business_vault.bv_home_master
UNION ALL
SELECT '09_proposed_motor_rows', count(*) FROM tmp_proposed_bv_motor
UNION ALL
SELECT '10_current_bv_motor_rows', count(*) FROM business_vault.bv_motor_master
ORDER BY check_name;

-- Difference check: these should be zero when proposed flow matches current BV.
SELECT 'home_missing_from_current_bv' AS diff_check, count(*) AS row_count
FROM (
    SELECT global_home_identifier FROM tmp_proposed_bv_home
    EXCEPT
    SELECT global_home_identifier FROM business_vault.bv_home_master
) x
UNION ALL
SELECT 'home_extra_in_current_bv', count(*)
FROM (
    SELECT global_home_identifier FROM business_vault.bv_home_master
    EXCEPT
    SELECT global_home_identifier FROM tmp_proposed_bv_home
) x
UNION ALL
SELECT 'motor_missing_from_current_bv', count(*)
FROM (
    SELECT global_motor_identifier FROM tmp_proposed_bv_motor
    EXCEPT
    SELECT global_motor_identifier FROM business_vault.bv_motor_master
) x
UNION ALL
SELECT 'motor_extra_in_current_bv', count(*)
FROM (
    SELECT global_motor_identifier FROM business_vault.bv_motor_master
    EXCEPT
    SELECT global_motor_identifier FROM tmp_proposed_bv_motor
) x
ORDER BY diff_check;

-- Unmatched anchor check: CRM policy/object rows that cannot receive SAP enrichment.
SELECT 'home_anchor_without_sap_enrichment' AS gap_check, count(*) AS row_count
FROM tmp_crm_home_anchor ch
LEFT JOIN tmp_proposed_bv_home ph
  ON ph.crm_home_id = ch.crm_home_id
WHERE ph.crm_home_id IS NULL
UNION ALL
SELECT 'motor_anchor_without_sap_enrichment', count(*)
FROM tmp_crm_motor_anchor cm
LEFT JOIN tmp_proposed_bv_motor pm
  ON pm.crm_motor_id = cm.crm_motor_id
WHERE pm.crm_motor_id IS NULL
UNION ALL
SELECT 'home_enriched_with_matched_person', count(*)
FROM tmp_proposed_bv_home
UNION ALL
SELECT 'motor_enriched_with_matched_person', count(*)
FROM tmp_proposed_bv_motor
ORDER BY gap_check;

-- Sample rows for inspection.
SELECT
    'HOME' AS object_type,
    global_person_identifier,
    match_type,
    crm_person_id,
    sap_person_id,
    crm_policy_id,
    sap_policy_id,
    global_home_identifier AS global_object_identifier,
    global_product_identifier
FROM tmp_proposed_bv_home
ORDER BY global_home_identifier
LIMIT 5;

SELECT
    'MOTOR' AS object_type,
    global_person_identifier,
    match_type,
    crm_person_id,
    sap_person_id,
    crm_policy_id,
    sap_policy_id,
    global_motor_identifier AS global_object_identifier,
    global_product_identifier
FROM tmp_proposed_bv_motor
ORDER BY global_motor_identifier
LIMIT 5;
