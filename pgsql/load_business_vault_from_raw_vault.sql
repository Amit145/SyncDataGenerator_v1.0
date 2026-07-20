-- Rebuild Business Vault from Raw Vault.

-- Source schema: raw_vault

-- Target schema: business_vault

-- Generated from pgsql/load_business_vault.py so PgAdmin can run the same logic directly.


DROP SCHEMA IF EXISTS business_vault CASCADE;
CREATE SCHEMA business_vault;

CREATE TABLE business_vault.bv_address_master (
    global_address_identifier TEXT PRIMARY KEY,
    master_address_hash_key TEXT,
    address_type TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    region TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT,
    global_person_identifier TEXT NOT NULL
);

CREATE TABLE business_vault.bv_address_master_xref (
    global_address_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_home_master (
    global_home_identifier TEXT PRIMARY KEY,
    master_home_hash_key TEXT,
    policy_identifier TEXT,
    home_type TEXT,
    home_location TEXT,
    home_state TEXT,
    wall_construction_type TEXT,
    roof_construction_type TEXT,
    is_existing_home_customer TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT,
    global_product_identifier TEXT NOT NULL
);

CREATE TABLE business_vault.bv_home_master_xref (
    global_home_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_legal_person_master (
    global_person_identifier TEXT PRIMARY KEY,
    master_legal_person_hash_key TEXT,
    tenant_identifier TEXT,
    is_lead TEXT,
    person_type TEXT,
    operational_paperless_consent TEXT,
    source_identifier TEXT,
    source_type TEXT,
    organization TEXT,
    org_establishment_date DATE,
    email_address TEXT,
    phone_number TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT
);

CREATE TABLE business_vault.bv_legal_person_master_xref (
    global_person_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_motor_master (
    global_motor_identifier TEXT PRIMARY KEY,
    master_motor_hash_key TEXT,
    policy_identifier TEXT,
    motor_class TEXT,
    motor_model TEXT,
    motor_type TEXT,
    motor_variant TEXT,
    body_type TEXT,
    fuel_type TEXT,
    gear_type TEXT,
    body_colour TEXT,
    motor_parked_location TEXT,
    motor_registration_state TEXT,
    motor_manufacturing_date DATE,
    motor_age INTEGER,
    motor_risk_class_code TEXT,
    motor_owner_type TEXT,
    driver_experience_years INTEGER,
    license_status TEXT,
    motor_sum_insured DOUBLE PRECISION,
    is_existing_motor_customer TEXT,
    motor_lapsed_policies TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT,
    global_product_identifier TEXT NOT NULL
);

CREATE TABLE business_vault.bv_motor_master_xref (
    global_motor_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_natural_person_master (
    global_person_identifier TEXT PRIMARY KEY,
    master_natural_person_hash_key TEXT,
    tenant_identifier TEXT,
    is_lead TEXT,
    person_type TEXT,
    operational_paperless_consent TEXT,
    source_identifier TEXT,
    source_type TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    full_name TEXT,
    date_of_birth DATE,
    gender TEXT,
    occupation TEXT,
    courtesy_title TEXT,
    role TEXT,
    nationality TEXT,
    marital_status TEXT,
    assesed_disability_degree TEXT,
    preferred_language TEXT,
    job_title TEXT,
    personal_email TEXT,
    work_email TEXT,
    work_phone TEXT,
    home_phone TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT
);

CREATE TABLE business_vault.bv_natural_person_master_xref (
    global_person_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_product_master (
    global_product_identifier TEXT PRIMARY KEY,
    master_product_hash_key TEXT,
    product_type TEXT,
    product_sub_type TEXT,
    product_name TEXT,
    product_launch_date DATE,
    product_status TEXT,
    product_line_of_business TEXT,
    underwriting_group TEXT,
    regulatory_approval_code TEXT,
    effective_from DATE,
    effective_to DATE,
    current_active_flag TEXT
);

CREATE TABLE business_vault.bv_product_master_xref (
    global_product_identifier TEXT NOT NULL,
    rawdv_source_name TEXT,
    rawdv_source_business_key TEXT,
    rawdv_hashkey TEXT,
    load_timestamp TIMESTAMP
);



INSERT INTO business_vault.bv_product_master
    WITH product_match AS (
        SELECT DISTINCT ON (concat(hcrm.product_id, '||', hsap.product_id))
            concat(hcrm.product_id, '||', hsap.product_id) AS global_product_identifier,
            concat(hcrm.product_hash_key, '||', hsap.product_hash_key) AS master_product_hash_key,
            sap.product_type,
            sap.product_sub_type,
            coalesce(nullif(sap.product_name, ''), nullif(crm.product_name, '')) AS product_name,
            nullif(crm.product_launch_date, '')::date AS product_launch_date,
            crm.product_status,
            sap.line_of_business AS product_line_of_business,
            crm.underwriting_group,
            crm.regulatory_approval_code
        FROM raw_vault.hub_product hcrm
        JOIN raw_vault.sat_product_crm crm
          ON crm.product_hash_key = hcrm.product_hash_key
        JOIN raw_vault.hub_product hsap
          ON hsap.record_source = 'SAP'
        JOIN raw_vault.sat_product_sap sap
          ON sap.product_hash_key = hsap.product_hash_key
        WHERE hcrm.record_source = 'CRM'
          AND upper(trim(crm.type)) = upper(trim(sap.product_type))
          AND upper(trim(crm.product_variant)) = upper(trim(sap.product_sub_type))
          AND upper(trim(crm.product_name)) = upper(trim(sap.product_name))
    )
    SELECT
        global_product_identifier,
        master_product_hash_key,
        product_type,
        product_sub_type,
        product_name,
        product_launch_date,
        product_status,
        product_line_of_business,
        underwriting_group,
        regulatory_approval_code,
        current_date,
        date '9999-12-31',
        'Y'
    FROM product_match;


INSERT INTO business_vault.bv_product_master_xref
    WITH product_match AS (
        SELECT DISTINCT
            concat(hcrm.product_id, '||', hsap.product_id) AS global_product_identifier,
            hcrm.record_source AS crm_source,
            hcrm.product_id AS crm_business_key,
            hcrm.product_hash_key AS crm_hash_key,
            hcrm.load_date AS crm_load_date,
            hsap.record_source AS sap_source,
            hsap.product_id AS sap_business_key,
            hsap.product_hash_key AS sap_hash_key,
            hsap.load_date AS sap_load_date
        FROM raw_vault.hub_product hcrm
        JOIN raw_vault.sat_product_crm crm ON crm.product_hash_key = hcrm.product_hash_key
        JOIN raw_vault.hub_product hsap ON hsap.record_source = 'SAP'
        JOIN raw_vault.sat_product_sap sap ON sap.product_hash_key = hsap.product_hash_key
        WHERE hcrm.record_source = 'CRM'
          AND upper(trim(crm.type)) = upper(trim(sap.product_type))
          AND upper(trim(crm.product_variant)) = upper(trim(sap.product_sub_type))
          AND upper(trim(crm.product_name)) = upper(trim(sap.product_name))
    )
    SELECT global_product_identifier, crm_source, crm_business_key, crm_hash_key, crm_load_date FROM product_match
    UNION ALL
    SELECT global_product_identifier, sap_source, sap_business_key, sap_hash_key, sap_load_date FROM product_match;


INSERT INTO business_vault.bv_natural_person_master
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)
,
    natural_rows AS (
        SELECT DISTINCT ON (nm.global_person_identifier)
            nm.global_person_identifier,
            concat(nm.crm_natural_person_hash_key, '||', nm.sap_natural_person_hash_key) AS master_natural_person_hash_key,
            spc.tenant_id,
            spc.is_lead,
            coalesce(nullif(sps.person_type, ''), nullif(spc.type, '')) AS person_type,
            spc.operational_paperless_consent,
            spc.source_id,
            spc.source_type,
            coalesce(nullif(sns.first_name, ''), nullif(snc.first_name, '')) AS first_name,
            sns.middle_name,
            coalesce(nullif(sns.last_name, ''), nullif(snc.last_name, '')) AS last_name,
            coalesce(nullif(concat_ws(' ', nullif(sns.first_name, ''), nullif(sns.middle_name, ''), nullif(sns.last_name, '')), ''), snc.full_name) AS full_name,
            coalesce(sns.date_of_birth, snc.birth_date) AS date_of_birth,
            coalesce(nullif(sns.gender, ''), nullif(snc.gender, '')) AS gender,
            coalesce(nullif(sns.occupation, ''), nullif(snc.occupation, '')) AS occupation,
            snc.courtesy_title,
            snc.role,
            snc.nationality,
            snc.marital_status,
            snc.assesed_disability_degree,
            snc.preferred_language,
            snc.job_title,
            sps.email_address AS personal_email,
            scc.work_email,
            scc.work_phone,
            coalesce(nullif(sps.phone_number, ''), nullif(scc.home_phone, '')) AS home_phone
        FROM natural_match nm
        JOIN raw_vault.sat_person_crm spc ON spc.person_hash_key = nm.crm_person_hash_key
        JOIN raw_vault.sat_person_sap sps ON sps.person_hash_key = nm.sap_person_hash_key
        JOIN raw_vault.sat_natural_person_crm snc ON snc.natural_person_hash_key = nm.crm_natural_person_hash_key
        JOIN raw_vault.sat_natural_person_sap sns ON sns.natural_person_hash_key = nm.sap_natural_person_hash_key
        LEFT JOIN raw_vault.link_person_contact lpc ON lpc.person_hash_key = nm.crm_person_hash_key
        LEFT JOIN raw_vault.sat_contact_crm scc ON scc.contact_hash_key = lpc.contact_hash_key
    )
    SELECT
        global_person_identifier,
        master_natural_person_hash_key,
        tenant_id,
        is_lead,
        person_type,
        operational_paperless_consent,
        source_id,
        source_type,
        first_name,
        middle_name,
        last_name,
        full_name,
        date_of_birth,
        gender,
        occupation,
        courtesy_title,
        role,
        nationality,
        marital_status,
        assesed_disability_degree,
        preferred_language,
        job_title,
        personal_email,
        work_email,
        work_phone,
        home_phone,
        current_date,
        date '9999-12-31',
        'Y'
    FROM natural_rows;


INSERT INTO business_vault.bv_natural_person_master_xref
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)

    SELECT nm.global_person_identifier, 'CRM', hn.natural_person_id, nm.crm_natural_person_hash_key, hn.load_date
    FROM natural_match nm
    JOIN raw_vault.hub_natural_person hn ON hn.natural_person_hash_key = nm.crm_natural_person_hash_key
    UNION ALL
    SELECT nm.global_person_identifier, 'SAP', hn.natural_person_id, nm.sap_natural_person_hash_key, hn.load_date
    FROM natural_match nm
    JOIN raw_vault.hub_natural_person hn ON hn.natural_person_hash_key = nm.sap_natural_person_hash_key;


INSERT INTO business_vault.bv_legal_person_master
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)
,
    legal_rows AS (
        SELECT DISTINCT ON (lm.global_person_identifier)
            lm.global_person_identifier,
            concat(lm.crm_legal_person_hash_key, '||', lm.sap_legal_person_hash_key) AS master_legal_person_hash_key,
            spc.tenant_id,
            spc.is_lead,
            coalesce(nullif(sps.person_type, ''), nullif(spc.type, '')) AS person_type,
            spc.operational_paperless_consent,
            slc.source_id,
            slc.source_type,
            coalesce(nullif(sls.organization, ''), nullif(slc.company_name, '')) AS organization,
            coalesce(sls.org_establishment_date, slc.date_of_constitution) AS org_establishment_date,
            sps.email_address,
            sps.phone_number
        FROM legal_match lm
        JOIN raw_vault.sat_person_crm spc ON spc.person_hash_key = lm.crm_person_hash_key
        JOIN raw_vault.sat_person_sap sps ON sps.person_hash_key = lm.sap_person_hash_key
        JOIN raw_vault.sat_legal_person_crm slc ON slc.legal_person_hash_key = lm.crm_legal_person_hash_key
        JOIN raw_vault.sat_legal_person_sap sls ON sls.legal_person_hash_key = lm.sap_legal_person_hash_key
    )
    SELECT
        global_person_identifier,
        master_legal_person_hash_key,
        tenant_id,
        is_lead,
        person_type,
        operational_paperless_consent,
        source_id,
        source_type,
        organization,
        org_establishment_date,
        email_address,
        phone_number,
        current_date,
        date '9999-12-31',
        'Y'
    FROM legal_rows;


INSERT INTO business_vault.bv_legal_person_master_xref
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)

    SELECT lm.global_person_identifier, 'CRM', hl.legal_person_id, lm.crm_legal_person_hash_key, hl.load_date
    FROM legal_match lm
    JOIN raw_vault.hub_legal_person hl ON hl.legal_person_hash_key = lm.crm_legal_person_hash_key
    UNION ALL
    SELECT lm.global_person_identifier, 'SAP', hl.legal_person_id, lm.sap_legal_person_hash_key, hl.load_date
    FROM legal_match lm
    JOIN raw_vault.hub_legal_person hl ON hl.legal_person_hash_key = lm.sap_legal_person_hash_key;


INSERT INTO business_vault.bv_address_master
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)
,
    address_rows AS (
        SELECT DISTINCT ON (concat(hac.address_id, '||', hasp.address_id))
            concat(hac.address_id, '||', hasp.address_id) AS global_address_identifier,
            concat(hac.address_hash_key, '||', hasp.address_hash_key) AS master_address_hash_key,
            sac.type AS address_type,
            sas.address_line_1,
            concat_ws(', ', nullif(sas.address_line_2, ''), nullif(sac.street, '')) AS address_line_2,
            coalesce(nullif(sac.city, ''), nullif(sas.city, '')) AS city,
            coalesce(nullif(sac.state, ''), nullif(sas.state, '')) AS state,
            coalesce(nullif(sac.postcode, ''), nullif(sas.zipcode, '')) AS postal_code,
            coalesce(nullif(sac.country, ''), nullif(sas.country, '')) AS country,
            sac.region,
            pm.global_person_identifier
        FROM person_match pm
        JOIN raw_vault.link_person_address lpac ON lpac.person_hash_key = pm.crm_person_hash_key
        JOIN raw_vault.hub_address hac ON hac.address_hash_key = lpac.address_hash_key AND hac.record_source = 'CRM'
        JOIN raw_vault.sat_address_crm sac ON sac.address_hash_key = hac.address_hash_key
        JOIN raw_vault.link_person_address lpas ON lpas.person_hash_key = pm.sap_person_hash_key
        JOIN raw_vault.hub_address hasp ON hasp.address_hash_key = lpas.address_hash_key AND hasp.record_source = 'SAP'
        JOIN raw_vault.sat_address_sap sas ON sas.address_hash_key = hasp.address_hash_key
    )
    SELECT
        global_address_identifier,
        master_address_hash_key,
        address_type,
        address_line_1,
        address_line_2,
        city,
        state,
        postal_code,
        country,
        region,
        current_date,
        date '9999-12-31',
        'Y',
        global_person_identifier
    FROM address_rows;


INSERT INTO business_vault.bv_address_master_xref
    
WITH natural_match AS (
    SELECT DISTINCT
        crm.crm_person_id,
        sap.sap_person_id,
        crm.crm_person_hash_key,
        sap.sap_person_hash_key,
        crm.crm_natural_person_hash_key,
        sap.sap_natural_person_hash_key,
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        concat(crm.crm_person_id, '||', sap.sap_person_id) AS global_person_identifier
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
        sap_person_hash_key
    FROM natural_match
    UNION
    SELECT
        global_person_identifier,
        crm_person_id,
        sap_person_id,
        crm_person_hash_key,
        sap_person_hash_key
    FROM legal_match
)
,
    address_rows AS (
        SELECT DISTINCT
            concat(hac.address_id, '||', hasp.address_id) AS global_address_identifier,
            hac.record_source AS crm_source,
            hac.address_id AS crm_business_key,
            hac.address_hash_key AS crm_hash_key,
            hac.load_date AS crm_load_date,
            hasp.record_source AS sap_source,
            hasp.address_id AS sap_business_key,
            hasp.address_hash_key AS sap_hash_key,
            hasp.load_date AS sap_load_date
        FROM person_match pm
        JOIN raw_vault.link_person_address lpac ON lpac.person_hash_key = pm.crm_person_hash_key
        JOIN raw_vault.hub_address hac ON hac.address_hash_key = lpac.address_hash_key AND hac.record_source = 'CRM'
        JOIN raw_vault.link_person_address lpas ON lpas.person_hash_key = pm.sap_person_hash_key
        JOIN raw_vault.hub_address hasp ON hasp.address_hash_key = lpas.address_hash_key AND hasp.record_source = 'SAP'
    )
    SELECT global_address_identifier, crm_source, crm_business_key, crm_hash_key, crm_load_date FROM address_rows
    UNION ALL
    SELECT global_address_identifier, sap_source, sap_business_key, sap_hash_key, sap_load_date FROM address_rows;


INSERT INTO business_vault.bv_home_master
    WITH latest_policy_product AS (
        SELECT * FROM (
            SELECT lpp.*, row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
            FROM raw_vault.link_policy_product lpp
        ) x WHERE rn = 1
    ),
    crm_home AS (
        SELECT DISTINCT
            hh.insured_object_home_id AS crm_home_id,
            hh.home_hash_key AS crm_home_hash_key,
            hp.policy_id AS crm_policy_id,
            hprod.product_id AS crm_product_id,
            shc.*
        FROM raw_vault.hub_home hh
        JOIN raw_vault.link_insured_object_home lioh ON lioh.home_hash_key = hh.home_hash_key
        JOIN raw_vault.link_policy_insured_object lpio ON lpio.insured_object_hash_key = lioh.insured_object_hash_key
        JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = lpio.policy_hash_key AND hp.record_source = 'CRM'
        LEFT JOIN latest_policy_product lpp ON lpp.policy_hash_key = hp.policy_hash_key
        LEFT JOIN raw_vault.hub_product hprod ON hprod.product_hash_key = lpp.product_hash_key
        JOIN raw_vault.sat_home_crm shc ON shc.home_hash_key = hh.home_hash_key
        WHERE hh.record_source = 'CRM'
    ),
    home_rows AS (
        SELECT DISTINCT ON (concat(crm.crm_home_id, '||', hsap.insured_object_home_id))
            concat(crm.crm_home_id, '||', hsap.insured_object_home_id) AS global_home_identifier,
            concat(crm.crm_home_hash_key, '||', hsap.home_hash_key) AS master_home_hash_key,
            concat(crm.crm_policy_id, '||', shs.policy_id) AS policy_identifier,
            coalesce(nullif(shs.home_type, ''), nullif(crm.home_type, '')) AS home_type,
            coalesce(nullif(shs.home_location, ''), nullif(crm.home_risk_address, '')) AS home_location,
            crm.home_state,
            coalesce(nullif(shs.wall_type, ''), nullif(crm.wall_construction, '')) AS wall_construction_type,
            coalesce(nullif(shs.roof_material, ''), nullif(crm.roof_construction, '')) AS roof_construction_type,
            crm.is_existing_home_customer,
            concat(crm.crm_product_id, '||', shs.product_id) AS global_product_identifier
        FROM crm_home crm
        JOIN raw_vault.sat_home_sap shs
          ON regexp_replace(shs.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(crm.crm_policy_id, '^(SAP_|CRM_)', '')
        JOIN raw_vault.hub_home hsap ON hsap.home_hash_key = shs.home_hash_key AND hsap.record_source = 'SAP'
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
        current_date,
        date '9999-12-31',
        'Y',
        global_product_identifier
    FROM home_rows;


INSERT INTO business_vault.bv_home_master_xref
    SELECT global_home_identifier, source_name, business_key, hash_key, load_date
    FROM (
        WITH latest_policy_product AS (
            SELECT * FROM (
                SELECT lpp.*, row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
                FROM raw_vault.link_policy_product lpp
            ) x WHERE rn = 1
        ),
        crm_home AS (
            SELECT DISTINCT hh.insured_object_home_id AS crm_home_id, hh.home_hash_key AS crm_home_hash_key, hh.load_date AS crm_load_date,
                   hp.policy_id AS crm_policy_id
            FROM raw_vault.hub_home hh
            JOIN raw_vault.link_insured_object_home lioh ON lioh.home_hash_key = hh.home_hash_key
            JOIN raw_vault.link_policy_insured_object lpio ON lpio.insured_object_hash_key = lioh.insured_object_hash_key
            JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = lpio.policy_hash_key AND hp.record_source = 'CRM'
            WHERE hh.record_source = 'CRM'
        ),
        rows AS (
            SELECT DISTINCT
                concat(crm.crm_home_id, '||', hsap.insured_object_home_id) AS global_home_identifier,
                crm.crm_home_id,
                crm.crm_home_hash_key,
                crm.crm_load_date,
                hsap.insured_object_home_id AS sap_home_id,
                hsap.home_hash_key AS sap_home_hash_key,
                hsap.load_date AS sap_load_date
            FROM crm_home crm
            JOIN raw_vault.sat_home_sap shs
              ON regexp_replace(shs.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(crm.crm_policy_id, '^(SAP_|CRM_)', '')
            JOIN raw_vault.hub_home hsap ON hsap.home_hash_key = shs.home_hash_key AND hsap.record_source = 'SAP'
        )
        SELECT global_home_identifier, 'CRM' AS source_name, crm_home_id AS business_key, crm_home_hash_key AS hash_key, crm_load_date AS load_date FROM rows
        UNION ALL
        SELECT global_home_identifier, 'SAP', sap_home_id, sap_home_hash_key, sap_load_date FROM rows
    ) x;


INSERT INTO business_vault.bv_motor_master
    WITH latest_policy_product AS (
        SELECT * FROM (
            SELECT lpp.*, row_number() OVER (PARTITION BY policy_hash_key ORDER BY load_date DESC) AS rn
            FROM raw_vault.link_policy_product lpp
        ) x WHERE rn = 1
    ),
    crm_motor AS (
        SELECT DISTINCT
            hm.insured_object_motor_id AS crm_motor_id,
            hm.motor_hash_key AS crm_motor_hash_key,
            hp.policy_id AS crm_policy_id,
            hprod.product_id AS crm_product_id,
            smc.*
        FROM raw_vault.hub_motor hm
        JOIN raw_vault.link_insured_object_motor liom ON liom.motor_hash_key = hm.motor_hash_key
        JOIN raw_vault.link_policy_insured_object lpio ON lpio.insured_object_hash_key = liom.insured_object_hash_key
        JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = lpio.policy_hash_key AND hp.record_source = 'CRM'
        LEFT JOIN latest_policy_product lpp ON lpp.policy_hash_key = hp.policy_hash_key
        LEFT JOIN raw_vault.hub_product hprod ON hprod.product_hash_key = lpp.product_hash_key
        JOIN raw_vault.sat_motor_crm smc ON smc.motor_hash_key = hm.motor_hash_key
        WHERE hm.record_source = 'CRM'
    ),
    motor_rows AS (
        SELECT DISTINCT ON (concat(crm.crm_motor_id, '||', hsap.insured_object_motor_id))
            concat(crm.crm_motor_id, '||', hsap.insured_object_motor_id) AS global_motor_identifier,
            concat(crm.crm_motor_hash_key, '||', hsap.motor_hash_key) AS master_motor_hash_key,
            concat(crm.crm_policy_id, '||', sms.policy_id) AS policy_identifier,
            coalesce(nullif(sms.motor_class, ''), nullif(crm.vehicle_class, '')) AS motor_class,
            coalesce(nullif(sms.motor_model, ''), nullif(crm.vehicle_model, '')) AS motor_model,
            coalesce(nullif(sms.motor_type, ''), nullif(crm.vehicle_type, '')) AS motor_type,
            crm.variant AS motor_variant,
            crm.body_type,
            coalesce(nullif(sms.fuel_type, ''), nullif(crm.fuel_type, '')) AS fuel_type,
            sms.gear_type,
            sms.body_colour,
            sms.motor_parked_location,
            crm.vehicle_regstate AS motor_registration_state,
            to_date(nullif(coalesce(sms.manufacturing_date, crm.vehicle_year), ''), 'YYYY') AS motor_manufacturing_date,
            nullif(crm.vehicle_age, '')::integer AS motor_age,
            crm.risk_class_code AS motor_risk_class_code,
            crm.vehicle_owner_type AS motor_owner_type,
            nullif(crm.driver_experience_years, '')::integer AS driver_experience_years,
            crm.license_status,
            nullif(crm.motor_sum_insrd, '')::double precision AS motor_sum_insured,
            crm.is_existing_motor_customer,
            crm.motor_lapsed_policies,
            concat(crm.crm_product_id, '||', sms.product_id) AS global_product_identifier
        FROM crm_motor crm
        JOIN raw_vault.sat_motor_sap sms
          ON regexp_replace(sms.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(crm.crm_policy_id, '^(SAP_|CRM_)', '')
        JOIN raw_vault.hub_motor hsap ON hsap.motor_hash_key = sms.motor_hash_key AND hsap.record_source = 'SAP'
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
        current_date,
        date '9999-12-31',
        'Y',
        global_product_identifier
    FROM motor_rows;


INSERT INTO business_vault.bv_motor_master_xref
    SELECT global_motor_identifier, source_name, business_key, hash_key, load_date
    FROM (
        WITH crm_motor AS (
            SELECT DISTINCT hm.insured_object_motor_id AS crm_motor_id, hm.motor_hash_key AS crm_motor_hash_key, hm.load_date AS crm_load_date,
                   hp.policy_id AS crm_policy_id
            FROM raw_vault.hub_motor hm
            JOIN raw_vault.link_insured_object_motor liom ON liom.motor_hash_key = hm.motor_hash_key
            JOIN raw_vault.link_policy_insured_object lpio ON lpio.insured_object_hash_key = liom.insured_object_hash_key
            JOIN raw_vault.hub_policy hp ON hp.policy_hash_key = lpio.policy_hash_key AND hp.record_source = 'CRM'
            WHERE hm.record_source = 'CRM'
        ),
        rows AS (
            SELECT DISTINCT
                concat(crm.crm_motor_id, '||', hsap.insured_object_motor_id) AS global_motor_identifier,
                crm.crm_motor_id,
                crm.crm_motor_hash_key,
                crm.crm_load_date,
                hsap.insured_object_motor_id AS sap_motor_id,
                hsap.motor_hash_key AS sap_motor_hash_key,
                hsap.load_date AS sap_load_date
            FROM crm_motor crm
            JOIN raw_vault.sat_motor_sap sms
              ON regexp_replace(sms.policy_id, '^(SAP_|CRM_)', '') = regexp_replace(crm.crm_policy_id, '^(SAP_|CRM_)', '')
            JOIN raw_vault.hub_motor hsap ON hsap.motor_hash_key = sms.motor_hash_key AND hsap.record_source = 'SAP'
        )
        SELECT global_motor_identifier, 'CRM' AS source_name, crm_motor_id AS business_key, crm_motor_hash_key AS hash_key, crm_load_date AS load_date FROM rows
        UNION ALL
        SELECT global_motor_identifier, 'SAP', sap_motor_id, sap_motor_hash_key, sap_load_date FROM rows
    ) x;

-- Row-count check after load.
SELECT table_name,
       (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM business_vault.%I', table_name), false, true, '')))[1]::text::bigint AS row_count
FROM information_schema.tables
WHERE table_schema = 'business_vault'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
