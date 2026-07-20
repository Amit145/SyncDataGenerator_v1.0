-- Rebuild Business Vault from Raw Vault for Databricks SQL.

-- Source schema: raw_vault

-- Target schema: business_vault

-- Databricks-compatible version: no PostgreSQL DISTINCT ON syntax, no PostgreSQL casts.


DROP SCHEMA IF EXISTS business_vault CASCADE;
CREATE SCHEMA business_vault;

CREATE TABLE business_vault.bv_address_master (
    global_address_identifier STRING,
    master_address_hash_key STRING,
    address_type STRING,
    address_line_1 STRING,
    address_line_2 STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    region STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING,
    global_person_identifier STRING NOT NULL
);

CREATE TABLE business_vault.bv_address_master_xref (
    global_address_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_home_master (
    global_home_identifier STRING,
    master_home_hash_key STRING,
    policy_identifier STRING,
    home_type STRING,
    home_location STRING,
    home_state STRING,
    wall_construction_type STRING,
    roof_construction_type STRING,
    is_existing_home_customer STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING,
    global_product_identifier STRING NOT NULL
);

CREATE TABLE business_vault.bv_home_master_xref (
    global_home_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_legal_person_master (
    global_person_identifier STRING,
    master_legal_person_hash_key STRING,
    tenant_identifier STRING,
    is_lead STRING,
    person_type STRING,
    operational_paperless_consent STRING,
    source_identifier STRING,
    source_type STRING,
    organization STRING,
    org_establishment_date DATE,
    email_address STRING,
    phone_number STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING
);

CREATE TABLE business_vault.bv_legal_person_master_xref (
    global_person_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_motor_master (
    global_motor_identifier STRING,
    master_motor_hash_key STRING,
    policy_identifier STRING,
    motor_class STRING,
    motor_model STRING,
    motor_type STRING,
    motor_variant STRING,
    body_type STRING,
    fuel_type STRING,
    gear_type STRING,
    body_colour STRING,
    motor_parked_location STRING,
    motor_registration_state STRING,
    motor_manufacturing_date DATE,
    motor_age INTEGER,
    motor_risk_class_code STRING,
    motor_owner_type STRING,
    driver_experience_years INTEGER,
    license_status STRING,
    motor_sum_insured DOUBLE,
    is_existing_motor_customer STRING,
    motor_lapsed_policies STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING,
    global_product_identifier STRING NOT NULL
);

CREATE TABLE business_vault.bv_motor_master_xref (
    global_motor_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_natural_person_master (
    global_person_identifier STRING,
    master_natural_person_hash_key STRING,
    tenant_identifier STRING,
    is_lead STRING,
    person_type STRING,
    operational_paperless_consent STRING,
    source_identifier STRING,
    source_type STRING,
    first_name STRING,
    middle_name STRING,
    last_name STRING,
    full_name STRING,
    date_of_birth DATE,
    gender STRING,
    occupation STRING,
    courtesy_title STRING,
    role STRING,
    nationality STRING,
    marital_status STRING,
    assesed_disability_degree STRING,
    preferred_language STRING,
    job_title STRING,
    personal_email STRING,
    work_email STRING,
    work_phone STRING,
    home_phone STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING
);

CREATE TABLE business_vault.bv_natural_person_master_xref (
    global_person_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);

CREATE TABLE business_vault.bv_product_master (
    global_product_identifier STRING,
    master_product_hash_key STRING,
    product_type STRING,
    product_sub_type STRING,
    product_name STRING,
    product_launch_date DATE,
    product_status STRING,
    product_line_of_business STRING,
    underwriting_group STRING,
    regulatory_approval_code STRING,
    effective_from DATE,
    effective_to DATE,
    current_active_flag STRING
);

CREATE TABLE business_vault.bv_product_master_xref (
    global_product_identifier STRING NOT NULL,
    rawdv_source_name STRING,
    rawdv_source_business_key STRING,
    rawdv_hashkey STRING,
    load_timestamp TIMESTAMP
);



INSERT INTO business_vault.bv_product_master
    WITH product_match AS (
        SELECT
            concat(hcrm.product_id, '||', hsap.product_id) AS global_product_identifier,
            concat(hcrm.product_hash_key, '||', hsap.product_hash_key) AS master_product_hash_key,
            sap.product_type,
            sap.product_sub_type,
            coalesce(nullif(sap.product_name, ''), nullif(crm.product_name, '')) AS product_name,
            to_date(nullif(crm.product_launch_date, '')) AS product_launch_date,
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
        QUALIFY row_number() OVER (
            PARTITION BY concat(hcrm.product_id, '||', hsap.product_id)
            ORDER BY hcrm.load_date DESC, hsap.load_date DESC
        ) = 1
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
        current_date(),
        DATE '9999-12-31',
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
        SELECT
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
        QUALIFY row_number() OVER (
            PARTITION BY nm.global_person_identifier
            ORDER BY spc.person_hash_key, sps.person_hash_key
        ) = 1
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
        current_date(),
        DATE '9999-12-31',
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
        SELECT
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
        QUALIFY row_number() OVER (
            PARTITION BY lm.global_person_identifier
            ORDER BY spc.person_hash_key, sps.person_hash_key
        ) = 1
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
        current_date(),
        DATE '9999-12-31',
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
        SELECT
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
        QUALIFY row_number() OVER (
            PARTITION BY concat(hac.address_id, '||', hasp.address_id)
            ORDER BY hac.load_date DESC, hasp.load_date DESC
        ) = 1
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
        current_date(),
        DATE '9999-12-31',
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
        SELECT
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
        QUALIFY row_number() OVER (
            PARTITION BY concat(hac.address_id, '||', hasp.address_id)
            ORDER BY hac.load_date DESC, hasp.load_date DESC
        ) = 1
    )
    SELECT global_address_identifier, crm_source, crm_business_key, crm_hash_key, crm_load_date FROM address_rows
    UNION ALL
    SELECT global_address_identifier, sap_source, sap_business_key, sap_hash_key, sap_load_date FROM address_rows;

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

-- Row-count check after load.
SELECT 'bv_address_master' AS table_name, count(*) AS row_count FROM business_vault.bv_address_master
UNION ALL
SELECT 'bv_address_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_address_master_xref
UNION ALL
SELECT 'bv_home_master' AS table_name, count(*) AS row_count FROM business_vault.bv_home_master
UNION ALL
SELECT 'bv_home_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_home_master_xref
UNION ALL
SELECT 'bv_legal_person_master' AS table_name, count(*) AS row_count FROM business_vault.bv_legal_person_master
UNION ALL
SELECT 'bv_legal_person_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_legal_person_master_xref
UNION ALL
SELECT 'bv_motor_master' AS table_name, count(*) AS row_count FROM business_vault.bv_motor_master
UNION ALL
SELECT 'bv_motor_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_motor_master_xref
UNION ALL
SELECT 'bv_natural_person_master' AS table_name, count(*) AS row_count FROM business_vault.bv_natural_person_master
UNION ALL
SELECT 'bv_natural_person_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_natural_person_master_xref
UNION ALL
SELECT 'bv_product_master' AS table_name, count(*) AS row_count FROM business_vault.bv_product_master
UNION ALL
SELECT 'bv_product_master_xref' AS table_name, count(*) AS row_count FROM business_vault.bv_product_master_xref
ORDER BY table_name;
