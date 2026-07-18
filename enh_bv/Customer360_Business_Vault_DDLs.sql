
CREATE TABLE bv_address_master
(
	global_address_identifier STRING NOT NULL,
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

ALTER TABLE bv_address_master
	ADD CONSTRAINT XPKBV_Address_Master PRIMARY KEY (global_address_identifier);

CREATE TABLE bv_address_master_xref
(
	global_address_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

CREATE TABLE bv_home_master
(
	global_home_identifier STRING NOT NULL,
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

ALTER TABLE bv_home_master
	ADD CONSTRAINT XPKBV_Home_Master PRIMARY KEY (global_home_identifier);

CREATE TABLE bv_home_master_xref
(
	global_home_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

CREATE TABLE bv_legal_person_master
(
	global_person_identifier STRING NOT NULL,
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

ALTER TABLE bv_legal_person_master
	ADD CONSTRAINT XPKBV_Legal_Person_Master PRIMARY KEY (global_person_identifier);

CREATE TABLE bv_legal_person_master_xref
(
	global_person_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

CREATE TABLE bv_motor_master
(
	global_motor_identifier STRING NOT NULL,
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

ALTER TABLE bv_motor_master
	ADD CONSTRAINT XPKBV_Motor_Master PRIMARY KEY (global_motor_identifier);

CREATE TABLE bv_motor_master_xref
(
	global_motor_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

CREATE TABLE bv_natural_person_master
(
	global_person_identifier STRING NOT NULL,
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

ALTER TABLE bv_natural_person_master
	ADD CONSTRAINT XPKBV_Natural_Person_Master PRIMARY KEY (global_person_identifier);

CREATE TABLE bv_natural_person_master_xref
(
	global_person_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

CREATE TABLE bv_product_master
(
	global_product_identifier STRING NOT NULL,
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

ALTER TABLE bv_product_master
	ADD CONSTRAINT XPKBV_Product_Master PRIMARY KEY (global_product_identifier);

CREATE TABLE bv_product_master_xref
(
	global_product_identifier STRING NOT NULL,
	rawdv_source_name STRING,
	rawdv_source_business_key STRING,
	rawdv_hashkey STRING,
	load_timestamp TIMESTAMP
);

ALTER TABLE bv_address_master
	ADD CONSTRAINT R_123 FOREIGN KEY (global_person_identifier) REFERENCES bv_legal_person_master (global_person_identifier);

ALTER TABLE bv_address_master
	ADD CONSTRAINT R_124 FOREIGN KEY (global_person_identifier) REFERENCES bv_natural_person_master (global_person_identifier);

ALTER TABLE bv_address_master_xref
	ADD CONSTRAINT R_125 FOREIGN KEY (global_address_identifier) REFERENCES bv_address_master (global_address_identifier);

ALTER TABLE bv_home_master
	ADD CONSTRAINT R_133 FOREIGN KEY (global_product_identifier) REFERENCES bv_product_master (global_product_identifier);

ALTER TABLE bv_home_master_xref
	ADD CONSTRAINT R_126 FOREIGN KEY (global_home_identifier) REFERENCES bv_home_master (global_home_identifier);

ALTER TABLE bv_legal_person_master_xref
	ADD CONSTRAINT R_127 FOREIGN KEY (global_person_identifier) REFERENCES bv_legal_person_master (global_person_identifier);

ALTER TABLE bv_motor_master
	ADD CONSTRAINT R_132 FOREIGN KEY (global_product_identifier) REFERENCES bv_product_master (global_product_identifier);

ALTER TABLE bv_motor_master_xref
	ADD CONSTRAINT R_128 FOREIGN KEY (global_motor_identifier) REFERENCES bv_motor_master (global_motor_identifier);

ALTER TABLE bv_natural_person_master_xref
	ADD CONSTRAINT R_129 FOREIGN KEY (global_person_identifier) REFERENCES bv_natural_person_master (global_person_identifier);

ALTER TABLE bv_product_master_xref
	ADD CONSTRAINT R_130 FOREIGN KEY (global_product_identifier) REFERENCES bv_product_master (global_product_identifier);
