
CREATE TABLE hub_catastrophe
(
	catastrophe_hash_key STRING NOT NULL,
	catastrophe_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_catastrophe
	ADD CONSTRAINT pk_hub_catastrophe PRIMARY KEY (catastrophe_hash_key);

CREATE TABLE hub_claim
(
	claim_hash_key STRING NOT NULL,
	claim_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_claim
	ADD CONSTRAINT pk_hub_claim PRIMARY KEY (claim_hash_key);

CREATE TABLE hub_claim_event
(
	claim_event_hash_key STRING NOT NULL,
	claim_event_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_claim_event
	ADD CONSTRAINT pk_hub_claim_event PRIMARY KEY (claim_event_hash_key);

CREATE TABLE hub_claim_investigation
(
	claim_investigation_hash_key STRING NOT NULL,
	claim_investigation_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_claim_investigation
	ADD CONSTRAINT pk_hub_claim_investigation PRIMARY KEY (claim_investigation_hash_key);

CREATE TABLE hub_claim_participant
(
	claim_participant_hash_key STRING NOT NULL,
	claim_participant_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_claim_participant
	ADD CONSTRAINT pk_hub_claim_participant PRIMARY KEY (claim_participant_hash_key);

CREATE TABLE hub_coverage
(
	coverage_hash_key STRING NOT NULL,
	coverage_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_coverage
	ADD CONSTRAINT pk_hub_coverage PRIMARY KEY (coverage_hash_key);

CREATE TABLE hub_diagnosis
(
	diagnosis_hash_key STRING NOT NULL,
	diagnosis_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_diagnosis
	ADD CONSTRAINT pk_hub_diagnosis PRIMARY KEY (diagnosis_hash_key);

CREATE TABLE hub_health_insurance_claim
(
	health_insurance_claim_hash_key STRING NOT NULL,
	health_insurance_claim_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_health_insurance_claim
	ADD CONSTRAINT pk_hub_health_insurance_claim PRIMARY KEY (health_insurance_claim_hash_key);

CREATE TABLE hub_injury
(
	injury_hash_key STRING NOT NULL,
	injury_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_injury
	ADD CONSTRAINT pk_hub_injury PRIMARY KEY (injury_hash_key);

CREATE TABLE hub_insured_entity
(
	insured_entity_hash_key STRING NOT NULL,
	insured_entity_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_insured_entity
	ADD CONSTRAINT pk_hub_insured_entity PRIMARY KEY (insured_entity_hash_key);

CREATE TABLE hub_litigation
(
	litigation_hash_key STRING NOT NULL,
	litigation_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_litigation
	ADD CONSTRAINT pk_hub_litigation PRIMARY KEY (litigation_hash_key);

CREATE TABLE hub_loss_event
(
	loss_event_hash_key STRING NOT NULL,
	loss_event_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_loss_event
	ADD CONSTRAINT pk_hub_loss_event PRIMARY KEY (loss_event_hash_key);

CREATE TABLE hub_medical_assessment
(
	medical_assessment_hash_key STRING NOT NULL,
	medical_assessment_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_medical_assessment
	ADD CONSTRAINT pk_hub_medical_assessment PRIMARY KEY (medical_assessment_hash_key);

CREATE TABLE hub_medical_condition
(
	medical_condition_hash_key STRING NOT NULL,
	medical_condition_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_medical_condition
	ADD CONSTRAINT pk_hub_medical_condition PRIMARY KEY (medical_condition_hash_key);

CREATE TABLE hub_medical_report
(
	medical_report_hash_key STRING NOT NULL,
	medical_report_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_medical_report
	ADD CONSTRAINT pk_hub_medical_report PRIMARY KEY (medical_report_hash_key);

CREATE TABLE hub_person
(
	person_hash_key STRING NOT NULL,
	person_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_person
	ADD CONSTRAINT pk_hub_person PRIMARY KEY (person_hash_key);

CREATE TABLE hub_physical_place
(
	physical_place_hash_key STRING NOT NULL,
	physical_place_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_physical_place
	ADD CONSTRAINT pk_hub_physical_place PRIMARY KEY (physical_place_hash_key);

CREATE TABLE hub_police_report
(
	police_report_hash_key STRING NOT NULL,
	police_report_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_police_report
	ADD CONSTRAINT pk_hub_police_report PRIMARY KEY (police_report_hash_key);

CREATE TABLE hub_policy
(
	policy_hash_key STRING NOT NULL,
	policy_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_policy
	ADD CONSTRAINT pk_hub_policy PRIMARY KEY (policy_hash_key);

CREATE TABLE hub_policy_coverage
(
	policy_coverage_hash_key STRING NOT NULL,
	policy_coverage_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_policy_coverage
	ADD CONSTRAINT pk_hub_policy_coverage PRIMARY KEY (policy_coverage_hash_key);

CREATE TABLE hub_repair
(
	repair_hash_key STRING NOT NULL,
	repair_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_repair
	ADD CONSTRAINT pk_hub_repair PRIMARY KEY (repair_hash_key);

CREATE TABLE hub_settlement
(
	settlement_hash_key STRING NOT NULL,
	settlement_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_settlement
	ADD CONSTRAINT pk_hub_settlement PRIMARY KEY (settlement_hash_key);

CREATE TABLE hub_treatment
(
	treatment_hash_key STRING NOT NULL,
	treatment_id STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE hub_treatment
	ADD CONSTRAINT pk_hub_treatment PRIMARY KEY (treatment_hash_key);

CREATE TABLE link_claim_claim_event
(
	claim_claim_event_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_claim_event
	ADD CONSTRAINT pk_link_claim_claim_event PRIMARY KEY (claim_claim_event_hash_key);

CREATE TABLE link_claim_claim_participant
(
	claim_claim_participant_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	claim_participant_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_claim_participant
	ADD CONSTRAINT pk_link_claim_claim_participant PRIMARY KEY (claim_claim_participant_hash_key);

CREATE TABLE link_claim_event_claim_investigation
(
	claim_event_claim_investigation_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	claim_investigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_claim_investigation
	ADD CONSTRAINT pk_link_claim_event_claim_investigation PRIMARY KEY (claim_event_claim_investigation_hash_key);

CREATE TABLE link_claim_event_injury
(
	claim_event_injury_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	injury_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_injury
	ADD CONSTRAINT pk_link_claim_event_injury PRIMARY KEY (claim_event_injury_hash_key);

CREATE TABLE link_claim_event_litigation
(
	claim_event_litigation_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	litigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_litigation
	ADD CONSTRAINT pk_link_claim_event_litigation PRIMARY KEY (claim_event_litigation_hash_key);

CREATE TABLE link_claim_event_medical_assessment
(
	claim_event_medical_assessment_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	medical_assessment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_medical_assessment
	ADD CONSTRAINT pk_link_claim_event_medical_assessment PRIMARY KEY (claim_event_medical_assessment_hash_key);

CREATE TABLE link_claim_event_repair
(
	claim_event_repair_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	repair_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_repair
	ADD CONSTRAINT pk_link_claim_event_repair PRIMARY KEY (claim_event_repair_hash_key);

CREATE TABLE link_claim_event_settlement
(
	claim_event_settlement_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	settlement_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_settlement
	ADD CONSTRAINT pk_link_claim_event_settlement PRIMARY KEY (claim_event_settlement_hash_key);

CREATE TABLE link_claim_event_treatment
(
	claim_event_treatment_hash_key STRING NOT NULL,
	claim_event_hash_key STRING NOT NULL,
	treatment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_event_treatment
	ADD CONSTRAINT pk_link_claim_event_treatment PRIMARY KEY (claim_event_treatment_hash_key);

CREATE TABLE link_claim_health_insurance_claim
(
	claim_health_insurance_claim_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	health_insurance_claim_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_health_insurance_claim
	ADD CONSTRAINT pk_link_claim_health_insurance_claim PRIMARY KEY (claim_health_insurance_claim_hash_key);

CREATE TABLE link_claim_litigation
(
	claim_litigation_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	litigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_litigation
	ADD CONSTRAINT pk_link_claim_litigation PRIMARY KEY (claim_litigation_hash_key);

CREATE TABLE link_claim_loss_event
(
	claim_loss_event_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	loss_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_loss_event
	ADD CONSTRAINT pk_link_claim_loss_event PRIMARY KEY (claim_loss_event_hash_key);

CREATE TABLE link_claim_medical_report
(
	claim_medical_report_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	medical_report_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_medical_report
	ADD CONSTRAINT pk_link_claim_medical_report PRIMARY KEY (claim_medical_report_hash_key);

CREATE TABLE link_claim_participant_repair
(
	claim_participant_repair_hash_key STRING NOT NULL,
	claim_participant_hash_key STRING NOT NULL,
	repair_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_participant_repair
	ADD CONSTRAINT pk_link_claim_participant_repair PRIMARY KEY (claim_participant_repair_hash_key);

CREATE TABLE link_claim_participant_treatment
(
	claim_participant_treatment_hash_key STRING NOT NULL,
	claim_participant_hash_key STRING NOT NULL,
	treatment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_participant_treatment
	ADD CONSTRAINT pk_link_claim_participant_treatment PRIMARY KEY (claim_participant_treatment_hash_key);

CREATE TABLE link_claim_police_report
(
	claim_police_report_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	police_report_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_claim_police_report
	ADD CONSTRAINT pk_link_claim_police_report PRIMARY KEY (claim_police_report_hash_key);

CREATE TABLE link_coverage_policy_coverage
(
	coverage_policy_coverage_hash_key STRING NOT NULL,
	coverage_hash_key STRING NOT NULL,
	policy_coverage_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_coverage_policy_coverage
	ADD CONSTRAINT pk_link_coverage_policy_coverage PRIMARY KEY (coverage_policy_coverage_hash_key);

CREATE TABLE link_injury_diagnosis
(
	injury_diagnosis_hash_key STRING NOT NULL,
	injury_hash_key STRING NOT NULL,
	diagnosis_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_injury_diagnosis
	ADD CONSTRAINT pk_link_injury_diagnosis PRIMARY KEY (injury_diagnosis_hash_key);

CREATE TABLE link_insured_entity_policy_coverage
(
	insured_entity_policy_coverage_hash_key STRING NOT NULL,
	insured_entity_hash_key STRING NOT NULL,
	policy_coverage_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_insured_entity_policy_coverage
	ADD CONSTRAINT pk_link_insured_entity_policy_coverage PRIMARY KEY (insured_entity_policy_coverage_hash_key);

CREATE TABLE link_loss_event_catastrophe
(
	loss_event_catastrophe_hash_key STRING NOT NULL,
	loss_event_hash_key STRING NOT NULL,
	catastrophe_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_loss_event_catastrophe
	ADD CONSTRAINT pk_link_loss_event_catastrophe PRIMARY KEY (loss_event_catastrophe_hash_key);

CREATE TABLE link_medical_condition_diagnosis
(
	medical_condition_diagnosis_hash_key STRING NOT NULL,
	medical_condition_hash_key STRING NOT NULL,
	diagnosis_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_medical_condition_diagnosis
	ADD CONSTRAINT pk_link_medical_condition_diagnosis PRIMARY KEY (medical_condition_diagnosis_hash_key);

CREATE TABLE link_medical_condition_treatment
(
	medical_condition_treatment_hash_key STRING NOT NULL,
	medical_condition_hash_key STRING NOT NULL,
	treatment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_medical_condition_treatment
	ADD CONSTRAINT pk_link_medical_condition_treatment PRIMARY KEY (medical_condition_treatment_hash_key);

CREATE TABLE link_person_claim_participant
(
	person_claim_participant_hash_key STRING NOT NULL,
	person_hash_key STRING NOT NULL,
	claim_participant_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_person_claim_participant
	ADD CONSTRAINT pk_link_person_claim_participant PRIMARY KEY (person_claim_participant_hash_key);

CREATE TABLE link_physical_place_loss_event
(
	physical_place_loss_event_hash_key STRING NOT NULL,
	physical_place_hash_key STRING NOT NULL,
	loss_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_physical_place_loss_event
	ADD CONSTRAINT pk_link_physical_place_loss_event PRIMARY KEY (physical_place_loss_event_hash_key);

CREATE TABLE link_policy_claim
(
	policy_claim_hash_key STRING NOT NULL,
	policy_hash_key STRING NOT NULL,
	claim_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT pk_link_policy_claim PRIMARY KEY (policy_claim_hash_key);

CREATE TABLE link_policy_policy_coverage
(
	policy_policy_coverage_hash_key STRING NOT NULL,
	policy_hash_key STRING NOT NULL,
	policy_coverage_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL
);

ALTER TABLE link_policy_policy_coverage
	ADD CONSTRAINT pk_link_policy_policy_coverage PRIMARY KEY (policy_policy_coverage_hash_key);

CREATE TABLE sat_catastrophe
(
	catastrophe_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	catastrophe_cause STRING,
	catastrophe_description STRING,
	catastrophe_begin_date DATE,
	catastrophe_end_date DATE,
	cresta_zone STRING,
	nat_cat_event_code STRING
);

ALTER TABLE sat_catastrophe
	ADD CONSTRAINT pk_sat_catastrophe PRIMARY KEY (catastrophe_hash_key, load_ts);

CREATE TABLE sat_claim_crm
(
	claim_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	claim_type STRING NOT NULL,
	claim_number STRING,
	third_party_claim_number STRING,
	claim_status STRING,
	claim_state STRING,
	claim_status_date DATE,
	claim_open_date DATE,
	claim_close_date DATE,
	claim_reopen_date DATE,
	claim_reopen_reason STRING,
	claims_made_date DATE,
	movement_date DATE,
	claim_duration INT,
	claim_type_code STRING,
	claim_process_method STRING,
	claim_specific_flag STRING,
	bodily_injury_indicator STRING,
	applicable_deductible_flag STRING,
	total_loss_flag STRING,
	litigation_flag STRING,
	proven_claim_fraud_flag STRING,
	cross_border_claim_indicator STRING,
	intercompany_agreement_flag STRING,
	no_claims_discount DOUBLE,
	claim_requested_amount DOUBLE,
	outstanding_subrogation_amount DOUBLE,
	recovery_actual DOUBLE,
	recovery_type STRING,
	claims_rejection_reason STRING,
	finalization_reason STRING,
	indemnity_logic STRING,
	coverage_verification_result STRING,
	instruction_closure_date DATE,
	subrogation_paid_date DATE,
	claims_history_lob STRING,
	fire_brigade_fees_and_levies DOUBLE,
	total_incurred DOUBLE,
	total_payment_amount DOUBLE,
	reimbursable_claim_amount DOUBLE,
	settlement_amount_pc DOUBLE,
	claim_approval_date DATE,
	claim_payment_date DATE,
	claim_amounts_description STRING,
	insured_entity_id STRING NOT NULL,
	outstanding_reserve STRING,
	claims_expenses STRING,
	is_claim_suspicious STRING,
	suspected_amt STRING,
	fraud_amt STRING,
	is_recovery_happened STRING,
	days_to_first_recovery STRING,
	days_to_last_recovery STRING,
	litigation_duration_days STRING,
	claim_reason STRING,
	claim_channel STRING,
	claim_product STRING,
	claim_band STRING,
	claim_band_sort STRING,
	claim_fraud_status STRING,
	claim_fraud_type STRING,
	claim_fraud_detection_method STRING,
	recovery_band STRING,
	recovery_category STRING,
	recovery_source STRING
);

ALTER TABLE sat_claim_crm
	ADD CONSTRAINT pk_sat_claim PRIMARY KEY (claim_hash_key, load_ts);

CREATE TABLE sat_claim_event
(
	claim_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	event_type STRING NOT NULL,
	event_date DATE,
	event_description STRING
);

ALTER TABLE sat_claim_event
	ADD CONSTRAINT pk_sat_claim_event PRIMARY KEY (claim_event_hash_key, load_ts);

CREATE TABLE sat_claim_investigation_crm
(
	claim_investigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	claim_handler_identifier STRING,
	claim_handler_notes STRING,
	claim_investigation_start_date DATE,
	claim_investigation_end_date DATE,
	fraud_indicator STRING,
	investigator_flag STRING,
	claim_fraud_assessment STRING
);

ALTER TABLE sat_claim_investigation_crm
	ADD CONSTRAINT pk_sat_claim_investigation PRIMARY KEY (claim_investigation_hash_key, load_ts);

CREATE TABLE sat_claim_investigation_sap
(
	claim_investigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	claim_investigator_id STRING,
	investigation_notes STRING,
	case_open_date DATE,
	case_close_date DATE,
	fraud_confirmed_flag STRING,
	claim_review_flag STRING,
	fraud_risk_assessment STRING
);

ALTER TABLE sat_claim_investigation_sap
	ADD CONSTRAINT pk_sat_claim_investigation PRIMARY KEY (claim_investigation_hash_key, load_ts);

CREATE TABLE sat_claim_participant
(
	claim_participant_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	role_in_claim STRING,
	is_primary_indicator STRING,
	liability_percentage DECIMAL(5,2),
	injury_flag STRING,
	involvement_type STRING,
	role_start_date DATE,
	role_end_date DATE
);

ALTER TABLE sat_claim_participant
	ADD CONSTRAINT pk_sat_claim_participant PRIMARY KEY (claim_participant_hash_key, load_ts);

CREATE TABLE sat_claim_sap
(
	claim_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	ext_claim_number STRING,
	loss_category STRING,
	tp_claim_ref CHAR(18),
	claim_stage STRING NOT NULL,
	case_state CHAR(18),
	stage_update_date CHAR(18),
	first_notice_date DATE,
	claim_settlement_date DATE,
	last_activity_date DATE,
	case_age_days INT,
	category_code STRING,
	handling_method STRING,
	risk_level STRING,
	injury_flag STRING,
	write_off_flag STRING,
	legal_case_flag STRING,
	confirmed_fraud_flag STRING,
	hospitalization_flag STRING,
	estimated_loss_amount DOUBLE,
	expected_recovery_amount DOUBLE,
	recovered_amount DOUBLE,
	recovery_category STRING,
	denial_reason STRING,
	closure_reason STRING,
	recovery_payment_date DATE,
	work_status_prior_incident STRING,
	return_to_work_status STRING,
	total_estimated_cost DOUBLE,
	total_paid_amount DOUBLE,
	eligible_reimbursement_amount DOUBLE,
	medical_reimbursement_amount DOUBLE,
	proposed_settlement_amount DOUBLE,
	approval_date DATE,
	payment_release_date DATE
);

ALTER TABLE sat_claim_sap
	ADD CONSTRAINT pk_sat_claim PRIMARY KEY (claim_hash_key, load_ts);

CREATE TABLE sat_coverage
(
	coverage_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	coverage_type STRING NOT NULL,
	coverage_description STRING,
	coverage_start_date DATE,
	coverage_end_date DATE,
	coverage_type_id STRING,
	maximum_deductible DOUBLE,
	limit_id STRING,
	exclusion_id STRING,
	deductible_id STRING,
	benefit_id STRING,
	risk_id STRING,
	peril_id STRING,
	product_element_id STRING
);

ALTER TABLE sat_coverage
	ADD CONSTRAINT pk_sat_coverage PRIMARY KEY (coverage_hash_key, load_ts);

CREATE TABLE sat_diagnosis
(
	diagnosis_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	diagnosis_code STRING,
	diagnosis_description STRING,
	diagnosis_priority INT,
	diagnosis_standard STRING
);

ALTER TABLE sat_diagnosis
	ADD CONSTRAINT pk_sat_diagnosis PRIMARY KEY (diagnosis_hash_key, load_ts);

CREATE TABLE sat_health_insurance_claim
(
	health_insurance_claim_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	date_of_first_medical_expert_report DATE,
	health_data_consent STRING
);

ALTER TABLE sat_health_insurance_claim
	ADD CONSTRAINT pk_sat_health_insurance_claim PRIMARY KEY (health_insurance_claim_hash_key, load_ts);

CREATE TABLE sat_injury
(
	injury_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	bodily_injury_date DATE,
	injury_description STRING
);

ALTER TABLE sat_injury
	ADD CONSTRAINT pk_sat_injury PRIMARY KEY (injury_hash_key, load_ts);

CREATE TABLE sat_insured_entity
(
	insured_entity_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	entity_type STRING NOT NULL,
	insured_person_identifier STRING,
	insured_object_owner_identifier STRING,
	insured_object_address_flag STRING,
	insured_object_sum_insured DOUBLE,
	exposure_base STRING,
	grouped_location_flag STRING,
	replacement_value DOUBLE,
	type_of_insured_entity STRING,
	value_of_content DOUBLE,
	value_of_goods_carried DOUBLE,
	actual_pre_event_entity_value DOUBLE,
	damaged_entity STRING,
	insured_object_desc STRING,
	insured_object_current_status STRING,
	insured_object_start_date STRING,
	insured_object_end_date STRING,
	wall_construction_material_type STRING,
	home_risk_address STRING,
	home_type STRING,
	home_state STRING,
	vehicle_body_type STRING,
	vehicle_fuel_type STRING,
	vehicle_risk_address STRING,
	vehicle_risk_class_code STRING,
	vehicle_variant STRING,
	vehicle_reg_state STRING,
	vehicle_class STRING,
	vehicle_model STRING
);

ALTER TABLE sat_insured_entity
	ADD CONSTRAINT pk_sat_insured_entity PRIMARY KEY (insured_entity_hash_key, load_ts);

CREATE TABLE sat_litigation
(
	litigation_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	court_matter_type STRING,
	date_of_legal_representation DATE,
	first_litigation_date DATE,
	litigation_date DATE,
	decision_of_court STRING,
	litigation_start_date DATE,
	litigation_end_date DATE
);

ALTER TABLE sat_litigation
	ADD CONSTRAINT pk_sat_litigation PRIMARY KEY (litigation_hash_key, load_ts);

CREATE TABLE sat_loss_event_crm
(
	loss_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	loss_event_name STRING,
	loss_event_description STRING,
	loss_date DATE,
	loss_time TIMESTAMP,
	loss_event_start_date DATE,
	loss_event_end_date DATE,
	loss_event_status STRING,
	loss_type STRING,
	loss_category STRING,
	loss_cause STRING,
	property_liability_loss_cause STRING,
	effective_loss_date DATE,
	notification_date DATE,
	notification_channel STRING,
	fatality_flag STRING,
	minimal_impact_flag STRING,
	number_of_injured_parties INT,
	number_of_people_involved INT,
	number_of_vehicles_involved INT,
	drugs_alcohol_indicator STRING,
	contributory_negligence_flag STRING,
	damage_specification STRING
);

ALTER TABLE sat_loss_event_crm
	ADD CONSTRAINT pk_sat_loss_event PRIMARY KEY (loss_event_hash_key, load_ts);

CREATE TABLE sat_loss_event_sap
(
	loss_event_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	incident_name STRING,
	incident_description STRING,
	event_date DATE,
	event_time TIMESTAMP,
	incident_start_date DATE,
	incident_end_date DATE,
	incident_status STRING,
	event_type STRING,
	incident_category STRING,
	root_cause STRING,
	liability_cause STRING,
	reported_date DATE,
	reporting_channel STRING,
	days_to_fnol INT,
	notification_duration INT,
	casualty_indicator STRING,
	catastrophe_flag STRING,
	natcat_flag STRING,
	event_severity_low_flag STRING,
	casualty_count INT,
	impacted_person_count INT,
	impacted_vehicle_count INT,
	impairment_flag STRING,
	shared_liability_flag STRING,
	impact_description STRING
);

ALTER TABLE sat_loss_event_sap
	ADD CONSTRAINT pk_sat_loss_event PRIMARY KEY (loss_event_hash_key, load_ts);

CREATE TABLE sat_medical_assessment
(
	medical_assessment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	assessed_disability_degree DOUBLE,
	permanent_disability_degree DOUBLE,
	permanent_disability_flag STRING,
	medical_report_flag STRING,
	medical_report_date DATE,
	person_hospital_status STRING,
	psychological_factors STRING,
	medical_condition_description STRING
);

ALTER TABLE sat_medical_assessment
	ADD CONSTRAINT pk_sat_medical_assessment PRIMARY KEY (medical_assessment_hash_key, load_ts);

CREATE TABLE sat_medical_condition
(
	medical_condition_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	disease_name STRING,
	disability_status STRING,
	pre_existing_condition STRING
);

ALTER TABLE sat_medical_condition
	ADD CONSTRAINT pk_sat_medical_condition PRIMARY KEY (medical_condition_hash_key, load_ts);

CREATE TABLE sat_medical_report
(
	medical_report_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	document_id STRING NOT NULL,
	health_declaration STRING
);

ALTER TABLE sat_medical_report
	ADD CONSTRAINT pk_sat_medical_report PRIMARY KEY (medical_report_hash_key, load_ts);

CREATE TABLE sat_person
(
	person_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	is_lead STRING,
	preferred_language STRING,
	source_identifier STRING,
	source_type STRING,
	tenant_identifier STRING,
	assessed_disability_degree STRING,
	is_operational_paperless_consent STRING,
	is_opt_in_legitimate_interest STRING,
	is_opt_in_validated STRING,
	person_type STRING,
	digital_id STRING,
	identification_type STRING,
	legal_consent_flag STRING,
	credit_rating STRING,
	credit_rating_provider STRING,
	rate_class STRING,
	person_status STRING,
	natural_ref STRING,
	first_name STRING,
	last_name STRING,
	full_name STRING,
	courtesy_title STRING,
	occupation STRING,
	birth_date date,
	birth_year INT,
	nationality STRING,
	gender STRING,
	marital_status STRING,
	job_title STRING,
	role STRING,
	converted_date TIMESTAMP,
	person_score INT,
	email_home STRING,
	email_work STRING,
	phone_work STRING,
	phone_home STRING,
	legal_ref STRING,
	company_name STRING,
	date_of_constitution TIMESTAMP,
	legal_job_title STRING,
	legal_src_ref STRING,
	legal_src_type STRING,
	legal_status STRING
);

ALTER TABLE sat_person
	ADD CONSTRAINT pk_sat_person PRIMARY KEY (person_hash_key, load_ts);

CREATE TABLE sat_physical_place
(
	physical_place_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	place_id STRING NOT NULL,
	city STRING,
	state STRING,
	country STRING,
	applicable_jurisdiction STRING,
	census_zone STRING,
	commune_code STRING,
	geocoding_level STRING,
	latitude DOUBLE,
	longitude DOUBLE,
	municipal_district STRING,
	natcat_hazard_zone_scheme STRING,
	point_of_interest STRING,
	possible_max_business_interruption DOUBLE,
	sub_region STRING,
	surface_elevation DOUBLE
);

ALTER TABLE sat_physical_place
	ADD CONSTRAINT pk_sat_physical_place PRIMARY KEY (physical_place_hash_key, load_ts);

CREATE TABLE sat_police_report
(
	police_report_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	police_report_number STRING,
	police_report_date DATE,
	police_report_flag STRING
);

ALTER TABLE sat_police_report
	ADD CONSTRAINT pk_sat_police_report PRIMARY KEY (police_report_hash_key, load_ts);

CREATE TABLE sat_policy
(
	policy_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	policy_type STRING NOT NULL,
	policy_number STRING,
	aspire_policy_id STRING,
	policy_status_code STRING,
	policy_status_date DATE,
	policy_inception_date DATE,
	policy_inception_date_as_new_business DATE,
	policy_inception_date_of_first_allianz_policy DATE,
	policy_issue_date DATE,
	policy_end_date DATE,
	policy_expiry_date DATE,
	policy_cancellation_date DATE,
	policy_cancellation_notification_date DATE,
	policy_cancellation_reason STRING,
	policy_renewal_date DATE,
	policy_renewal_notification_date DATE,
	policy_duration INT,
	premium DOUBLE,
	gross_written_premium DOUBLE,
	gross_earned_premium DOUBLE,
	gross_original_premium DOUBLE,
	paid_premium DOUBLE,
	sum_of_paid_premium DOUBLE,
	unearned_premium_amount DOUBLE,
	adjustment_premium DOUBLE,
	adjustment_earned_premium DOUBLE,
	estimated_premium_income DOUBLE,
	subject_premium_income DOUBLE,
	technical_price DOUBLE,
	technical_expected_loss DOUBLE,
	projected_loss_ratio DOUBLE,
	underwriting_premium_surcharge DOUBLE,
	commission_due_amount DOUBLE,
	gross_written_commission_amount DOUBLE,
	gross_earned_commission DOUBLE,
	gross_original_commission DOUBLE,
	commission_share DOUBLE,
	differed_acquisition_cost DOUBLE,
	frequency_of_installments STRING,
	preferred_payment_date DATE,
	date_of_debit DATE,
	tax_code STRING,
	tariff_version STRING,
	premium_configuration_code STRING,
	ifrs17_measurement_model STRING,
	current_bonus_malus_class STRING,
	previous_bonus_malus_class STRING,
	no_claims_discount DOUBLE,
	annual_aggregate_deductible DOUBLE,
	limit_for_indemnification DOUBLE,
	limit_business_interruption DOUBLE,
	number_of_insured_persons INT,
	number_of_substituting_policy INT,
	automatic_renewal_years INT,
	reinsurance_flag STRING,
	retention_flag STRING,
	tacit_renewal_flag STRING,
	multi_year_contracts STRING,
	lapse_flag STRING,
	flag_change_of_contract STRING,
	flag_cross_selling STRING,
	flag_quote_conversion STRING,
	marketing_campaign_flag STRING,
	primary_excess_liability_flag STRING,
	operational_paperless_consent STRING,
	legal_consent STRING,
	declared_driver STRING,
	broker_wording STRING,
	manual_clauses STRING,
	rationale_for_cession STRING,
	statute_of_limitation_date DATE,
	product_group_id STRING,
	policy_holder_id STRING,
	portfolio_id STRING,
	party_in_role_id STRING
);

ALTER TABLE sat_policy
	ADD CONSTRAINT pk_sat_policy PRIMARY KEY (policy_hash_key, load_ts);

CREATE TABLE sat_policy_coverage
(
	policy_coverage_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	coverage_group_id STRING,
	coverage_status_date DATE,
	coverage_status_code STRING,
	sum_insured DOUBLE,
	sum_insured_after_coinsurance DOUBLE,
	gross_annualized_premium DOUBLE,
	peril_premium DOUBLE,
	annual_aggregate_limit DOUBLE,
	excess DOUBLE,
	minimum_deductible DOUBLE,
	indexation STRING,
	automatic_indexation_flag STRING,
	coverage_extension STRING,
	coverage_trigger STRING,
	coverage_level_lh STRING,
	logic_of_limit STRING,
	scope_of_limit STRING,
	pct_loss_deductible DOUBLE,
	actual_price DOUBLE,
	agreed_value DOUBLE,
	technical_price_natural_perils DOUBLE,
	mandatory_participation STRING,
	liability_agreed_flag STRING,
	liability_exposure_value DOUBLE,
	flag_dual_insurance STRING,
	flag_inhabited_building STRING,
	motor_accessories_sum_insured DOUBLE,
	time_element_sum_insured DOUBLE,
	value_insured_property_damage DOUBLE,
	contingent_bi_flag STRING,
	contingent_bi_waiting_period INT,
	coverage_sum_at_risk DOUBLE,
	employer_contribution_level DOUBLE,
	premium_paid_previous_period DOUBLE,
	position_grade STRING,
	benefit_grouping STRING
);

ALTER TABLE sat_policy_coverage
	ADD CONSTRAINT pk_sat_policy_coverage PRIMARY KEY (policy_coverage_hash_key, load_ts);

CREATE TABLE sat_repair
(
	repair_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	number_of_spare_parts_repaired INT,
	paid_spare_parts_costs DOUBLE,
	spare_part_repair_shop_price DOUBLE,
	spare_part_type STRING
);

ALTER TABLE sat_repair
	ADD CONSTRAINT pk_sat_repair PRIMARY KEY (repair_hash_key, load_ts);

CREATE TABLE sat_settlement
(
	settlement_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	date_of_final_settlement DATE,
	date_of_settlement_offer_decision DATE,
	insurers_current_settlement_offer_amount DOUBLE,
	insurers_first_settlement_offer_amount DOUBLE,
	outstanding_medical_expenses DOUBLE,
	settlement_table STRING,
	settlement_type STRING
);

ALTER TABLE sat_settlement
	ADD CONSTRAINT pk_sat_settlement PRIMARY KEY (settlement_hash_key, load_ts);

CREATE TABLE sat_treatment
(
	treatment_hash_key STRING NOT NULL,
	load_ts TIMESTAMP NOT NULL,
	record_source STRING NOT NULL,
	treatment_code STRING,
	treatment_code_standard STRING,
	treatment_description STRING,
	treatment_complexity_level STRING,
	initial_treatment_date DATE,
	treatment_start_date DATE,
	number_of_treatments INT,
	paid_treatment_amount DOUBLE,
	treatment_amount_currency STRING,
	medication_code STRING,
	treatment_type STRING
);

ALTER TABLE sat_treatment
	ADD CONSTRAINT pk_sat_treatment PRIMARY KEY (treatment_hash_key, load_ts);

ALTER TABLE link_claim_claim_event
	ADD CONSTRAINT fk_link_claim_claim_event_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_claim_event
	ADD CONSTRAINT fk_link_claim_claim_event_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_claim_participant
	ADD CONSTRAINT fk_link_claim_claim_participant_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_claim_participant
	ADD CONSTRAINT fk_link_claim_claim_participant_hub_claim_participant FOREIGN KEY (claim_participant_hash_key) REFERENCES hub_claim_participant (claim_participant_hash_key);

ALTER TABLE link_claim_event_claim_investigation
	ADD CONSTRAINT fk_link_claim_event_claim_investigation_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_claim_investigation
	ADD CONSTRAINT fk_link_claim_event_claim_investigation_hub_claim_investigation FOREIGN KEY (claim_investigation_hash_key) REFERENCES hub_claim_investigation (claim_investigation_hash_key);

ALTER TABLE link_claim_event_injury
	ADD CONSTRAINT fk_link_claim_event_injury_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_injury
	ADD CONSTRAINT fk_link_claim_event_injury_hub_injury FOREIGN KEY (injury_hash_key) REFERENCES hub_injury (injury_hash_key);

ALTER TABLE link_claim_event_litigation
	ADD CONSTRAINT fk_link_claim_event_litigation_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_litigation
	ADD CONSTRAINT fk_link_claim_event_litigation_hub_litigation FOREIGN KEY (litigation_hash_key) REFERENCES hub_litigation (litigation_hash_key);

ALTER TABLE link_claim_event_medical_assessment
	ADD CONSTRAINT fk_link_claim_event_medical_assessment_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_medical_assessment
	ADD CONSTRAINT fk_link_claim_event_medical_assessment_hub_medical_assessment FOREIGN KEY (medical_assessment_hash_key) REFERENCES hub_medical_assessment (medical_assessment_hash_key);

ALTER TABLE link_claim_event_repair
	ADD CONSTRAINT fk_link_claim_event_repair_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_repair
	ADD CONSTRAINT fk_link_claim_event_repair_hub_repair FOREIGN KEY (repair_hash_key) REFERENCES hub_repair (repair_hash_key);

ALTER TABLE link_claim_event_settlement
	ADD CONSTRAINT fk_link_claim_event_settlement_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_settlement
	ADD CONSTRAINT fk_link_claim_event_settlement_hub_settlement FOREIGN KEY (settlement_hash_key) REFERENCES hub_settlement (settlement_hash_key);

ALTER TABLE link_claim_event_treatment
	ADD CONSTRAINT fk_link_claim_event_treatment_hub_claim_event FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE link_claim_event_treatment
	ADD CONSTRAINT fk_link_claim_event_treatment_hub_treatment FOREIGN KEY (treatment_hash_key) REFERENCES hub_treatment (treatment_hash_key);

ALTER TABLE link_claim_health_insurance_claim
	ADD CONSTRAINT fk_link_claim_health_insurance_claim_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_health_insurance_claim
	ADD CONSTRAINT fk_link_claim_health_insurance_claim_hub_health_insurance_claim FOREIGN KEY (health_insurance_claim_hash_key) REFERENCES hub_health_insurance_claim (health_insurance_claim_hash_key);

ALTER TABLE link_claim_litigation
	ADD CONSTRAINT fk_link_claim_litigation_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_litigation
	ADD CONSTRAINT fk_link_claim_litigation_hub_litigation FOREIGN KEY (litigation_hash_key) REFERENCES hub_litigation (litigation_hash_key);

ALTER TABLE link_claim_loss_event
	ADD CONSTRAINT fk_link_claim_loss_event_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_loss_event
	ADD CONSTRAINT fk_link_claim_loss_event_hub_loss_event FOREIGN KEY (loss_event_hash_key) REFERENCES hub_loss_event (loss_event_hash_key);

ALTER TABLE link_claim_medical_report
	ADD CONSTRAINT fk_link_claim_medical_report_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_medical_report
	ADD CONSTRAINT fk_link_claim_medical_report_hub_medical_report FOREIGN KEY (medical_report_hash_key) REFERENCES hub_medical_report (medical_report_hash_key);

ALTER TABLE link_claim_participant_repair
	ADD CONSTRAINT fk_link_claim_participant_repair_hub_claim_participant FOREIGN KEY (claim_participant_hash_key) REFERENCES hub_claim_participant (claim_participant_hash_key);

ALTER TABLE link_claim_participant_repair
	ADD CONSTRAINT fk_link_claim_participant_repair_hub_repair FOREIGN KEY (repair_hash_key) REFERENCES hub_repair (repair_hash_key);

ALTER TABLE link_claim_participant_treatment
	ADD CONSTRAINT fk_link_claim_participant_treatment_hub_claim_participant FOREIGN KEY (claim_participant_hash_key) REFERENCES hub_claim_participant (claim_participant_hash_key);

ALTER TABLE link_claim_participant_treatment
	ADD CONSTRAINT fk_link_claim_participant_treatment_hub_treatment FOREIGN KEY (treatment_hash_key) REFERENCES hub_treatment (treatment_hash_key);

ALTER TABLE link_claim_police_report
	ADD CONSTRAINT fk_link_claim_police_report_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_claim_police_report
	ADD CONSTRAINT fk_link_claim_police_report_hub_police_report FOREIGN KEY (police_report_hash_key) REFERENCES hub_police_report (police_report_hash_key);

ALTER TABLE link_coverage_policy_coverage
	ADD CONSTRAINT fk_link_coverage_policy_coverage_hub_coverage FOREIGN KEY (coverage_hash_key) REFERENCES hub_coverage (coverage_hash_key);

ALTER TABLE link_coverage_policy_coverage
	ADD CONSTRAINT fk_link_coverage_policy_coverage_hub_policy_coverage FOREIGN KEY (policy_coverage_hash_key) REFERENCES hub_policy_coverage (policy_coverage_hash_key);

ALTER TABLE link_injury_diagnosis
	ADD CONSTRAINT fk_link_injury_diagnosis_hub_injury FOREIGN KEY (injury_hash_key) REFERENCES hub_injury (injury_hash_key);

ALTER TABLE link_injury_diagnosis
	ADD CONSTRAINT fk_link_injury_diagnosis_hub_diagnosis FOREIGN KEY (diagnosis_hash_key) REFERENCES hub_diagnosis (diagnosis_hash_key);

ALTER TABLE link_insured_entity_policy_coverage
	ADD CONSTRAINT fk_link_insured_entity_policy_coverage_hub_insured_entity FOREIGN KEY (insured_entity_hash_key) REFERENCES hub_insured_entity (insured_entity_hash_key);

ALTER TABLE link_insured_entity_policy_coverage
	ADD CONSTRAINT fk_link_insured_entity_policy_coverage_hub_policy_coverage FOREIGN KEY (policy_coverage_hash_key) REFERENCES hub_policy_coverage (policy_coverage_hash_key);

ALTER TABLE link_loss_event_catastrophe
	ADD CONSTRAINT fk_link_loss_event_catastrophe_hub_loss_event FOREIGN KEY (loss_event_hash_key) REFERENCES hub_loss_event (loss_event_hash_key);

ALTER TABLE link_loss_event_catastrophe
	ADD CONSTRAINT fk_link_loss_event_catastrophe_hub_catastrophe FOREIGN KEY (catastrophe_hash_key) REFERENCES hub_catastrophe (catastrophe_hash_key);

ALTER TABLE link_medical_condition_diagnosis
	ADD CONSTRAINT fk_link_medical_condition_diagnosis_hub_medical_condition FOREIGN KEY (medical_condition_hash_key) REFERENCES hub_medical_condition (medical_condition_hash_key);

ALTER TABLE link_medical_condition_diagnosis
	ADD CONSTRAINT fk_link_medical_condition_diagnosis_hub_diagnosis FOREIGN KEY (diagnosis_hash_key) REFERENCES hub_diagnosis (diagnosis_hash_key);

ALTER TABLE link_medical_condition_treatment
	ADD CONSTRAINT fk_link_medical_condition_treatment_hub_medical_condition FOREIGN KEY (medical_condition_hash_key) REFERENCES hub_medical_condition (medical_condition_hash_key);

ALTER TABLE link_medical_condition_treatment
	ADD CONSTRAINT fk_link_medical_condition_treatment_hub_treatment FOREIGN KEY (treatment_hash_key) REFERENCES hub_treatment (treatment_hash_key);

ALTER TABLE link_person_claim_participant
	ADD CONSTRAINT fk_link_person_claim_participant_hub_person FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE link_person_claim_participant
	ADD CONSTRAINT fk_link_person_claim_participant_hub_claim_participant FOREIGN KEY (claim_participant_hash_key) REFERENCES hub_claim_participant (claim_participant_hash_key);

ALTER TABLE link_physical_place_loss_event
	ADD CONSTRAINT fk_link_physical_place_loss_event_hub_physical_place FOREIGN KEY (physical_place_hash_key) REFERENCES hub_physical_place (physical_place_hash_key);

ALTER TABLE link_physical_place_loss_event
	ADD CONSTRAINT fk_link_physical_place_loss_event_hub_loss_event FOREIGN KEY (loss_event_hash_key) REFERENCES hub_loss_event (loss_event_hash_key);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT fk_link_policy_claim_hub_policy FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_claim
	ADD CONSTRAINT fk_link_policy_claim_hub_claim FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE link_policy_policy_coverage
	ADD CONSTRAINT fk_link_policy_policy_coverage_hub_policy FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE link_policy_policy_coverage
	ADD CONSTRAINT fk_link_policy_policy_coverage_hub_policy_coverage FOREIGN KEY (policy_coverage_hash_key) REFERENCES hub_policy_coverage (policy_coverage_hash_key);

ALTER TABLE sat_catastrophe
	ADD CONSTRAINT fk_sat_catastrophe_hub FOREIGN KEY (catastrophe_hash_key) REFERENCES hub_catastrophe (catastrophe_hash_key);

ALTER TABLE sat_claim_crm
	ADD CONSTRAINT fk_sat_claim_hub FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE sat_claim_event
	ADD CONSTRAINT fk_sat_claim_event_hub FOREIGN KEY (claim_event_hash_key) REFERENCES hub_claim_event (claim_event_hash_key);

ALTER TABLE sat_claim_investigation_crm
	ADD CONSTRAINT fk_sat_claim_investigation_hub FOREIGN KEY (claim_investigation_hash_key) REFERENCES hub_claim_investigation (claim_investigation_hash_key);

ALTER TABLE sat_claim_investigation_sap
	ADD CONSTRAINT R_82 FOREIGN KEY (claim_investigation_hash_key) REFERENCES hub_claim_investigation (claim_investigation_hash_key);

ALTER TABLE sat_claim_participant
	ADD CONSTRAINT fk_sat_claim_participant_hub FOREIGN KEY (claim_participant_hash_key) REFERENCES hub_claim_participant (claim_participant_hash_key);

ALTER TABLE sat_claim_sap
	ADD CONSTRAINT R_80 FOREIGN KEY (claim_hash_key) REFERENCES hub_claim (claim_hash_key);

ALTER TABLE sat_coverage
	ADD CONSTRAINT fk_sat_coverage_hub FOREIGN KEY (coverage_hash_key) REFERENCES hub_coverage (coverage_hash_key);

ALTER TABLE sat_diagnosis
	ADD CONSTRAINT fk_sat_diagnosis_hub FOREIGN KEY (diagnosis_hash_key) REFERENCES hub_diagnosis (diagnosis_hash_key);

ALTER TABLE sat_health_insurance_claim
	ADD CONSTRAINT fk_sat_health_insurance_claim_hub FOREIGN KEY (health_insurance_claim_hash_key) REFERENCES hub_health_insurance_claim (health_insurance_claim_hash_key);

ALTER TABLE sat_injury
	ADD CONSTRAINT fk_sat_injury_hub FOREIGN KEY (injury_hash_key) REFERENCES hub_injury (injury_hash_key);

ALTER TABLE sat_insured_entity
	ADD CONSTRAINT fk_sat_insured_entity_hub FOREIGN KEY (insured_entity_hash_key) REFERENCES hub_insured_entity (insured_entity_hash_key);

ALTER TABLE sat_litigation
	ADD CONSTRAINT fk_sat_litigation_hub FOREIGN KEY (litigation_hash_key) REFERENCES hub_litigation (litigation_hash_key);

ALTER TABLE sat_loss_event_crm
	ADD CONSTRAINT R_12 FOREIGN KEY (loss_event_hash_key) REFERENCES hub_loss_event (loss_event_hash_key);

ALTER TABLE sat_loss_event_sap
	ADD CONSTRAINT R_83 FOREIGN KEY (loss_event_hash_key) REFERENCES hub_loss_event (loss_event_hash_key);

ALTER TABLE sat_medical_assessment
	ADD CONSTRAINT fk_sat_medical_assessment_hub FOREIGN KEY (medical_assessment_hash_key) REFERENCES hub_medical_assessment (medical_assessment_hash_key);

ALTER TABLE sat_medical_condition
	ADD CONSTRAINT fk_sat_medical_condition_hub FOREIGN KEY (medical_condition_hash_key) REFERENCES hub_medical_condition (medical_condition_hash_key);

ALTER TABLE sat_medical_report
	ADD CONSTRAINT fk_sat_medical_report_hub FOREIGN KEY (medical_report_hash_key) REFERENCES hub_medical_report (medical_report_hash_key);

ALTER TABLE sat_person
	ADD CONSTRAINT fk_sat_person_hub FOREIGN KEY (person_hash_key) REFERENCES hub_person (person_hash_key);

ALTER TABLE sat_physical_place
	ADD CONSTRAINT fk_sat_physical_place_hub FOREIGN KEY (physical_place_hash_key) REFERENCES hub_physical_place (physical_place_hash_key);

ALTER TABLE sat_police_report
	ADD CONSTRAINT fk_sat_police_report_hub FOREIGN KEY (police_report_hash_key) REFERENCES hub_police_report (police_report_hash_key);

ALTER TABLE sat_policy
	ADD CONSTRAINT fk_sat_policy_hub FOREIGN KEY (policy_hash_key) REFERENCES hub_policy (policy_hash_key);

ALTER TABLE sat_policy_coverage
	ADD CONSTRAINT fk_sat_policy_coverage_hub FOREIGN KEY (policy_coverage_hash_key) REFERENCES hub_policy_coverage (policy_coverage_hash_key);

ALTER TABLE sat_repair
	ADD CONSTRAINT fk_sat_repair_hub FOREIGN KEY (repair_hash_key) REFERENCES hub_repair (repair_hash_key);

ALTER TABLE sat_settlement
	ADD CONSTRAINT fk_sat_settlement_hub FOREIGN KEY (settlement_hash_key) REFERENCES hub_settlement (settlement_hash_key);

ALTER TABLE sat_treatment
	ADD CONSTRAINT fk_sat_treatment_hub FOREIGN KEY (treatment_hash_key) REFERENCES hub_treatment (treatment_hash_key);
