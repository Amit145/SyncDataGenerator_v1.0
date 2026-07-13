# Claims Product Flow

The claims product is an optional generation path that creates a claim-domain stack from the claim CDM/LDM and Data Vault references under `claims/`.

## Inputs

- `claims/Business_CDM_&_LDM/CLAIM_CONCEPTUAL_MODEL v1.PDF`
- `claims/Business_CDM_&_LDM/CLAIM_LOGICAL_MODEL v1.PDF`
- `claims/Business_CDM_&_LDM/Claim - Definition and Table Structure.xlsx`
- `claims/DataVault/DATAVAULT_CLAIM.PDF`

The PDFs are image-based model diagrams. The XLSX workbook provides the machine-readable entity definitions and sample LDM table columns.

## Enablement

Claims generation is disabled by default to avoid slowing normal synthetic runs.

Run once from CLI:

```powershell
python main.py --include-claims-product
```

Or enable in `config/scenario_v1.json`:

```json
"output_settings": {
  "generate_claims_product": true
}
```

## Output Layers

For run id `<run_id>`, the claims product writes:

- `data/raw/claims/<run_id>/raw`
- `data/bronze/claims/<run_id>`
- `data/silver/claims/<run_id>`
- `data/gold/claims/<run_id>`

## Raw Layer

Raw claims is generated from the LDM entity set:

- `coverage`
- `policy_coverage`
- `person`
- `claim_participant`
- `insured_entity`
- `policy`
- `claim`
- `loss_event`
- `claim_event`
- `physical_place`
- `catastrophe`
- `treatment`
- `health_insurance_claim`
- `police_report`
- `medical_report`
- `settlement`
- `injury`
- `claim_investigation`
- `repair`
- `litigation`
- `medical_assessment`
- `medical_condition`
- `diagnosis`

The raw generator links claims back to the existing generated policy/person context. This preserves referential integrity with generated customers, policies, products, homes, and motors where applicable.

Raw business keys use full `identifier` naming. Duplicate short `id` columns are intentionally not generated when an equivalent `identifier` column exists. For example, raw `claim.csv` uses `claim_identifier`, `policy_identifier`, `policy_coverage_identifier`, and `insured_entity_identifier`; it does not also emit `claim_id` or `policy_id`.

Raw physical file names use source-style names instead of direct logical entity names. Examples:

- `claim_register.csv` becomes bronze `claim.csv`
- `policy_register.csv` becomes bronze `policy.csv`
- `party_master.csv` becomes bronze `person.csv`
- `claim_event_log.csv` becomes bronze `claim_event.csv`
- `coverage_catalog.csv` becomes bronze `coverage.csv`
- `medical_condition_catalog.csv` becomes bronze `medical_condition.csv`

This keeps raw looking like source extracts while bronze remains aligned to the logical model and silver vault builder.

Additional claim LDM attributes covered in raw/bronze/silver include:

- `litigation.date_of_legal_representation`
- `medical_assessment.medical_report_flag`
- `medical_assessment.psychological_factors`
- `claim_investigation.investigator_flag`
- `claim_investigation.claim_handler_notes`
- `treatment.treatment_code_standard`
- `treatment.initial_treatment_date`
- `treatment.medication_code`
- `treatment.medical_condition_identifier`
- person tenant/source/consent/rating/rate-class fields
- physical place elevation, business interruption, census, commune, geocoding, district fields
- expanded loss-event fields for damage, negligence, alcohol/drugs, vehicle/people/injury counts, NatCat/catastrophe/fatality flags, notification, effective loss date, event dates/status/type/time/name/description, and property/liability cause

## Bronze Layer

Bronze is a normalized copy of the raw LDM files. It is intentionally close to raw so downstream SQL/modeling can use stable file/table names while raw remains the source extract representation.

## Silver Layer

Silver is a claims Data Vault projection. It creates hubs, satellites, and links for the claim-domain entities.

Examples:

- Hubs: `hub_claim`, `hub_policy`, `hub_coverage`, `hub_loss_event`, `hub_claim_event`, `hub_person`, `hub_claim_participant`
- Satellites: `sat_claim`, `sat_policy`, `sat_coverage`, `sat_loss_event`, `sat_claim_event`, `sat_claim_participant`
- Links: `link_claim_policy`, `link_claim_policy_coverage`, `link_claim_insured_entity`, `link_claim_loss_event`, `link_claim_event_claim`, `link_claim_participant_person`

Hash keys are deterministic MD5 hashes of source business identifiers. Link hash keys are deterministic MD5 hashes of the endpoint hash keys in relationship order.

## Gold Layer

Gold currently creates:

- `fact_claim.csv`
- `claim_status_summary.csv`

These are lightweight claim analytics outputs over the bronze LDM data.

## Validation

Run:

```powershell
python misc/verify_claims_product.py --run-id <run_id>
```

The verifier checks:

- expected bronze files exist
- raw/LDM primary keys are unique and non-blank
- raw/LDM does not emit duplicate short `*_id` business columns where `*_identifier` is the source key
- raw/LDM foreign keys resolve
- claim dates are valid against policy dates
- loss notification date is not before loss date, effective loss date equals loss date, and loss-event end date is not before start date
- litigation legal representation date is not after first litigation date
- expected silver hub/link/satellite files exist
- silver hub hash keys are unique and non-blank
- silver link hash keys resolve to parent hubs
- gold `fact_claim.csv` and `claim_status_summary.csv` exist

## Code Entry Points

- Raw LDM generation: `generators/claim_ldm_generator.py`
- Bronze/silver/gold build: `misc/claims_product_pipeline.py`
- Validation: `misc/verify_claims_product.py`
- Main integration: `main.py --include-claims-product`
