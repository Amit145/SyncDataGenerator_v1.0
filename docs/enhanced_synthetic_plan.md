# Enhanced Synthetic Generation Plan

## Goal

Add a separate enhanced synthetic output without breaking the existing base 360 generation flow.

The current base pipeline remains unchanged:

- `data/output/<run_id>`
- `data/synthetic/base/<run_id>`
- `data/scd2/base/<run_id>`
- existing raw, canonical, silver, and validation behavior

The enhanced pipeline should generate a new output containing the full enhanced Data Vault model:

- all existing 52 base 360 tables
- plus the enhanced-only tables from `enhanced_360`
- expected total: 73 tables

The enhanced output should be generated from the same synthetic run context as the base output so all records are connected consistently.

## Source Inputs

Use the enhanced DDL as the target schema contract:

- `enhanced_360/Enhanced_Customer360_DataVault_DDL.sql`

Use the sample files as seed/reference data for values and distributions:

- `enhanced_360/data_example/*.csv`

The DDL defines:

- table names
- column names
- primary keys
- foreign key relationships
- required enhanced output shape

The sample data can guide:

- realistic attribute values
- status/reason/category distributions
- date/value ranges
- reference data pools

Sample data should not be copied blindly unless explicitly required. Enhanced records should be generated from the base synthetic universe first, then populated with realistic values from the examples.

## Model Scope

Base DDL:

- 17 hubs
- 18 links
- 17 satellites
- 52 tables total

Enhanced DDL:

- 24 hubs
- 25 links
- 24 satellites
- 73 tables total

Enhanced-only additions:

- 7 hubs
- 7 links
- 7 satellites
- 21 tables total

Enhanced-only entity groups:

- `broker`
- `campaign`
- `channel`
- `claim`
- `complaints`
- `override`
- `regulations`

## Output Location

Preferred output layout:

- `data/synthetic/enhanced/<run_id>`
- `data/scd2/enhanced/<run_id>`

The exact folder names can be finalized during implementation, but enhanced output should be separate from base output.

## Integration Rule

Base 360 is the core customer-policy-product spine. Enhanced records should attach to existing base records through deterministic relationships.

Every enhanced transactional entity must link back to an existing base entity. Do not generate isolated enhanced records and try to match them later.

Recommended links:

- `broker` links to `person` through `link_person_broker`
- `campaign` links to `person` through `link_person_campaign`
- `channel` links to `policy` through `link_policy_channel`
- `claim` links to `policy` through `link_policy_claim`
- `complaints` links to `customer` through `link_complaints_customer`
- `override` links to `policy` through `link_override_policy`
- `regulations` links to `product` through `link_regulations_product`

Use existing base context maps where possible:

- `person_to_quote`
- `policy_person_map`
- `policy_to_product_id`
- `person_to_customer`
- `hub_prod_rows`
- `sat_policy`
- `sat_customer`
- `sat_quote`

## Count Rules

The base config creates the synthetic universe. Enhanced counts should be derived from that universe by entity type.

Do not apply `total_people` directly to every enhanced table.

Recommended count behavior:

| Enhanced entity | Count rule | Reason |
|---|---:|---|
| `broker` | fixed/reference pool | Brokers are reused across many persons/policies |
| `campaign` | fixed/reference pool | Campaigns are reused across many leads/persons |
| `channel` | fixed/reference pool | Channel is a small reference domain |
| `claim` | percentage of policies | Claims occur against policies |
| `complaints` | percentage of customers or policies | Complaints come from customers |
| `override` | percentage of policies | Overrides apply to selected policies |
| `regulations` | fixed/reference pool or capped sample count | Regulations apply to products |

Example future config shape:

```json
{
  "enhanced_settings": {
    "enabled": true,
    "broker_count": 20,
    "campaign_count": 10,
    "channel_count": 4,
    "claim_policy_rate": 0.08,
    "complaint_customer_rate": 0.02,
    "override_policy_rate": 0.05,
    "regulation_count": 100
  }
}
```

## Consistency Rules

Suggested business consistency:

- Claims should only exist for existing policies.
- Claim dates should fall within, or close to, the linked policy period.
- Complaint dates should be after the customer exists.
- Overrides should only exist for selected policies.
- Channels should align with policy sales channel where possible.
- Campaigns should mainly attach to lead or quote persons.
- Regulations should attach to existing products.
- Broker assignments should attach to persons involved in quotes or policies.

## SCD2 Support

Enhanced should support SCD2 behavior similar to the base pipeline.

Expected behavior:

- First enhanced run creates the enhanced baseline output.
- Later runs can create SCD2-style changed satellite records.
- Enhanced SCD2 output should be separate from base SCD2.

Recommended location:

- `data/scd2/enhanced/<run_id>`

Enhanced SCD2 should include enhanced satellite tables and preserve compatibility with existing base SCD2 behavior.

## Verification

Enhanced verification should be separate from base verification.

It should check:

- all 73 enhanced tables exist
- columns match the enhanced DDL
- primary keys are unique
- link foreign keys exist in the referenced hubs
- satellite foreign keys exist in the referenced hubs
- no orphan enhanced rows are generated
- key business consistency rules pass

Recommended verifier:

- parse or load enhanced DDL metadata
- validate table/file presence
- validate columns
- validate PK uniqueness
- validate FK integrity
- report counts by hub/link/satellite group

## Run And Verify

Run the full generation flow:

```powershell
$env:PYTHONUTF8='1'
python .\main.py
```

This keeps the existing base outputs and also writes enhanced outputs:

- base output: `data/output/<run_id>`
- base normalized synthetic: `data/synthetic/base/<run_id>`
- enhanced synthetic: `data/synthetic/enhanced/<run_id>`
- enhanced SCD2, when a previous enhanced run exists: `data/scd2/enhanced/<run_id>`

Verify the latest enhanced synthetic output:

```powershell
python .\misc\verify_enhanced_synthetic.py
```

Verify a specific enhanced synthetic folder:

```powershell
python .\misc\verify_enhanced_synthetic.py .\data\synthetic\enhanced\<run_id>
```

Build silver outputs for all latest raw sources:

```powershell
python .\misc\transform_all_raw_to_silver.py
```

Verify all latest silver outputs:

```powershell
python .\misc\verify_all_silver.py
```

Expected successful checks:

- `main.py` base DDL/file check passes
- `main.py` base referential and business integrity check passes
- enhanced verifier reports 73 expected tables
- enhanced verifier reports 24 hubs, 25 links, and 24 satellites
- enhanced verifier passes column, primary key, and foreign key checks
- silver verifier passes for available latest source outputs

## Implementation Direction

Preferred implementation approach:

1. Keep existing base generation unchanged.
2. Add an enhanced schema/metadata layer for the enhanced DDL.
3. Add an enhanced generator that consumes the existing base context from the run.
4. Write all 73 enhanced tables to a separate enhanced output folder.
5. Add enhanced SCD2 generation.
6. Add enhanced verification.

This keeps the current base pipeline stable while adding a parallel enhanced pipeline.
