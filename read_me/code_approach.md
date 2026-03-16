cfg = load_config()
→ Loads your configuration (likely from a JSON/YAML file) into a Python dict named cfg.

seed = cfg["run_settings"]["random_seed"]
→ Reads the random seed value from config (under run_settings.random_seed). This is used to make generation reproducible.

random.seed(seed)
→ Sets Python’s random generator to that seed, so the same config + seed produces the same “random” outputs every run.

run_id = get_run_id(seed)
→ Creates a run identifier derived from the seed (often something like a timestamp+seed or a hash). Purpose: uniquely label this execution.

out = f"output/{run_id}"
→ Builds the output folder path where CSVs/files for this run will be written, e.g. output/20260216_1234 or output/seed_1234.


This block is doing one thing: it calls a “hub builder” once, then unpacks everything it produced into local variables so the rest of the pipeline can use them. Step-by-step:

1) ctx = build_hubs(cfg, run_id)

Runs your hub-generation phase.

It uses:

cfg (how many rows, countries, scenario rules, cardinality, etc.)

run_id (to stamp/load-date/run metadata, or to keep output organized)

It returns a context dictionary ctx that contains:

the generated hub rows (lists of dicts or rows)

lookup maps (person → hash key, id → hash key, etc.)

entity sets (leads, prospects, customers)

other helper mappings needed later for links/sats

So ctx is basically: “everything we generated + all the maps required to connect entities later.”

2) hub_person_rows = ctx["hub_person_rows"] etc. (Unpacking)

You’re extracting pieces of ctx into named variables for readability.

Person hub + classifications

hub_person_rows: the actual rows for Hub Person

person_hks: list/set of person hash keys

person_type: mapping like person_hk -> Lead/Prospect/Customer or similar

hub_nat, hub_leg: likely additional person subtypes:

natural person hub rows

legal entity hub rows

person_to_nat, person_to_leg: maps connecting a person to nat/leg entities

Product hub

hub_prod_rows: rows for Hub Product

prod_hk_by_id: map product_id -> product_hash_key

Lead & Customer-related outputs

leads, prospects, customers_all: collections of person ids/hks split by type

channels: list/dict of channels generated/used (web, agent, branch, etc.)

hub_lead_rows: rows for Hub Lead

person_to_lead: map person_hk -> lead_hk (or person -> lead entity)

hub_cust_rows: rows for Hub Customer

person_to_customer: map person_hk -> customer_hk

Identity / Contact / Consent / Account / Preferences hubs

Each of these follows the same pattern:

hub_id_rows + person_to_identity

hub_con_rows + person_to_contact

hub_cns_rows + person_to_consent

hub_acc_rows + person_to_account

hub_mpr_rows + person_to_mpr (marketing preference?)

hub_men_rows + person_to_men (maybe “marketing engagement” / “mention” / whatever MEN stands for in your model)

These are all:
hub rows + a mapping to attach them back to the person later (for links/sats).

Quote hub

hub_quo_rows: rows for Hub Quote

person_to_quote: map linking persons to their quote hash keys

quote_persons: subset/list of persons who actually got quotes (useful for KPI logic like “% leads with ≥1 quote”)

3) assert_unique(hub_person_rows, "Person Hash Key")

This is a primary key integrity check for Hub Person.

It verifies the column "Person Hash Key" is unique across hub_person_rows.

If duplicates exist, it fails early (good).

“unchanged” means you want this guardrail to remain even after modularization.

Step-by-step, what this block does (no jumping):

---

## 1) Build Policies for customers (kept in main)

```python
cust_person_set = set(customers_all)
hub_pol_rows, policy_person_map = hub_policy(cust_person_set, run_id)
```

* `customers_all` is your list of customer persons (likely person hash keys).
* You convert it to a set: `cust_person_set` (faster lookups, no duplicates).
* `hub_policy(...)` generates **Hub Policy rows** for those customers and returns:

  * `hub_pol_rows`: the actual policy hub rows (each with a Policy Hash Key etc.)
  * `policy_person_map`: a mapping that ties each policy back to a person (customer).
    Example shape (typical): `policy_hk -> person_hk` or `policy_hk -> {"person_hk":..., ...}`.

So: **only customers get policies**.

---

## 2) Assign Product to each Policy (kept in main)

```python
policy_to_product_id = assign_product_for_policy(policy_person_map, person_type)
```

* Takes your `policy_person_map` and `person_type`.
* Produces a dictionary mapping each **policy** to a **product id**.
  Example: `policy_hk -> "PRD_MOTOR_PERSONAL"`.

So: **policy gets a product** based on the person type rules.

---

## 3) Build Quote → Product mapping (kept in main)

```python
quote_to_product_id = {}
```

You start an empty map: `quote_hk -> product_id`.

### Loop through quotes per person

```python
for person_hk, quote_hks in person_to_quote.items():
    for quote_hk in quote_hks:
```

* `person_to_quote` is likely: `person_hk -> [quote_hk1, quote_hk2, ...]`
* This code ensures you handle **multiple quotes per person** (that `for quote_hk in quote_hks` is key).

### Choose product based on person type

```python
if person_type[person_hk] == "NATURAL":
    quote_to_product_id[quote_hk] = random.choices(
        ["PRD_MOTOR_PERSONAL", "PRD_HOME_PERSONAL"], [0.6, 0.4]
    )[0]
else:
    quote_to_product_id[quote_hk] = random.choices(
        ["PRD_COMMERCIAL_MOTOR", "PRD_PROPERTY_COMMERCIAL"], [0.6, 0.4]
    )[0]
```

* If person is **NATURAL**, quote gets either:

  * motor personal (60%)
  * home personal (40%)
* Else (non-natural / legal), quote gets either:

  * commercial motor (60%)
  * commercial property (40%)

So: **each quote independently gets a product** based on person type.

---

## 4) Create Link rows for Quote ↔ Product

```python
link_quote_product = []
for qhk, prod_id in quote_to_product_id.items():
    link_quote_product.append(
        make_link("Quote Product", {"Quote Hash Key": qhk, "Product Hash Key": prod_hk_by_id[prod_id]})
    )
```

* Iterates quote→product map.
* Converts product *id* to product *hash key* using `prod_hk_by_id`.
* Uses `make_link(...)` to create a standardized DV link row for “Quote Product”.
* Collects all rows in `link_quote_product`.

So: **builds Link_Quote_Product dataset**.

---

## 5) Write the link to CSV

```python
write_csv(out, "Link_Quote_Product.csv", link_quote_product)
```

* Writes the link table to your run output folder.

---

## One-line summary

This block **creates policies only for customers, assigns products to policies, assigns products to each quote based on person type with weighted randomness, builds the Quote↔Product link rows, and writes them to CSV.**
