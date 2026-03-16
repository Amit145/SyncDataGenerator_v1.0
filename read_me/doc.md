Perfect 👍
Now we’ll create a **proper technical documentation** for your Synthetic Customer 360 Data Vault Generator — not just cardinality — but full architecture, flow, modules, logic, validations, and design intent.

You can directly use this as:

```
SYNTHETIC_DATA_VAULT_ENGINE_DESIGN.md
```

---

# 📘 Synthetic Customer 360 – Data Vault 2.0 Generator

**Version:** v2
**Approach:** Faker-based, Config-driven, Cardinality-controlled
**Architecture:** Modular Python

---

# 1️⃣ Objective

This system generates **fully referentially-integrated synthetic Customer 360 data** aligned to:

* Logical Model (Customer 360)
* Data Vault 2.0 modeling principles
* DDL schema definitions
* Configurable business scenarios
* Deterministic reproducibility via seed
* Bounded cardinality enforcement

It simulates a real insurance lifecycle:

```
Person → Lead → Customer → Quote → Policy → Product → Asset (Motor/Home)
```

---

# 2️⃣ Architectural Principles

### ✅ Data Vault 2.0 Compliance

* Hubs = Business Keys
* Links = Relationships
* Satellites = Descriptive history
* Hash-based surrogate keys

### ✅ Deterministic Execution

* Controlled via `random_seed`
* Same seed → same dataset

### ✅ Configurable

* Run settings in `config.json`
* Cardinality rules in `cardinality.json`
* Scenario-based generation

### ✅ Referential Integrity First

All links built from existing hub hash keys only.

---

# 3️⃣ High-Level Code Flow

## Step 1 — Initialization

```python
cfg = load_config()
seed = cfg["run_settings"]["random_seed"]
random.seed(seed)
run_id = get_run_id(seed)
```

Purpose:

* Load configuration
* Make run reproducible
* Create output folder

---

## Step 2 — Build Hubs

Executed via:

```python
ctx = build_hubs(cfg, run_id)
```

### Hubs Created

* Hub_Person
* Hub_Natural_Person
* Hub_Legal_Person
* Hub_Product
* Hub_Lead
* Hub_Customer
* Hub_Quote
* Hub_Policy
* Hub_Motor
* Hub_Home
* Hub_Account
* Hub_Contact
* Hub_Consent
* Hub_Identities
* Hub_Marketing_Preference
* Hub_Marketing_Engagement

Each hub:

* Generates business ID
* Generates hash key (MD5 of business key)
* Stores metadata (load_date, record_source)

### Important Rule

Hub = 1 row per business key
No duplicates allowed
Validated by:

```python
assert_unique(...)
```

---

# 4️⃣ Link Construction Logic

Links are built using:

```
helper/link_builder.py
```

## Cardinality Enforcement

Each link reads from:

```
config/cardinality.json
```

Example:

```json
"Link_Person_Contact": { "min": 1, "max": 2 }
```

Process:

1. Identify parent set
2. Randomly select child count between min and max
3. Create link row
4. Generate Link PK as:

```
MD5(concatenated foreign keys)
```

This guarantees:

* No duplicate link rows
* Deterministic relationship identity

---

# 5️⃣ Detailed Relationship Semantics

## 👤 Person Domain

| Relationship        | Business Meaning                              |
| ------------------- | --------------------------------------------- |
| Person → Natural    | Every person is an individual (current setup) |
| Person → Legal      | Optional company representation               |
| Person → Contact    | Must have 1–2 contact methods                 |
| Person → Identities | Must have 1–3 identities                      |
| Person → Consent    | Optional consent records                      |
| Person → Account    | Must have at least 1 account                  |
| Person → Lead       | May have lead history                         |

---

## 🧑 Customer Domain

Customer is a role of Person.

| Relationship      | Meaning                 |
| ----------------- | ----------------------- |
| Customer → Person | Mandatory               |
| Customer → Lead   | Historical lead linkage |

---

## 📄 Quote Domain

Quote created per eligible person.

Product assigned based on person type:

```python
if NATURAL:
    choose motor_personal or home_personal
else:
    choose commercial variants
```

Currently:

Quote → Product = effectively 1..1

---

## 📑 Policy Domain

Policies only created for Customers.

| Relationship      | Meaning   |
| ----------------- | --------- |
| Policy → Customer | Mandatory |
| Policy → Product  | Mandatory |

Policies simulate conversion from Quote.

---

## 🏷 Product Domain

Product acts as master offering.

| Relationship    | Meaning |
| --------------- | ------- |
| Product → Motor | 1–3     |
| Product → Home  | 0–1     |

Motor/Home represent insured asset entities.

---

# 6️⃣ Satellite Strategy

Satellites store descriptive attributes:

Examples:

* Person name, DOB
* Contact value
* Policy premium
* Product type
* Consent flag

Design:

* 1 satellite row per hub per run
* SCD2-ready (load_date exists)
* Can be extended to multi-version later

---

# 7️⃣ Validation Layer

After generation:

## 1️⃣ PK Validation

Ensure unique hash keys in hubs and links.

## 2️⃣ Referential Integrity Check

Every FK in Link must exist in corresponding Hub.

## 3️⃣ DDL Validation

Columns match expected schema.

## 4️⃣ KPI Report

Conversion ratios:

* Leads → Customers
* Quotes → Policies
* Product distribution

---

# 8️⃣ Determinism Design

Because:

```
random.seed(seed)
```

The entire dataset is reproducible.

This is important for:

* Unit testing
* Regression testing
* Demo stability

---

# 9️⃣ Data Output Structure

```
output/<run_id>/
    hub_person.csv
    hub_customer.csv
    link_person_contact.csv
    sat_person.csv
    ...
```

All files:

* Lowercase
* Underscore naming
* DDL aligned

---

# 🔟 Design Strengths

✔ Fully modular
✔ Config-driven
✔ Deterministic
✔ Referentially safe
✔ Cardinality bounded
✔ DDL aligned
✔ Scenario adaptable
✔ Databricks / Postgres ready

---

# 1️⃣1️⃣ Known Design Constraints

* Quote_Product config allows 1..2, code generates 1
* Person subtype forces natural
* Satellites currently single-version

These are design decisions, not structural flaws.

---

# 1️⃣2️⃣ How This System Should Be Positioned

This is not a simple Faker script.

It is:

> A configurable Data Vault synthetic data engine with deterministic cardinality control, business lifecycle modeling, and enterprise validation support.

---

If you want next:

I can now create:

* 📊 A clean architecture flow diagram
* 🏗 A module-level dependency diagram
* 📈 A lifecycle flow diagram (Lead → Policy)
* 📄 Or generate a polished PDF version for sharing internally

Tell me which direction you want.
