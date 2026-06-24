from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.storage_paths import SYNTHETIC_MLOPS_ROOT


DDL_PATH = ROOT / "mlops" / "Enhanced_Customer360_Dimensional_Model_DDL.sql"
OUT_ROOT = ROOT / "data" / "dim_fact_direct" / "mlops"
FAR_FUTURE = "9999-12-31T00:00:00"
CREATED_BY = "DIRECT_DIM_FACT_GENERATOR"
ONBOARDING_FEEDBACK_PHRASES = {
    "POSITIVE": [
        "Comprehensive cover for the price for appropriate policy",
        "Flexible excess options available",
        "Policy documents easy to understand",
        "Good cover options for the premium",
        "Online quote was quick and easy to complete",
        "Documents arrived promptly after purchase",
        "Cover level matched my needs clearly",
        "Payment setup was simple and transparent",
        "Agent explained the policy options well",
        "Smooth onboarding with no repeated questions",
    ],
    "NEUTRAL": [
        "Cover options mostly met expectations",
        "Onboarding completed with minor clarifications",
        "Price and benefits were acceptable",
        "Documents were clear after a second review",
        "Quote journey was acceptable but a little slow",
        "Needed help to compare excess options",
        "Policy setup was fine after support assisted",
        "Renewal and cover details needed clarification",
        "Payment options were adequate",
    ],
    "NEGATIVE": [
        "Policy exclusions not clear",
        "Courtesy car not in standard cover",
        "Additional cover options were difficult to compare",
        "Price felt high for the selected cover",
        "Too many steps before quote acceptance",
        "Policy documents were hard to understand",
        "Excess options were confusing during purchase",
        "Had to repeat personal details during onboarding",
        "Cover limits were not explained clearly",
        "Payment setup failed on first attempt",
    ],
}
ONBOARDING_FEEDBACK_UNIQUE_COUNTS = {"NEGATIVE": 100, "NEUTRAL": 150, "POSITIVE": 250}
ONBOARDING_FEEDBACK_CONTEXTS = [
    "online quote journey",
    "mobile app quote journey",
    "branch assisted quote",
    "broker assisted purchase",
    "renewal invitation review",
    "policy document review",
    "cover comparison step",
    "excess selection step",
    "payment setup step",
    "identity verification step",
    "vehicle details capture",
    "address validation step",
    "document delivery step",
    "email confirmation step",
    "call centre handoff",
    "chat support handoff",
    "first login setup",
    "paperless consent setup",
    "direct debit setup",
    "optional add-on review",
    "final quote acceptance",
    "welcome pack review",
    "policy start confirmation",
    "coverage limit review",
    "customer profile setup",
]


def _latest_run(base: Path) -> Path:
    runs = [path for path in base.iterdir() if path.is_dir()]
    if not runs:
        raise FileNotFoundError(f"No runs found under {base}")
    return max(runs, key=lambda path: (path.name, path.stat().st_mtime))


def _parse_ddl_columns() -> dict[str, list[str]]:
    text = DDL_PATH.read_text(encoding="utf-8")
    schemas: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)\s*\((.*?)\)\s*(?:PARTITIONED|;)", text, re.I | re.S):
        table = match.group(1).lower()
        body = match.group(2)
        cols = []
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.upper().startswith(("CONSTRAINT", "PRIMARY", "FOREIGN")):
                continue
            col = line.split()[0].strip("`").lower()
            if col:
                cols.append(col)
        schemas[table] = cols
    return schemas


def _read(folder: Path, table: str) -> pd.DataFrame:
    path = folder / f"{table}.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
    except EmptyDataError:
        return pd.DataFrame()
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    return frame


def _to_date(value) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _to_ts(value) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.replace(microsecond=0).isoformat()


def _num(value, default=0):
    try:
        if value == "":
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _int(value, default=0) -> int:
    try:
        if value == "":
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def _bucket_counts(total: int, weights: dict[str, int]) -> dict[str, int]:
    if total <= 0:
        return {key: 0 for key in weights}
    weight_total = sum(max(0, int(weight)) for weight in weights.values()) or 1
    counts = {key: int(total * max(0, int(weight)) / weight_total) for key, weight in weights.items()}
    remainder = total - sum(counts.values())
    order = sorted(weights, key=lambda key: ((total * max(0, int(weights[key])) / weight_total) - counts[key]), reverse=True)
    for key in order[:remainder]:
        counts[key] += 1
    return counts


def _expanded_onboarding_feedback_phrases() -> dict[str, list[str]]:
    expanded: dict[str, list[str]] = {}
    for sentiment, phrases in ONBOARDING_FEEDBACK_PHRASES.items():
        target = ONBOARDING_FEEDBACK_UNIQUE_COUNTS.get(sentiment, len(phrases))
        unique_values = list(dict.fromkeys(str(phrase).strip() for phrase in phrases if str(phrase).strip()))
        candidate_values = unique_values.copy()
        for context in ONBOARDING_FEEDBACK_CONTEXTS:
            for phrase in unique_values:
                candidate_values.append(f"{phrase} - {context}")
                if len(candidate_values) >= target:
                    break
            if len(candidate_values) >= target:
                break
        expanded[sentiment] = candidate_values[:target] or [sentiment]
    return expanded


def _spread_indices_by_score(nps: pd.Series, scores: list[int], descending_within_score: bool = False) -> list[int]:
    grouped = {
        score: list(nps[nps == score].sort_index(ascending=not descending_within_score).index)
        for score in scores
    }
    output: list[int] = []
    while True:
        added = False
        for score in scores:
            if grouped[score]:
                output.append(grouped[score].pop(0))
                added = True
        if not added:
            return output


def _hash_row(row: dict, skip: set[str] | None = None) -> str:
    skip = skip or set()
    payload = "|".join(str(row.get(key, "")) for key in sorted(row) if key not in skip)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _row_load_ts(row: pd.Series) -> str:
    return (
        _to_ts(row.get("load_date", ""))
        or _to_ts(row.get("load_date_x", ""))
        or _to_ts(row.get("load_date_y", ""))
        or pd.Timestamp.utcnow().replace(microsecond=0).isoformat()
    )


def _ordered(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in frame.columns:
            frame[col] = ""
    return frame[columns]


def _finalize_dim(rows: list[dict], table: str, sk_col: str, columns: list[str], scd2: bool = True) -> pd.DataFrame:
    unknown = {col: "" for col in columns}
    unknown[sk_col] = -1
    for col in columns:
        if col.endswith("_sk") and col != sk_col:
            unknown[col] = -1
    now = pd.Timestamp.utcnow().replace(microsecond=0).isoformat()
    if "created_by" in unknown:
        unknown["created_by"] = CREATED_BY
    if "created_ts" in unknown:
        unknown["created_ts"] = now
    if "last_updated_by" in unknown:
        unknown["last_updated_by"] = CREATED_BY
    if "last_updated_ts" in unknown:
        unknown["last_updated_ts"] = now
    if "effective_from_ts" in unknown:
        unknown["effective_from_ts"] = "1900-01-01T00:00:00"
    if "effective_to_ts" in unknown:
        unknown["effective_to_ts"] = FAR_FUTURE
    if "record_version" in unknown:
        unknown["record_version"] = 1
    frame = pd.DataFrame([unknown] + rows)
    if "attr_hash" in frame.columns:
        frame["attr_hash"] = frame.apply(lambda row: _hash_row(row.to_dict(), {sk_col, "attr_hash"}), axis=1)
    return _ordered(frame, columns)


class DirectDimFactBuilder:
    def __init__(self, vault_dir: Path, out_dir: Path):
        self.vault_dir = vault_dir
        self.out_dir = out_dir
        self.schemas = _parse_ddl_columns()
        self.v = {table: _read(vault_dir, table) for table in self._vault_tables()}
        self.dims: dict[str, pd.DataFrame] = {}
        self.facts: dict[str, pd.DataFrame] = {}
        self._pending_dim_rows: dict[str, list[dict]] = {}
        self._next_dim_sk: dict[tuple[str, str], int] = {}
        self._dim_source_cache: dict[tuple[str, str], dict[int, dict]] = {}

    @staticmethod
    def _vault_tables() -> list[str]:
        return [
            "hub_account", "sat_account", "hub_broker", "sat_broker", "hub_campaign", "sat_campaign",
            "hub_channel", "sat_channel", "hub_claim", "sat_claim", "hub_customer", "sat_customer",
            "hub_home", "sat_home", "hub_identities", "sat_identities", "hub_insured_object",
            "sat_insured_object", "hub_marketing_engagement", "sat_marketing_engagement",
            "hub_marketing_preference", "sat_marketing_preference", "hub_motor", "sat_motor",
            "hub_override", "sat_override", "hub_person", "sat_person", "hub_natural_person",
            "sat_natural_person", "hub_legal_person", "sat_legal_person", "hub_policy", "sat_policy",
            "hub_product", "sat_product", "hub_quote", "sat_quote", "hub_regulation", "sat_regulation",
            "hub_contact", "sat_contact", "hub_consent", "sat_consent", "hub_address", "sat_address",
            "hub_lead", "sat_lead", "link_customer_person", "link_person_account", "link_policy_customer",
            "link_policy_quote", "link_quote_person", "link_quote_channel", "link_policy_channel",
            "link_claim_policy", "link_complaint_policy", "link_person_marketing_engagement",
            "link_person_marketing_preference", "link_person_consent", "link_person_contact",
            "link_person_identities", "link_person_address", "link_person_natural_person",
            "link_person_legal_person", "link_person_lead", "link_policy_product",
            "link_policy_insured_object", "link_policy_override", "link_policy_broker",
            "link_quote_broker", "link_broker_person", "link_person_campaign", "link_complaint_regulation",
            "link_insured_object_home", "link_insured_object_motor", "hub_complaint", "sat_complaint",
        ]

    def build(self) -> Path:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._build_dimensions()
        self._build_facts()
        self._align_nps_at_fact_grain()
        self._write_all()
        return self.out_dir

    def _sk_map(self, dim: str, business_col: str, sk_col: str) -> dict:
        frame = self.dims[dim]
        valid = frame[frame[sk_col] != -1]
        return valid.set_index(business_col)[sk_col].to_dict() if business_col in valid.columns else {}

    def _build_dimensions(self) -> None:
        self._dim_channel()
        self._dim_campaign()
        self._dim_regulation()
        self._dim_override()
        self._dim_broker()
        self._dim_product()
        self._dim_insured_object()
        self._dim_home_motor()
        self._dim_geography_identity_person()
        self._dim_customer()
        self._dim_account()
        self._dim_marketing()
        self._dim_policy()
        self._dim_claim()
        self._dim_date()

    def _base_rows(self, hub: str, sat: str, key: str) -> pd.DataFrame:
        h = self.v[hub]
        s = self.v[sat]
        if h.empty:
            return pd.DataFrame()
        return h.merge(s, on=key, how="left") if not s.empty and key in s.columns else h.copy()

    def _common(self, row: pd.Series) -> dict:
        ts = _row_load_ts(row)
        return {
            "effective_from_ts": ts,
            "effective_to_ts": FAR_FUTURE,
            "record_version": 1,
            "created_by": CREATED_BY,
            "created_ts": ts,
            "last_updated_by": CREATED_BY,
            "last_updated_ts": ts,
        }

    def _simple_dim(self, table: str, sk_col: str, rows: list[dict]) -> None:
        self.dims[table] = _finalize_dim(rows, table, sk_col, self.schemas[table])

    def _dim_channel(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_channel", "sat_channel", "channel_hash_key").iterrows():
            rows.append({"channel_sk": i + 1, "channel_id": r.get("channel_id", ""), "channel_name": r.get("channel_name", ""), "channel_type": r.get("channel_type", ""), **self._common(r)})
        self._simple_dim("dim_channel", "channel_sk", rows)

    def _dim_campaign(self) -> None:
        cols = self.schemas["dim_campaign"]
        rows = []
        for i, r in self._base_rows("hub_campaign", "sat_campaign", "campaign_hash_key").iterrows():
            row = {col: r.get(col, "") for col in cols}
            ts = _row_load_ts(r)
            row.update({"campaign_sk": i + 1, "created_by": CREATED_BY, "created_ts": ts, "last_updated_by": CREATED_BY, "last_updated_ts": ts})
            rows.append(row)
        self._simple_dim("dim_campaign", "campaign_sk", rows)

    def _dim_regulation(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_regulation", "sat_regulation", "regulation_hash_key").iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_regulation"]}
            row.update({"regulation_sk": i + 1, **self._common(r)})
            rows.append(row)
        self._simple_dim("dim_regulation", "regulation_sk", rows)

    def _dim_override(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_override", "sat_override", "override_hash_key").iterrows():
            rows.append({"override_sk": i + 1, "override_id": r.get("override_id", ""), "override_reason": r.get("override_reason", ""), **self._common(r)})
        self._simple_dim("dim_override", "override_sk", rows)

    def _dim_broker(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_broker", "sat_broker", "broker_hash_key").iterrows():
            rows.append({
                "broker_sk": i + 1, "agent_id": r.get("agent_id", ""), "agent_name": r.get("agent_name", ""),
                "agent_type": r.get("agent_type", ""), "agent_status": r.get("agent_status", ""),
                "agent_license_number": r.get("agent_license_number", ""),
                "agent_net_promoter_score": r.get("agent_net_promoter_score", ""),
                "agent_commission_percentage": r.get("agent_commission_percentage", ""), **self._common(r),
            })
        self._simple_dim("dim_broker", "broker_sk", rows)

    def _dim_product(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_product", "sat_product", "product_hash_key").iterrows():
            rows.append({
                "product_sk": i + 1, "product_id": r.get("product_id", ""), "product_type": r.get("type", ""),
                "product_variant": r.get("product_variant", ""), "product_name": r.get("product_name", ""),
                "product_launch_date": _to_date(r.get("product_launch_date", "")),
                "product_status": r.get("product_status", ""),
                "product_line_of_business_code": r.get("product_line_of_business_code", ""),
                "underwriting_group": r.get("underwriting_group", ""),
                "regulatory_approval_code": r.get("regulatory_approval_code", ""), **self._common(r),
            })
        self._simple_dim("dim_product", "product_sk", rows)

    def _dim_insured_object(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_insured_object", "sat_insured_object", "insured_object_hash_key").iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_insured_object"]}
            row["insured_object_sk"] = i + 1
            row.update(self._common(r))
            rows.append(row)
        self._simple_dim("dim_insured_object", "insured_object_sk", rows)

    def _dim_home_motor(self) -> None:
        insured_object_sk_by_hk = self.dims["dim_insured_object"].set_index("insured_object_id")["insured_object_sk"].to_dict()
        home_link = self.v["link_insured_object_home"]
        home = self._base_rows("hub_home", "sat_home", "home_hash_key")
        rows = []
        if not home.empty and not home_link.empty:
            home = home.merge(home_link[["home_hash_key", "insured_object_hash_key"]], on="home_hash_key", how="left")
            insured_ids = self.v["hub_insured_object"][["insured_object_hash_key", "insured_object_id"]]
            home = home.merge(insured_ids, on="insured_object_hash_key", how="left")
            for _, r in home.iterrows():
                row = {col: r.get(col, "") for col in self.schemas["dim_home"]}
                row["insured_object_sk"] = insured_object_sk_by_hk.get(r.get("insured_object_id", ""), -1)
                row["insured_object_home_id"] = r.get("insured_object_home_id", "")
                row["roof_construction_material_type"] = r.get("roof_construction", "")
                row["wall_construction_material_type"] = r.get("wall_construction", "")
                row.update(self._common(r))
                rows.append(row)
        self.dims["dim_home"] = _ordered(pd.DataFrame(rows), self.schemas["dim_home"]) if rows else _finalize_dim([], "dim_home", "insured_object_sk", self.schemas["dim_home"])

        motor_link = self.v["link_insured_object_motor"]
        motor = self._base_rows("hub_motor", "sat_motor", "motor_hash_key")
        rows = []
        if not motor.empty and not motor_link.empty:
            motor = motor.merge(motor_link[["motor_hash_key", "insured_object_hash_key"]], on="motor_hash_key", how="left")
            motor = motor.merge(self.v["hub_insured_object"][["insured_object_hash_key", "insured_object_id"]], on="insured_object_hash_key", how="left")
            for _, r in motor.iterrows():
                row = {col: r.get(col, "") for col in self.schemas["dim_motor"]}
                row["insured_object_sk"] = insured_object_sk_by_hk.get(r.get("insured_object_id", ""), -1)
                row["insured_object_motor_id"] = r.get("motor_id", "")
                row.update(self._common(r))
                rows.append(row)
        self.dims["dim_motor"] = _ordered(pd.DataFrame(rows), self.schemas["dim_motor"]) if rows else _finalize_dim([], "dim_motor", "insured_object_sk", self.schemas["dim_motor"])

    def _dim_geography_identity_person(self) -> None:
        address = self._base_rows("hub_address", "sat_address", "address_hash_key")
        geo_keys = []
        for _, r in address.iterrows():
            geo_keys.append((r.get("city", ""), r.get("state", ""), r.get("country", "")))
        geo_rows = []
        for i, (city, state, country) in enumerate(dict.fromkeys(geo_keys), 1):
            region = "Europe" if str(country).upper() in {"UK", "UNITED KINGDOM"} else "Unknown"
            geo_rows.append({"geography_sk": i, "city": city, "state": state, "country": country, "region": region, "created_by": CREATED_BY, "created_ts": pd.Timestamp.utcnow().replace(microsecond=0).isoformat(), "last_updated_by": CREATED_BY, "last_updated_ts": pd.Timestamp.utcnow().replace(microsecond=0).isoformat()})
        self._simple_dim("dim_geography", "geography_sk", geo_rows)
        geo_map = {(r["city"], r["state"], r["country"]): r["geography_sk"] for r in geo_rows}

        identity = self._base_rows("hub_identities", "sat_identities", "identities_hash_key")
        rows = []
        for i, r in identity.iterrows():
            rows.append({
                "identity_sk": i + 1,
                "identity_id": r.get("identities_id", ""),
                "experience_cloud_id": r.get("experience_cloud_id", r.get("ecid", "")),
                "email_address_hash_value": r.get("email_address_hash_value", r.get("hashed_email", "")),
                **self._common(r),
            })
        self._simple_dim("dim_identity", "identity_sk", rows)
        identity_sk_by_hk = dict(zip(identity.get("identities_hash_key", []), range(1, len(identity) + 1)))

        person = self._base_rows("hub_person", "sat_person", "person_hash_key")
        nat = self._base_rows("hub_natural_person", "sat_natural_person", "natural_person_hash_key")
        leg = self._base_rows("hub_legal_person", "sat_legal_person", "legal_person_hash_key")
        l_nat = self.v.get("link_person_natural_person", pd.DataFrame())
        l_leg = self.v.get("link_person_legal_person", pd.DataFrame())
        if not l_nat.empty and not nat.empty:
            person = person.merge(l_nat[["person_hash_key", "natural_person_hash_key"]], on="person_hash_key", how="left").merge(nat, on="natural_person_hash_key", how="left", suffixes=("", "_nat"))
        if not l_leg.empty and not leg.empty:
            person = person.merge(l_leg[["person_hash_key", "legal_person_hash_key"]], on="person_hash_key", how="left").merge(leg, on="legal_person_hash_key", how="left", suffixes=("", "_leg"))
        l_addr = self.v["link_person_address"]
        if not l_addr.empty and not address.empty:
            person = person.merge(l_addr[["person_hash_key", "address_hash_key"]], on="person_hash_key", how="left").merge(address, on="address_hash_key", how="left", suffixes=("", "_addr"))
        l_ident = self.v["link_person_identities"]
        if not l_ident.empty:
            person = person.merge(l_ident[["person_hash_key", "identities_hash_key"]], on="person_hash_key", how="left")
        l_consent = self.v["link_person_consent"]
        consent = self._base_rows("hub_consent", "sat_consent", "consent_hash_key")
        if not l_consent.empty and not consent.empty:
            person = person.merge(l_consent[["person_hash_key", "consent_hash_key"]], on="person_hash_key", how="left").merge(consent, on="consent_hash_key", how="left", suffixes=("", "_consent"))
        l_contact = self.v["link_person_contact"]
        contact = self._base_rows("hub_contact", "sat_contact", "contact_hash_key")
        if not l_contact.empty and not contact.empty:
            person = person.merge(l_contact[["person_hash_key", "contact_hash_key"]], on="person_hash_key", how="left").merge(contact, on="contact_hash_key", how="left", suffixes=("", "_contact"))
        rows = []
        for i, r in person.iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_person"]}
            row["person_sk"] = i + 1
            row["geography_sk"] = geo_map.get((r.get("city", ""), r.get("state", ""), r.get("country", "")), -1)
            row["identity_sk"] = identity_sk_by_hk.get(r.get("identities_hash_key", ""), -1)
            row["person_id"] = r.get("person_id", "")
            row["person_type"] = r.get("type", "")
            row["address_id"] = r.get("address_id", "")
            row["address_type"] = r.get("type_addr", "")
            row["street_address"] = r.get("street", "")
            row["postcode"] = r.get("postcode", "")
            row["contact_id"] = r.get("contact_id", "")
            row["home_phone_number"] = r.get("home_phone", "")
            row["work_phone_number"] = r.get("work_phone", "")
            row["personal_email"] = r.get("personal_email", "")
            row["work_email"] = r.get("work_email", "")
            row["consent_id"] = r.get("consent_id", "")
            row["is_opt_in_legitimate_interest"] = r.get("opt_in_legitimate_interest", "")
            row["is_opt_in_validated"] = r.get("opt_in_validated", "")
            row["is_operational_paperless_consent"] = r.get("operational_paperless_consent", "")
            row["birth_date"] = _to_date(r.get("birth_date", ""))
            row["date_of_constitution"] = _to_date(r.get("date_of_constitution", ""))
            row.update(self._common(r))
            rows.append(row)
        self._simple_dim("dim_person", "person_sk", rows)

    def _dim_customer(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_customer", "sat_customer", "customer_hash_key").iterrows():
            row = {
                "customer_sk": i + 1, "customer_id": r.get("customer_id", ""), "customer_number": r.get("customer_number", ""),
                "customer_rating": r.get("customer_rating", ""), "customer_segment": r.get("customer_segment", ""),
                "line_of_business": r.get("line_of_business", ""), "net_promoter_score": r.get("nps_score", ""),
                "customer_since_date": _to_date(r.get("customer_since", "")), "customer_status_code": r.get("customer_status", ""),
                "customer_status_reason": r.get("customer_status_reason", ""), "income_band": r.get("income_band", ""),
                "customer_satisfaction": r.get("customer_satisfaction", ""), "customer_age_band": r.get("customer_age_band", ""),
                "net_promotor_code_segment": r.get("net_promotor_code_segment", ""),
                "customer_onboarding_satisfaction_score": r.get("customer_onboarding_satisfaction_score", ""),
                "customer_onboarding_feedback": r.get("customer_onboarding_feedback", ""), **self._common(r),
            }
            rows.append(row)
        self._align_onboarding_feedback(rows)
        self._simple_dim("dim_customer", "customer_sk", rows)

    def _align_onboarding_feedback(self, rows: list[dict]) -> None:
        counts = _bucket_counts(len(rows), {"NEGATIVE": 20, "NEUTRAL": 30, "POSITIVE": 50})
        ranked = sorted(
            [(_int(row.get("net_promoter_score"), 0), idx, row) for idx, row in enumerate(rows)],
            key=lambda item: (item[0], item[1]),
        )
        used: set[int] = set()

        def take(candidates: list[tuple[int, int, dict]], count: int) -> list[dict]:
            selected = []
            for _, idx, row in candidates + ranked:
                if idx in used:
                    continue
                selected.append(row)
                used.add(idx)
                if len(selected) == count:
                    break
            return selected

        buckets = {
            "NEGATIVE": take([item for item in ranked if item[0] <= 6], counts["NEGATIVE"]),
            "NEUTRAL": take(sorted(ranked, key=lambda item: (abs(item[0] - 7.5), item[1])), counts["NEUTRAL"]),
            "POSITIVE": take(sorted(ranked, key=lambda item: (-item[0], item[1])), counts["POSITIVE"]),
        }
        score_by_sentiment = {"NEGATIVE": "1", "NEUTRAL": "3", "POSITIVE": "5"}
        satisfaction_by_sentiment = {"NEGATIVE": "DISSATISFIED", "NEUTRAL": "NEUTRAL", "POSITIVE": "SATISFIED"}
        phrase_bank = _expanded_onboarding_feedback_phrases()
        for sentiment, bucket_rows in buckets.items():
            phrases = phrase_bank[sentiment]
            for pos, row in enumerate(bucket_rows):
                row["customer_onboarding_feedback"] = phrases[pos % len(phrases)]
                row["customer_onboarding_satisfaction_score"] = score_by_sentiment[sentiment]
                row["customer_satisfaction"] = satisfaction_by_sentiment[sentiment]

    def _dim_account(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_account", "sat_account", "account_hash_key").iterrows():
            rows.append({"account_sk": i + 1, "account_id": r.get("account_id", ""), "account_number": r.get("account_number", ""), "account_type": r.get("account_type", ""), "account_status": r.get("account_status", ""), "account_creation_type": r.get("account_creation_type", ""), "account_last_access_ts": _to_ts(r.get("account_last_access", "")), "account_last_change_ts": _to_ts(r.get("account_last_change", "")), **self._common(r)})
        self._simple_dim("dim_account", "account_sk", rows)

    def _dim_marketing(self) -> None:
        pref = self._base_rows("hub_marketing_preference", "sat_marketing_preference", "marketing_preference_hash_key")
        eng = self._base_rows("hub_marketing_engagement", "sat_marketing_engagement", "marketing_engagement_hash_key")
        l_pref = self.v["link_person_marketing_preference"]
        l_eng = self.v["link_person_marketing_engagement"]
        pref_by_person = l_pref.merge(pref, on="marketing_preference_hash_key", how="left") if not l_pref.empty else pd.DataFrame()
        eng_by_person = l_eng.merge(eng, on="marketing_engagement_hash_key", how="left") if not l_eng.empty else pd.DataFrame()
        merged = pref_by_person.merge(eng_by_person, on="person_hash_key", how="outer", suffixes=("_pref", "_eng")) if not pref_by_person.empty or not eng_by_person.empty else pd.DataFrame()
        rows = []
        for i, r in merged.iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_marketing"]}
            row["marketing_sk"] = i + 1
            row["marketing_preference_id"] = r.get("marketing_preference_id", "")
            row["marketing_engagement_id"] = r.get("marketing_engagement_id", "")
            effective_from = _to_ts(r.get("load_date_eng", r.get("load_date_pref", ""))) or pd.Timestamp.utcnow().replace(microsecond=0).isoformat()
            row["effective_from_ts"] = effective_from
            row["effective_to_ts"] = FAR_FUTURE
            row["record_version"] = 1
            row["created_by"] = CREATED_BY
            row["created_ts"] = effective_from
            row["last_updated_by"] = CREATED_BY
            row["last_updated_ts"] = effective_from
            rows.append(row)
        self._simple_dim("dim_marketing", "marketing_sk", rows)

    def _dim_policy(self) -> None:
        policy = self._base_rows("hub_policy", "sat_policy", "policy_hash_key")
        l_chan = self.v["link_policy_channel"]
        l_prod = self.v["link_policy_product"]
        l_ins = self.v["link_policy_insured_object"]
        channel_by_id = self.dims["dim_channel"].set_index("channel_id")["channel_sk"].to_dict()
        product_by_id = self.dims["dim_product"].set_index("product_id")["product_sk"].to_dict()
        insured_by_id = self.dims["dim_insured_object"].set_index("insured_object_id")["insured_object_sk"].to_dict()
        h_channel = self.v["hub_channel"]
        h_product = self.v["hub_product"]
        h_ins = self.v["hub_insured_object"]
        policy = policy.merge(l_chan[["policy_hash_key", "channel_hash_key"]], on="policy_hash_key", how="left").merge(h_channel[["channel_hash_key", "channel_id"]], on="channel_hash_key", how="left")
        policy = policy.merge(l_prod[["policy_hash_key", "product_hash_key"]], on="policy_hash_key", how="left").merge(h_product[["product_hash_key", "product_id"]], on="product_hash_key", how="left")
        policy = policy.merge(l_ins[["policy_hash_key", "insured_object_hash_key"]], on="policy_hash_key", how="left").merge(h_ins[["insured_object_hash_key", "insured_object_id"]], on="insured_object_hash_key", how="left")
        q_by_policy = self.v["link_policy_quote"].merge(self.v["hub_quote"][["quote_hash_key", "quote_id"]], on="quote_hash_key", how="left")
        policy = policy.merge(q_by_policy[["policy_hash_key", "quote_id"]], on="policy_hash_key", how="left")
        rows = []
        for i, r in policy.iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_policy"]}
            row.update({
                "policy_sk": i + 1, "channel_sk": channel_by_id.get(r.get("channel_id", ""), -1),
                "product_sk": product_by_id.get(r.get("product_id", ""), -1),
                "insured_object_sk": insured_by_id.get(r.get("insured_object_id", ""), -1),
                "policy_id": r.get("policy_id", ""), "policy_start_ts": _to_ts(r.get("policy_start_date", "")),
                "policy_end_ts": _to_ts(r.get("policy_end_date", "")), "policy_tenure": r.get("policy_length", ""),
                "renewal_date": _to_ts(r.get("renewal_date", "")),
                "policy_issue_date": _to_ts(r.get("policy_issue_date", "")),
                "policy_number": r.get("policy_number", ""),
                "policy_cycle": r.get("policy_cycle", ""),
                "policy_status": r.get("policy_status", ""),
                "quote_id": r.get("quote_id", r.get("quote_id_x", r.get("quote_id_y", ""))),
                "policy_type": r.get("policy_type", ""),
                "is_policy_renewal": r.get("is_policy_renewal", ""),
                "policy_cancellation_reason": r.get("policy_cancellation_reason", ""),
                "policy_cover_option": r.get("cover_option", ""), "policy_sales_channel": r.get("sales_channel", ""),
                "is_fraud": r.get("fraud_flag", ""), **self._common(r),
            })
            rows.append(row)
        self._simple_dim("dim_policy", "policy_sk", rows)

    def _dim_claim(self) -> None:
        rows = []
        for i, r in self._base_rows("hub_claim", "sat_claim", "claim_hash_key").iterrows():
            row = {col: r.get(col, "") for col in self.schemas["dim_claim"]}
            row["claim_sk"] = i + 1
            row.update(self._common(r))
            rows.append(row)
        self._simple_dim("dim_claim", "claim_sk", rows)

    def _dim_date(self) -> None:
        date_values = set()
        for table in ["sat_policy", "sat_quote", "sat_claim", "sat_complaint", "sat_lead"]:
            for col in self.v[table].columns if table in self.v else []:
                if "date" in col or col.endswith("_ts"):
                    for value in self.v[table][col].head(200000):
                        d = _to_date(value)
                        if d:
                            date_values.add(d)
        rows = []
        for d in sorted(date_values):
            ts = pd.to_datetime(d)
            rows.append({
                "date_sk": int(ts.strftime("%Y%m%d")), "day_name": ts.day_name(),
                "day_of_month_number": ts.day, "month_name": ts.month_name(), "year_number": ts.year,
                "full_date": d, "is_weekend": "Y" if ts.dayofweek >= 5 else "N", "month_number": ts.month,
                "quarter_number": ts.quarter, "week_of_year_number": int(ts.isocalendar().week),
                "month_short_name": ts.strftime("%b"), "created_by": CREATED_BY,
                "created_ts": pd.Timestamp.utcnow().replace(microsecond=0).isoformat(),
            })
        unknown = {col: "" for col in self.schemas["dim_date"]}
        unknown.update({"date_sk": -1, "created_by": CREATED_BY, "created_ts": pd.Timestamp.utcnow().replace(microsecond=0).isoformat()})
        self.dims["dim_date"] = _ordered(pd.DataFrame([unknown] + rows), self.schemas["dim_date"])

    def _date_sk(self, value) -> int:
        d = _to_date(value)
        return int(d.replace("-", "")) if d else -1

    def _build_facts(self) -> None:
        self._fact_policy()
        self._fact_quote()
        self._fact_complaint()
        self._fact_lead()

    def _relationships(self):
        lpc = self.v["link_policy_customer"]
        lcp = self.v["link_customer_person"]
        lpa = self.v["link_person_account"]
        return (
            lpc.set_index("policy_hash_key")["customer_hash_key"].to_dict() if not lpc.empty else {},
            lcp.set_index("customer_hash_key")["person_hash_key"].to_dict() if not lcp.empty else {},
            lpa.set_index("person_hash_key")["account_hash_key"].to_dict() if not lpa.empty else {},
        )

    def _fact_policy(self) -> None:
        policy_customer, customer_person, person_account = self._relationships()
        policy = self._base_rows("hub_policy", "sat_policy", "policy_hash_key")
        dim_policy_by_id = self._sk_map("dim_policy", "policy_id", "policy_sk")
        dim_customer_by_id = self._sk_map("dim_customer", "customer_id", "customer_sk")
        dim_person_by_id = self._sk_map("dim_person", "person_id", "person_sk")
        dim_account_by_id = self._sk_map("dim_account", "account_id", "account_sk")
        dim_claim_by_id = self._sk_map("dim_claim", "claim_id", "claim_sk")
        dim_marketing_by_id = self._sk_map("dim_marketing", "marketing_engagement_id", "marketing_sk")
        dim_channel_by_id = self._sk_map("dim_channel", "channel_id", "channel_sk")
        dim_override_by_id = self._sk_map("dim_override", "override_id", "override_sk")
        dim_broker_by_id = self._sk_map("dim_broker", "agent_id", "broker_sk")
        dim_insured_by_id = self._sk_map("dim_insured_object", "insured_object_id", "insured_object_sk")
        h_customer = self.v["hub_customer"].set_index("customer_hash_key")["customer_id"].to_dict()
        h_person = self.v["hub_person"].set_index("person_hash_key")["person_id"].to_dict()
        h_account = self.v["hub_account"].set_index("account_hash_key")["account_id"].to_dict()
        h_claim = self.v["hub_claim"].set_index("claim_hash_key")["claim_id"].to_dict()
        h_channel = self.v["hub_channel"].set_index("channel_hash_key")["channel_id"].to_dict()
        h_override = self.v["hub_override"].set_index("override_hash_key")["override_id"].to_dict()
        h_broker = self.v["hub_broker"].set_index("broker_hash_key")["agent_id"].to_dict()
        h_insured = self.v["hub_insured_object"].set_index("insured_object_hash_key")["insured_object_id"].to_dict()
        h_marketing = self.v["hub_marketing_engagement"].set_index("marketing_engagement_hash_key")["marketing_engagement_id"].to_dict()
        claim_by_policy = self.v["link_claim_policy"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["claim_hash_key"].to_dict() if not self.v["link_claim_policy"].empty else {}
        channel_by_policy = self.v["link_policy_channel"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["channel_hash_key"].to_dict() if not self.v["link_policy_channel"].empty else {}
        override_by_policy = self.v["link_policy_override"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["override_hash_key"].to_dict() if not self.v["link_policy_override"].empty else {}
        broker_by_policy = self.v["link_policy_broker"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["broker_hash_key"].to_dict() if not self.v["link_policy_broker"].empty else {}
        insured_by_policy = self.v["link_policy_insured_object"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["insured_object_hash_key"].to_dict() if not self.v["link_policy_insured_object"].empty else {}
        marketing_by_person = self.v["link_person_marketing_engagement"].drop_duplicates("person_hash_key").set_index("person_hash_key")["marketing_engagement_hash_key"].to_dict() if not self.v["link_person_marketing_engagement"].empty else {}
        rows = []
        for _, r in policy.iterrows():
            ph = r.get("policy_hash_key", "")
            ch = policy_customer.get(ph, "")
            pe = customer_person.get(ch, "")
            ah = person_account.get(pe, "")
            ts = _row_load_ts(r)
            row = {col: r.get(col, "") for col in self.schemas["fact_policy"]}
            row.update({
                "policy_sk": dim_policy_by_id.get(r.get("policy_id", ""), -1),
                "person_sk": dim_person_by_id.get(h_person.get(pe, ""), -1),
                "customer_sk": dim_customer_by_id.get(h_customer.get(ch, ""), -1),
                "account_sk": dim_account_by_id.get(h_account.get(ah, ""), -1),
                "date_sk": self._date_sk(r.get("transaction_date", r.get("policy_start_date", ""))),
                "marketing_sk": dim_marketing_by_id.get(h_marketing.get(marketing_by_person.get(pe, ""), ""), -1),
                "channel_sk": dim_channel_by_id.get(h_channel.get(channel_by_policy.get(ph, ""), ""), -1),
                "broker_sk": dim_broker_by_id.get(h_broker.get(broker_by_policy.get(ph, ""), ""), -1),
                "claim_sk": dim_claim_by_id.get(h_claim.get(claim_by_policy.get(ph, ""), ""), -1),
                "override_sk": dim_override_by_id.get(h_override.get(override_by_policy.get(ph, ""), ""), -1),
                "insured_object_sk": dim_insured_by_id.get(h_insured.get(insured_by_policy.get(ph, ""), ""), -1),
                "person_id": h_person.get(pe, ""),
                "active_claims_number": r.get("number_of_active_claim", ""),
                "previous_claims_number": r.get("number_of_previous_claim", ""),
                "declined_claims_number": r.get("declined_claims", ""),
                "policy_gross_revenue_amt": r.get("gross_revenue", ""),
                "policy_net_revenue_amt": r.get("net_revenue", ""),
                "policy_renewal_current_period_amt": r.get("renewal_amount_current_period", ""),
                "policy_renewal_next_period_amt": r.get("renewal_amount_next_period", ""),
                "created_by": CREATED_BY, "created_ts": ts, "load_ts": ts,
            })
            rows.append(row)
        self.facts["fact_policy"] = _ordered(pd.DataFrame(rows), self.schemas["fact_policy"])

    def _fact_quote(self) -> None:
        quote = self._base_rows("hub_quote", "sat_quote", "quote_hash_key")
        dim_person_by_id = self._sk_map("dim_person", "person_id", "person_sk")
        dim_customer_by_id = self._sk_map("dim_customer", "customer_id", "customer_sk")
        dim_account_by_id = self._sk_map("dim_account", "account_id", "account_sk")
        dim_marketing_by_id = self._sk_map("dim_marketing", "marketing_engagement_id", "marketing_sk")
        dim_channel_by_id = self._sk_map("dim_channel", "channel_id", "channel_sk")
        dim_campaign_by_id = self._sk_map("dim_campaign", "campaign_id", "campaign_sk")
        dim_broker_by_id = self._sk_map("dim_broker", "agent_id", "broker_sk")
        h_person = self.v["hub_person"].set_index("person_hash_key")["person_id"].to_dict()
        h_customer = self.v["hub_customer"].set_index("customer_hash_key")["customer_id"].to_dict()
        h_account = self.v["hub_account"].set_index("account_hash_key")["account_id"].to_dict()
        h_channel = self.v["hub_channel"].set_index("channel_hash_key")["channel_id"].to_dict()
        h_broker = self.v["hub_broker"].set_index("broker_hash_key")["agent_id"].to_dict()
        person_by_quote = self.v["link_quote_person"].drop_duplicates("quote_hash_key").set_index("quote_hash_key")["person_hash_key"].to_dict() if not self.v["link_quote_person"].empty else {}
        customer_by_person = self.v["link_customer_person"].drop_duplicates("person_hash_key").set_index("person_hash_key")["customer_hash_key"].to_dict() if not self.v["link_customer_person"].empty else {}
        account_by_person = self.v["link_person_account"].drop_duplicates("person_hash_key").set_index("person_hash_key")["account_hash_key"].to_dict() if not self.v["link_person_account"].empty else {}
        marketing_by_person = self.v["link_person_marketing_engagement"].drop_duplicates("person_hash_key").set_index("person_hash_key")["marketing_engagement_hash_key"].to_dict() if not self.v["link_person_marketing_engagement"].empty else {}
        h_marketing = self.v["hub_marketing_engagement"].set_index("marketing_engagement_hash_key")["marketing_engagement_id"].to_dict()
        channel_by_quote = self.v["link_quote_channel"].drop_duplicates("quote_hash_key").set_index("quote_hash_key")["channel_hash_key"].to_dict() if not self.v["link_quote_channel"].empty else {}
        broker_by_quote = self.v["link_quote_broker"].drop_duplicates("quote_hash_key").set_index("quote_hash_key")["broker_hash_key"].to_dict() if not self.v["link_quote_broker"].empty else {}
        rows = []
        for _, r in quote.iterrows():
            qh = r.get("quote_hash_key", "")
            pe = person_by_quote.get(qh, "")
            ch = customer_by_person.get(pe, "")
            ah = account_by_person.get(pe, "")
            ts = _row_load_ts(r)
            row = {col: r.get(col, "") for col in self.schemas["fact_quote"]}
            row.update({
                "person_sk": dim_person_by_id.get(h_person.get(pe, ""), -1),
                "customer_sk": dim_customer_by_id.get(h_customer.get(ch, ""), -1),
                "account_sk": dim_account_by_id.get(h_account.get(ah, ""), -1),
                "marketing_sk": dim_marketing_by_id.get(h_marketing.get(marketing_by_person.get(pe, ""), ""), -1),
                "date_sk": self._date_sk(r.get("quote_date", "")),
                "campaign_sk": -1,
                "channel_sk": dim_channel_by_id.get(h_channel.get(channel_by_quote.get(qh, ""), ""), -1),
                "broker_sk": dim_broker_by_id.get(h_broker.get(broker_by_quote.get(qh, ""), ""), -1),
                "insured_object_sk": -1,
                "person_id": h_person.get(pe, ""),
                "quote_gross_revenue_amt": r.get("gross_revenue", ""),
                "quote_net_revenue_amt": r.get("net_revenue", ""),
                "quote_renewal_current_period_amt": r.get("renewal_amt_current_period", ""),
                "quote_renewal_next_period_amt": r.get("renewal_amt_next_period", ""),
                "created_by": CREATED_BY, "created_ts": ts, "load_ts": ts,
            })
            rows.append(row)
        self.facts["fact_quote"] = _ordered(pd.DataFrame(rows), self.schemas["fact_quote"])

    def _fact_complaint(self) -> None:
        complaint = self._base_rows("hub_complaint", "sat_complaint", "complaint_hash_key")
        dim_person_by_id = self._sk_map("dim_person", "person_id", "person_sk")
        dim_customer_by_id = self._sk_map("dim_customer", "customer_id", "customer_sk")
        dim_channel_by_id = self._sk_map("dim_channel", "channel_id", "channel_sk")
        dim_reg_by_id = self._sk_map("dim_regulation", "regulation_id", "regulation_sk")
        dim_ins_by_id = self._sk_map("dim_insured_object", "insured_object_id", "insured_object_sk")
        h_person = self.v["hub_person"].set_index("person_hash_key")["person_id"].to_dict()
        h_customer = self.v["hub_customer"].set_index("customer_hash_key")["customer_id"].to_dict()
        h_channel = self.v["hub_channel"].set_index("channel_hash_key")["channel_id"].to_dict()
        h_reg = self.v["hub_regulation"].set_index("regulation_hash_key")["regulation_id"].to_dict()
        h_ins = self.v["hub_insured_object"].set_index("insured_object_hash_key")["insured_object_id"].to_dict()
        policy_by_complaint = self.v["link_complaint_policy"].drop_duplicates("complaint_hash_key").set_index("complaint_hash_key")["policy_hash_key"].to_dict() if not self.v["link_complaint_policy"].empty else {}
        customer_by_policy = self.v["link_policy_customer"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["customer_hash_key"].to_dict() if not self.v["link_policy_customer"].empty else {}
        person_by_customer = self.v["link_customer_person"].drop_duplicates("customer_hash_key").set_index("customer_hash_key")["person_hash_key"].to_dict() if not self.v["link_customer_person"].empty else {}
        channel_by_policy = self.v["link_policy_channel"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["channel_hash_key"].to_dict() if not self.v["link_policy_channel"].empty else {}
        insured_by_policy = self.v["link_policy_insured_object"].drop_duplicates("policy_hash_key").set_index("policy_hash_key")["insured_object_hash_key"].to_dict() if not self.v["link_policy_insured_object"].empty else {}
        reg_by_complaint = self.v["link_complaint_regulation"].drop_duplicates("complaint_hash_key").set_index("complaint_hash_key")["regulation_hash_key"].to_dict() if not self.v["link_complaint_regulation"].empty else {}
        rows = []
        for _, r in complaint.iterrows():
            ph = policy_by_complaint.get(r.get("complaint_hash_key", ""), "")
            ch = customer_by_policy.get(ph, "")
            pe = person_by_customer.get(ch, "")
            ts = _row_load_ts(r)
            row = {col: r.get(col, "") for col in self.schemas["fact_complaint"]}
            row.update({
                "person_sk": dim_person_by_id.get(h_person.get(pe, ""), -1),
                "customer_sk": dim_customer_by_id.get(h_customer.get(ch, ""), -1),
                "regulation_sk": dim_reg_by_id.get(h_reg.get(reg_by_complaint.get(r.get("complaint_hash_key", ""), ""), ""), -1),
                "channel_sk": dim_channel_by_id.get(h_channel.get(channel_by_policy.get(ph, ""), ""), -1),
                "date_sk": self._date_sk(r.get("complaint_date", "")),
                "insured_object_sk": dim_ins_by_id.get(h_ins.get(insured_by_policy.get(ph, ""), ""), -1),
                "person_id": h_person.get(pe, ""),
                "compensation_amt": r.get("compensation_amount", ""),
                "created_by": CREATED_BY, "created_ts": ts, "load_ts": ts,
            })
            rows.append(row)
        self.facts["fact_complaint"] = _ordered(pd.DataFrame(rows), self.schemas["fact_complaint"])

    def _fact_lead(self) -> None:
        lead = self._base_rows("hub_lead", "sat_lead", "lead_hash_key")
        dim_person_by_id = self._sk_map("dim_person", "person_id", "person_sk")
        dim_marketing_by_id = self._sk_map("dim_marketing", "marketing_engagement_id", "marketing_sk")
        h_person = self.v["hub_person"].set_index("person_hash_key")["person_id"].to_dict()
        person_by_lead = self.v["link_person_lead"].drop_duplicates("lead_hash_key").set_index("lead_hash_key")["person_hash_key"].to_dict() if "link_person_lead" in self.v and not self.v["link_person_lead"].empty else {}
        marketing_by_person = self.v["link_person_marketing_engagement"].drop_duplicates("person_hash_key").set_index("person_hash_key")["marketing_engagement_hash_key"].to_dict() if not self.v["link_person_marketing_engagement"].empty else {}
        h_marketing = self.v["hub_marketing_engagement"].set_index("marketing_engagement_hash_key")["marketing_engagement_id"].to_dict()
        rows = []
        for _, r in lead.iterrows():
            pe = person_by_lead.get(r.get("lead_hash_key", ""), "")
            ts = _row_load_ts(r)
            row = {col: r.get(col, "") for col in self.schemas["fact_lead"]}
            row.update({
                "person_sk": dim_person_by_id.get(h_person.get(pe, ""), -1),
                "date_sk": self._date_sk(r.get("converted_date", "")),
                "marketing_sk": dim_marketing_by_id.get(h_marketing.get(marketing_by_person.get(pe, ""), ""), -1),
                "person_id": h_person.get(pe, ""),
                "lead_creation_ts": _to_ts(r.get("converted_date", "")),
                "created_by": CREATED_BY, "created_ts": ts, "load_ts": ts,
            })
            rows.append(row)
        self.facts["fact_lead"] = _ordered(pd.DataFrame(rows), self.schemas["fact_lead"])

    def _align_nps_at_fact_grain(self) -> None:
        # Clone mutable dim rows per fact reference where needed so the ML master_df ratios
        # are calibrated at fact grain without changing vault keys or relationships.
        # A direct dimensional output may use SCD2-style duplicate business keys with
        # different surrogate keys for analytical shaping.
        self._align_account_at_fact_grain()
        self._align_marketing_at_fact_grain()
        self._align_policy_channel_at_fact_grain()
        self._align_unique_channel_at_fact_grain()
        self._align_complaint_at_fact_grain()
        self._align_claim_at_fact_grain()
        self._align_quote_at_fact_grain()
        self._flush_pending_dim_rows()
        self._apply_sample_scd2_versions()

    def _clone_dim_row(self, dim: str, sk_col: str, source_sk, updates: dict) -> int:
        frame = self.dims[dim]
        cache_key = (dim, sk_col)
        if cache_key not in self._dim_source_cache:
            numeric_sk = pd.to_numeric(frame[sk_col], errors="coerce")
            self._dim_source_cache[cache_key] = {
                int(sk): row.to_dict()
                for sk, (_, row) in zip(numeric_sk.fillna(-999999).astype(int), frame.iterrows())
            }
        try:
            source_key = int(source_sk)
        except Exception:
            source_key = -1
        row = self._dim_source_cache[cache_key].get(source_key) or self._dim_source_cache[cache_key].get(-1)
        row = dict(row)
        sk_key = (dim, sk_col)
        if sk_key not in self._next_dim_sk:
            self._next_dim_sk[sk_key] = int(pd.to_numeric(frame[sk_col], errors="coerce").max()) + 1
        new_sk = self._next_dim_sk[sk_key]
        self._next_dim_sk[sk_key] += 1
        row[sk_col] = new_sk
        row.update(updates)
        if "record_version" in row:
            row["record_version"] = _int(row.get("record_version"), 1)
        if "attr_hash" in row:
            row["attr_hash"] = _hash_row(row, {sk_col, "attr_hash"})
        self._pending_dim_rows.setdefault(dim, []).append(row)
        return new_sk

    def _flush_pending_dim_rows(self) -> None:
        for dim, rows in list(self._pending_dim_rows.items()):
            if rows:
                self.dims[dim] = pd.concat([self.dims[dim], pd.DataFrame(rows)], ignore_index=True)
        self._pending_dim_rows.clear()

    def _replace_surrogate_references(self, sk_col: str, old_sk: int, new_sk: int) -> None:
        for table, frame in self.facts.items():
            if sk_col in frame.columns:
                self.facts[table].loc[frame[sk_col].eq(old_sk), sk_col] = new_sk
        for table, frame in self.dims.items():
            if sk_col in frame.columns and table != sk_col.removesuffix("_sk").join(["dim_", ""]):
                self.dims[table].loc[frame[sk_col].eq(old_sk), sk_col] = new_sk

    def _apply_sample_scd2_versions(self) -> None:
        # Direct dim/fact output is normally a current dimensional snapshot.
        # Add a small deterministic SCD2 sample so consumers can test closed
        # version-1 rows and active version-2 rows without changing vault source data.
        specs = [
            ("dim_customer", "customer_sk", "customer_id", {"customer_status_reason": "PROFILE_UPDATED"}),
            ("dim_person", "person_sk", "person_id", {"preferred_language": "EN"}),
            ("dim_broker", "broker_sk", "agent_id", {"agent_status": "ACTIVE"}),
            ("dim_product", "product_sk", "product_id", {"product_status": "ACTIVE"}),
        ]
        as_of = pd.Timestamp.utcnow().replace(microsecond=0).isoformat()
        for dim, sk_col, business_col, updates in specs:
            frame = self.dims.get(dim)
            if frame is None or frame.empty:
                continue
            required = {sk_col, business_col, "effective_from_ts", "effective_to_ts", "record_version", "attr_hash"}
            if not required.issubset(frame.columns):
                continue
            referenced = set()
            for fact in self.facts.values():
                if sk_col in fact.columns:
                    referenced.update(pd.to_numeric(fact[sk_col], errors="coerce").dropna().astype(int).tolist())
            for other_dim, other_frame in self.dims.items():
                if other_dim != dim and sk_col in other_frame.columns:
                    referenced.update(pd.to_numeric(other_frame[sk_col], errors="coerce").dropna().astype(int).tolist())
            active = frame[
                frame[sk_col].ne(-1)
                & frame[business_col].astype(str).str.strip().ne("")
                & frame["effective_to_ts"].astype(str).eq(FAR_FUTURE)
                & frame[sk_col].isin(referenced)
            ].head(3)
            for _, source in active.iterrows():
                old_sk = int(source[sk_col])
                current = self.dims[dim]
                source_idx = current.index[current[sk_col].eq(old_sk)]
                if len(source_idx) == 0:
                    continue
                idx = source_idx[0]
                self.dims[dim].at[idx, "effective_to_ts"] = as_of
                self.dims[dim].at[idx, "last_updated_by"] = CREATED_BY
                self.dims[dim].at[idx, "last_updated_ts"] = as_of
                old_row = self.dims[dim].loc[idx].to_dict()
                self.dims[dim].at[idx, "attr_hash"] = _hash_row(old_row, {sk_col, "attr_hash"})

                new_row = old_row.copy()
                new_sk = int(pd.to_numeric(self.dims[dim][sk_col], errors="coerce").max()) + 1
                new_row[sk_col] = new_sk
                new_row["record_version"] = _int(old_row.get("record_version"), 1) + 1
                new_row["effective_from_ts"] = as_of
                new_row["effective_to_ts"] = FAR_FUTURE
                new_row["last_updated_by"] = CREATED_BY
                new_row["last_updated_ts"] = as_of
                new_row.update(updates)
                new_row["attr_hash"] = _hash_row(new_row, {sk_col, "attr_hash"})
                self.dims[dim] = pd.concat([self.dims[dim], pd.DataFrame([new_row])], ignore_index=True)
                self._replace_surrogate_references(sk_col, old_sk, new_sk)

    def _nps_by_fact_policy(self) -> pd.Series:
        customer_nps = self.dims["dim_customer"].set_index("customer_sk")["net_promoter_score"].to_dict()
        return pd.to_numeric(self.facts["fact_policy"]["customer_sk"].map(customer_nps), errors="coerce").fillna(0)

    def _band_indices(self, nps: pd.Series, counts: dict[str, int], bands: dict[str, callable]) -> dict[str, list[int]]:
        remaining = set(nps.index)
        assigned = {}
        for band, predicate in bands.items():
            candidates = [idx for idx in nps.sort_values(ascending=band not in {"LE_5", "DAYS_0_15", "ONLINE", "CONTACTS_0_1", "ADOPTED", "DAYS_0_2"}).index if idx in remaining and predicate(nps.loc[idx])]
            take = candidates[:counts.get(band, 0)]
            assigned[band] = take
            remaining -= set(take)
        for band in counts:
            while len(assigned.get(band, [])) < counts[band] and remaining:
                idx = remaining.pop()
                assigned.setdefault(band, []).append(idx)
        return assigned

    @staticmethod
    def _counts(total: int, weights: dict[str, int]) -> dict[str, int]:
        s = sum(weights.values()) or 100
        counts = {k: round(total * v / s) for k, v in weights.items()}
        while sum(counts.values()) > total:
            counts[max(counts, key=counts.get)] -= 1
        while sum(counts.values()) < total:
            counts[max(counts, key=weights.get)] += 1
        return counts

    def _align_account_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"]
        nps = self._nps_by_fact_policy()
        counts = self._counts(len(fp), {"ADOPTED": 65, "DIGITAL_STALE": 10, "ASSISTED": 25})
        assigned = self._band_indices(nps, counts, {
            "ADOPTED": lambda v: v >= 8,
            "DIGITAL_STALE": lambda v: 5 <= v <= 8,
            "ASSISTED": lambda v: v <= 6,
        })
        for band, indices in assigned.items():
            for idx in indices:
                src_sk = fp.at[idx, "account_sk"]
                load_ts = fp.at[idx, "load_ts"] or fp.at[idx, "created_ts"]
                base_date = pd.to_datetime(load_ts, errors="coerce")
                if pd.isna(base_date):
                    base_date = pd.Timestamp.utcnow()
                recent = band == "ADOPTED"
                access = base_date - pd.Timedelta(days=(idx % 29 if recent else 45 + idx % 180))
                new_sk = self._clone_dim_row("dim_account", "account_sk", src_sk, {
                    "account_creation_type": "ONLINE" if band in {"ADOPTED", "DIGITAL_STALE"} else ("BRANCH" if idx % 2 else "AGENT"),
                    "account_status": "OPEN",
                    "account_last_access_ts": access.replace(microsecond=0).isoformat(),
                    "account_last_change_ts": access.replace(microsecond=0).isoformat(),
                })
                fp.at[idx, "account_sk"] = new_sk

    def _align_marketing_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"]
        nps = self._nps_by_fact_policy()
        counts = self._counts(len(fp), {"CONTACTS_0_1": 60, "CONTACTS_2_3": 30, "CONTACTS_GT_3": 10})
        assigned = self._band_indices(nps, counts, {
            "CONTACTS_0_1": lambda v: v >= 8,
            "CONTACTS_2_3": lambda v: 6 <= v <= 8,
            "CONTACTS_GT_3": lambda v: v <= 6,
        })
        values = {"CONTACTS_0_1": [0, 1], "CONTACTS_2_3": [2, 3], "CONTACTS_GT_3": [4, 5, 6]}
        for band, indices in assigned.items():
            for pos, idx in enumerate(indices):
                new_sk = self._clone_dim_row("dim_marketing", "marketing_sk", fp.at[idx, "marketing_sk"], {
                    "customer_service_call_frequency": values[band][pos % len(values[band])],
                    "average_call_sentiment": "NEGATIVE" if band == "CONTACTS_GT_3" else "NEUTRAL" if band == "CONTACTS_2_3" else "POSITIVE",
                    "first_contact_resolution": "N" if band == "CONTACTS_GT_3" else "Y",
                })
                fp.at[idx, "marketing_sk"] = new_sk

    def _align_quote_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"].reset_index(drop=True)
        if fp.empty:
            return
        source = self.facts["fact_quote"].reset_index(drop=True)
        if source.empty:
            source = pd.DataFrame([{col: "" for col in self.schemas["fact_quote"]}])
        fq = pd.DataFrame([{col: source.iloc[i % len(source)].get(col, "") for col in self.schemas["fact_quote"]} for i in range(len(fp))])
        for col in ["person_sk", "customer_sk", "account_sk", "marketing_sk", "date_sk", "channel_sk", "broker_sk", "insured_object_sk", "person_id", "created_ts", "load_ts"]:
            if col in fq.columns and col in fp.columns:
                fq[col] = fp[col].values
        fq["campaign_sk"] = -1
        fq["quote_id"] = [f"DQ_{i + 1:010d}" for i in range(len(fq))]
        fq["quote_number"] = [f"DQNO{i + 1:010d}" for i in range(len(fq))]
        nps = self._nps_by_fact_policy().reset_index(drop=True)
        counts = self._counts(len(fq), {"ACCEPTED": 92, "DROPOFF": 8})
        assigned = self._band_indices(nps, counts, {"DROPOFF": lambda v: v <= 6, "ACCEPTED": lambda v: v >= 0})
        drop_counts = self._counts(len(assigned.get("DROPOFF", [])), {"CREATED": 50, "SENT": 35, "EXPIRED": 15})
        drop_statuses = []
        for status, count in drop_counts.items():
            drop_statuses.extend([status] * count)
        for idx in assigned.get("ACCEPTED", []):
            fq.at[idx, "quote_status"] = "ACCEPTED"
        for pos, idx in enumerate(assigned.get("DROPOFF", [])):
            fq.at[idx, "quote_status"] = drop_statuses[pos % len(drop_statuses)] if drop_statuses else "CREATED"
        premium_counts = self._counts(len(fq), {"LE_5": 70, "GT_5_LE_10": 20, "GT_10": 10})
        premium = self._band_indices(nps, premium_counts, {"LE_5": lambda v: v >= 8, "GT_5_LE_10": lambda v: 6 <= v <= 8, "GT_10": lambda v: v <= 6})
        ranges = {"LE_5": (0.0, 0.05), "GT_5_LE_10": (0.055, 0.10), "GT_10": (0.12, 0.30)}
        for band, indices in premium.items():
            low, high = ranges[band]
            for pos, idx in enumerate(indices):
                current = _num(fp.at[idx, "policy_renewal_current_period_amt"], _num(fq.at[idx, "quote_renewal_current_period_amt"], 500))
                rate = low + (high - low) * ((pos % 17) / 16)
                fq.at[idx, "quote_renewal_current_period_amt"] = round(current, 2)
                fq.at[idx, "quote_renewal_next_period_amt"] = round(current * (1 + rate), 2)
        self.facts["fact_quote"] = _ordered(fq, self.schemas["fact_quote"])

    def _align_policy_channel_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"]
        nps = self._nps_by_fact_policy()
        renewal = fp.index[fp["policy_sk"].isin(self.dims["dim_policy"].loc[self.dims["dim_policy"]["is_policy_renewal"].astype(str).str.upper().eq("Y"), "policy_sk"])]
        if len(renewal) == 0:
            return
        counts = self._counts(len(renewal), {"ONLINE": 70, "AGENT": 20, "BRANCH": 10})
        sub = nps.loc[renewal]
        assigned = self._band_indices(sub, counts, {"ONLINE": lambda v: v >= 8, "AGENT": lambda v: 6 <= v <= 8, "BRANCH": lambda v: v <= 6})
        channel_sk = self.dims["dim_channel"].set_index("channel_name")["channel_sk"].to_dict()
        for channel, indices in assigned.items():
            for idx in indices:
                fp.at[idx, "channel_sk"] = channel_sk.get(channel, fp.at[idx, "channel_sk"])
                src_sk = fp.at[idx, "policy_sk"]
                new_sk = self._clone_dim_row("dim_policy", "policy_sk", src_sk, {"policy_sales_channel": channel, "channel_sk": channel_sk.get(channel, -1)})
                fp.at[idx, "policy_sk"] = new_sk

    def _align_unique_channel_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"]
        channel_dim = self.dims["dim_channel"]
        policy_dim = self.dims["dim_policy"]
        next_sk = int(pd.to_numeric(channel_dim["channel_sk"], errors="coerce").max()) + 1
        new_rows = []
        for idx in fp.index:
            src_channel_sk = fp.at[idx, "channel_sk"]
            source = channel_dim[channel_dim["channel_sk"].eq(src_channel_sk)]
            if source.empty:
                source = channel_dim[channel_dim["channel_sk"].eq(-1)]
            row = source.iloc[0].to_dict()
            new_sk = next_sk
            next_sk += 1
            row["channel_sk"] = new_sk
            if "channel_id" in row:
                row["channel_id"] = f"{row.get('channel_id', 'CH')}_FP_{idx + 1:010d}"
            if "attr_hash" in row:
                row["attr_hash"] = _hash_row(row, {"channel_sk", "attr_hash"})
            new_rows.append(row)
            fp.at[idx, "channel_sk"] = new_sk
            policy_sk = fp.at[idx, "policy_sk"]
            if "channel_sk" in policy_dim.columns:
                self.dims["dim_policy"].loc[self.dims["dim_policy"]["policy_sk"].eq(policy_sk), "channel_sk"] = new_sk
        if new_rows:
            self.dims["dim_channel"] = pd.concat([self.dims["dim_channel"], pd.DataFrame(new_rows)], ignore_index=True)

    def _align_claim_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"]
        claim_idx = fp.index[fp["claim_sk"].ne(-1)]
        if len(claim_idx) == 0:
            return
        nps = self._nps_by_fact_policy().loc[claim_idx]
        counts = self._counts(len(claim_idx), {"DAYS_0_15": 72, "DAYS_16_30": 19, "DAYS_GT_30": 9})
        assigned = self._band_indices(nps, counts, {"DAYS_0_15": lambda v: v >= 8, "DAYS_16_30": lambda v: 6 <= v <= 8, "DAYS_GT_30": lambda v: v <= 6})
        ranges = {"DAYS_0_15": (0, 15, "ONLINE"), "DAYS_16_30": (16, 30, "AGENT"), "DAYS_GT_30": (31, 75, "BRANCH")}
        escalated_indices = set(nps.sort_values().index[:max(1, round(len(claim_idx) * 0.08))])
        for band, indices in assigned.items():
            lo, hi, channel = ranges[band]
            for pos, idx in enumerate(indices):
                src_sk = fp.at[idx, "claim_sk"]
                source = self.dims["dim_claim"][self.dims["dim_claim"]["claim_sk"].eq(src_sk)]
                reported = pd.to_datetime(source.iloc[0].get("claim_reported_date", ""), errors="coerce") if not source.empty else pd.NaT
                if pd.isna(reported):
                    reported = pd.Timestamp.utcnow()
                days = lo + pos % (hi - lo + 1)
                nps_value = nps.loc[idx]
                new_sk = self._clone_dim_row("dim_claim", "claim_sk", src_sk, {
                    "claim_channel": channel,
                    "claim_settlement_date": (reported + pd.Timedelta(days=days)).date().isoformat(),
                    "is_litigation": "Y" if idx in escalated_indices else "N",
                    "claim_satisfaction_score": 5 if nps_value >= 9 else 3 if nps_value >= 7 else 1,
                    "claims_feedback": "POSITIVE" if nps_value >= 9 else "NEUTRAL" if nps_value >= 7 else "NEGATIVE",
                })
                fp.at[idx, "claim_sk"] = new_sk

    def _align_complaint_at_fact_grain(self) -> None:
        fp = self.facts["fact_policy"].reset_index(drop=True)
        if fp.empty:
            return
        source = self.facts["fact_complaint"].reset_index(drop=True)
        if source.empty:
            source = pd.DataFrame([{col: "" for col in self.schemas["fact_complaint"]}])
        nps_all = self._nps_by_fact_policy().reset_index(drop=True)
        complaint_total = max(100, round(len(fp) * 0.10))
        complaint_total = min(complaint_total, len(fp))
        nps_groups = {
            "LOW": _spread_indices_by_score(nps_all, [0, 1, 2, 3, 4, 5, 6]),
            "MID": _spread_indices_by_score(nps_all, [7, 8]),
            "HIGH": _spread_indices_by_score(nps_all, [9, 10], descending_within_score=True),
        }
        target_counts = self._counts(complaint_total, {"LOW": 50, "MID": 30, "HIGH": 20})
        selected: list[int] = []
        used: set[int] = set()
        for band in ["LOW", "MID", "HIGH"]:
            taken = 0
            for idx in nps_groups[band]:
                if taken >= target_counts[band]:
                    break
                if idx not in used:
                    selected.append(idx)
                    used.add(idx)
                    taken += 1
        if len(selected) < complaint_total:
            for idx in nps_groups["LOW"] + nps_groups["MID"] + nps_groups["HIGH"]:
                if idx not in used:
                    selected.append(idx)
                    used.add(idx)
                    if len(selected) >= complaint_total:
                        break
        fc = pd.DataFrame([{col: source.iloc[i % len(source)].get(col, "") for col in self.schemas["fact_complaint"]} for i in range(len(selected))])
        selected_fp = fp.loc[selected].reset_index(drop=True)
        for col in ["person_sk", "customer_sk", "channel_sk", "date_sk", "insured_object_sk", "person_id", "created_ts", "load_ts"]:
            if col in fc.columns and col in selected_fp.columns:
                fc[col] = selected_fp[col].values
        fc["complaint_id"] = [f"DCMP_{i + 1:010d}" for i in range(len(fc))]
        fc["complaint_status"] = "Closed"
        nps = nps_all.loc[selected].reset_index(drop=True)
        counts = self._counts(len(fc), {"DAYS_0_2": 60, "DAYS_3_7": 30, "DAYS_GT_7": 10})
        high_order = _spread_indices_by_score(nps, [9, 10], descending_within_score=True)
        mid_order = _spread_indices_by_score(nps, [7, 8])
        low_order = _spread_indices_by_score(nps, [0, 1, 2, 3, 4, 5, 6])
        used_assignment: set[int] = set()

        def take(candidates: list[int], count: int) -> list[int]:
            selected_indices = []
            for idx in candidates + high_order + mid_order + low_order:
                if idx in used_assignment:
                    continue
                selected_indices.append(idx)
                used_assignment.add(idx)
                if len(selected_indices) >= count:
                    break
            return selected_indices

        assigned = {
            "DAYS_0_2": take(high_order, counts.get("DAYS_0_2", 0)),
            "DAYS_3_7": take(mid_order, counts.get("DAYS_3_7", 0)),
            "DAYS_GT_7": take(low_order, counts.get("DAYS_GT_7", 0)),
        }
        ranges = {"DAYS_0_2": (0, 2), "DAYS_3_7": (3, 7), "DAYS_GT_7": (8, 30)}
        for band, indices in assigned.items():
            lo, hi = ranges[band]
            for pos, idx in enumerate(indices):
                opened = pd.to_datetime(fc.at[idx, "complaint_date"], errors="coerce")
                if pd.isna(opened):
                    opened = pd.Timestamp.utcnow()
                days = lo + pos % (hi - lo + 1)
                fc.at[idx, "complaint_resolved_date"] = (opened + pd.Timedelta(days=days)).date().isoformat()
                fc.at[idx, "complaint_status"] = "Closed"
        esc_count = max(1, round(len(fc) * 0.02))
        low_indices = low_order[:esc_count]
        fc["is_financial_ombudsman_service_referral"] = "N"
        fc.loc[low_indices, "is_financial_ombudsman_service_referral"] = "Y"
        outcome_counts = self._counts(len(fc), {"NOT_UPHELD": 65, "UPHELD": 20, "PARTIALLY_UPHELD": 15})
        used_outcome: set[int] = set()

        def take_outcome(candidates: list[int], count: int) -> list[int]:
            selected_indices = []
            for idx in candidates + low_order + mid_order + high_order:
                if idx in used_outcome:
                    continue
                selected_indices.append(idx)
                used_outcome.add(idx)
                if len(selected_indices) >= count:
                    break
            return selected_indices

        outcome = {
            "NOT_UPHELD": take_outcome(low_order, outcome_counts.get("NOT_UPHELD", 0)),
            "UPHELD": take_outcome(mid_order + high_order, outcome_counts.get("UPHELD", 0)),
            "PARTIALLY_UPHELD": take_outcome(low_order + mid_order, outcome_counts.get("PARTIALLY_UPHELD", 0)),
        }
        labels = {"NOT_UPHELD": "Not Upheld", "UPHELD": "Upheld", "PARTIALLY_UPHELD": "Partially Upheld"}
        for band, indices in outcome.items():
            fc.loc[indices, "complaint_upheld_status"] = labels[band]
        self.facts["fact_complaint"] = _ordered(fc, self.schemas["fact_complaint"])

    def _write_all(self) -> None:
        for table, columns in self.schemas.items():
            frame = self.dims.get(table) if table.startswith("dim_") else self.facts.get(table)
            if frame is None:
                frame = pd.DataFrame(columns=columns)
            frame = _ordered(frame.copy(), columns)
            sk_col = "date_sk" if table == "dim_date" else ("insured_object_sk" if table in {"dim_home", "dim_motor"} else f"{table.removeprefix('dim_')}_sk")
            if table.startswith("dim_") and "attr_hash" in frame.columns:
                blank_hash = frame["attr_hash"].astype(str).str.strip().eq("")
                if blank_hash.any():
                    frame.loc[blank_hash, "attr_hash"] = frame.loc[blank_hash].apply(lambda row: _hash_row(row.to_dict(), {sk_col, "attr_hash"}), axis=1)
            if table.startswith("dim_") and sk_col in frame.columns:
                frame = frame[frame[sk_col].astype(str).ne("-1")]
            frame.to_csv(self.out_dir / f"{table}.csv", index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate direct dimensional MLOps dim/fact CSVs from a MLOps vault run.")
    parser.add_argument("--run-id", help="MLOps synthetic run id. Defaults to latest.")
    parser.add_argument("--source", help="Explicit MLOps vault folder path.")
    parser.add_argument("--output", help="Output folder. Defaults to data/dim_fact_direct/mlops/<run_id>.")
    args = parser.parse_args()

    source = Path(args.source) if args.source else (Path(SYNTHETIC_MLOPS_ROOT) / args.run_id if args.run_id else _latest_run(Path(SYNTHETIC_MLOPS_ROOT)))
    run_id = args.run_id or source.name
    output = Path(args.output) if args.output else OUT_ROOT / run_id
    built = DirectDimFactBuilder(source, output).build()
    print(f"DIRECT DIM/FACT OUTPUT: {built}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
