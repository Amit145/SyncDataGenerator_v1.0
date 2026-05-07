from __future__ import annotations

import csv
import os
import random
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from helper.csv_writer import write_csv
from helper.enhanced_ddl import parse_enhanced_ddl
from helper.enhanced_rules import (
    BASE_CUSTOMER_STATUSES,
    BASE_POLICY_CHANNELS,
    CHANNEL_TYPE_BY_NAME,
    claim_product_for_code,
    insurance_category_for_code,
)
from helper.key_factory import md5_hasher


ROOT = Path(__file__).resolve().parents[1]
ENHANCED_EXAMPLE_DIR = ROOT / "enhanced_360" / "data_example"
RS = "CRM"


def _norm_name(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def _norm_row(row: dict) -> dict:
    return {_norm_name(key): value for key, value in row.items()}


def _ordered(row: dict, columns: list[str]) -> dict:
    return {column: row.get(column, "") for column in columns}


def _read_example(file_name: str) -> list[dict]:
    path = ENHANCED_EXAMPLE_DIR / file_name
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _sample(rows: list[dict], idx: int) -> dict:
    if not rows:
        return {}
    return rows[idx % len(rows)]


def _date(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip()
    for fmt in ("%d-%b-%y", "%d-%b-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value[:19]).date().isoformat()
    except ValueError:
        return value


def _date_obj(value) -> date | None:
    text = _date(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def _int(value, default="") -> str:
    if value in (None, ""):
        return default
    try:
        return str(int(float(str(value).replace(",", "").strip())))
    except ValueError:
        return default


def _float(value, default="") -> str:
    if value in (None, ""):
        return default
    try:
        return str(float(str(value).replace(",", "").strip()))
    except ValueError:
        return default


def _link_row(table_name: str, left_col: str, left_hk: str, right_col: str, right_hk: str, load_date: str) -> dict:
    pk = f"{table_name.removeprefix('link_')}_hash_key"
    return {
        pk: md5_hasher(f"{left_hk}|{right_hk}"),
        "load_date": load_date,
        "record_source": RS,
        left_col: left_hk,
        right_col: right_hk,
    }


def _link_row_with_pk(pk: str, left_col: str, left_hk: str, right_col: str, right_hk: str, load_date: str) -> dict:
    return {
        pk: md5_hasher(f"{left_hk}|{right_hk}"),
        "load_date": load_date,
        "record_source": RS,
        left_col: left_hk,
        right_col: right_hk,
    }


def _id_from_hub(row: dict, id_col: str) -> str:
    return str(row.get(id_col, "") or row.get(id_col.replace("_id", "_number"), "") or "")


def _build_base_tables(ctx: dict) -> dict[str, list[dict]]:
    links = dict(ctx.get("links", {}))
    if ctx.get("link_quote_product"):
        links["Link_Quote_Product"] = ctx["link_quote_product"]

    sources = {
        "hub_person": ctx.get("hub_person_rows", []),
        "hub_natural_person": ctx.get("hub_nat", []),
        "hub_legal_person": ctx.get("hub_leg", []),
        "hub_product": ctx.get("hub_prod_rows", []),
        "hub_lead": ctx.get("hub_lead_rows", []),
        "hub_customer": ctx.get("hub_cust_rows", []),
        "hub_identities": ctx.get("hub_id_rows", []),
        "hub_contact": ctx.get("hub_con_rows", []),
        "hub_consent": ctx.get("hub_cns_rows", []),
        "hub_account": ctx.get("hub_acc_rows", []),
        "hub_marketing_preference": ctx.get("hub_mpr_rows", []),
        "hub_marketing_engagement": ctx.get("hub_men_rows", []),
        "hub_quote": ctx.get("hub_quo_rows", []),
        "hub_policy": ctx.get("hub_pol_rows", []),
        "hub_motor": ctx.get("hub_mot_rows", []),
        "hub_home": ctx.get("hub_home_rows", []),
        "hub_home_address": ctx.get("hub_addr_rows", []),
        "sat_natural_person": ctx.get("sat_nat", []),
        "sat_legal_person": ctx.get("sat_leg", []),
        "sat_person": ctx.get("sat_per", []),
        "sat_lead": ctx.get("sat_lea", []),
        "sat_customer": ctx.get("sat_cus", []),
        "sat_identities": ctx.get("sat_eci", []),
        "sat_contact": ctx.get("sat_con", []),
        "sat_consent": ctx.get("sat_cns", []),
        "sat_account": ctx.get("sat_acc", []),
        "sat_marketing_preference": ctx.get("sat_mpr", []),
        "sat_marketing_engagement": ctx.get("sat_men", []),
        "sat_quote": ctx.get("sat_quo", []),
        "sat_policy": ctx.get("sat_pol", []),
        "sat_motor": ctx.get("sat_mot", []),
        "sat_home": ctx.get("sat_hom", []),
        "sat_home_address": ctx.get("sat_adr", []),
        "sat_product": ctx.get("sat_product_rows", []),
    }

    for link_name, rows in links.items():
        sources[_norm_name(link_name)] = rows

    return {table_name: [_norm_row(row) for row in rows] for table_name, rows in sources.items()}


def _augment_base_satellites(tables: dict[str, list[dict]]) -> None:
    customers = _read_example("DimCustomer.csv")
    policies = _read_example("DimPolicy.csv")
    fact_policies = _read_example("FactPolicy.csv")
    products = _read_example("DimProduct.csv")
    quotes = _read_example("FactQuote.csv")

    for idx, row in enumerate(tables.get("sat_customer", [])):
        sample = _sample(customers, idx)
        row.update({
            "income_band": sample.get("IncomeBand", ""),
            "customer_satisfaction": sample.get("CustomerSatisfaction", ""),
            "customer_age_band": sample.get("AgeBand", ""),
            "net_promotor_code_segment": sample.get("NPS Segment", ""),
        })

    for idx, row in enumerate(tables.get("sat_policy", [])):
        dim = _sample(policies, idx)
        fact = _sample(fact_policies, idx)
        row.update({
            "quote_id": dim.get("QuoteID", ""),
            "policy_type": dim.get("Policy Type", ""),
            "is_policy_renewal": dim.get("Renewal Indicator", ""),
            "policy_cancellation_reason": dim.get("Cancellation Reason", ""),
            "policy_sum_insured": _int(dim.get("SumInsured")),
            "policy_retention_limit": _int(dim.get("RetentionLimit")),
            "policy_risk_score": _float(dim.get("RiskScore")),
            "policy_risk_band": dim.get("RiskBand", ""),
            "policy_base_premium": _int(fact.get("BasePrem")),
            "gross_written_premium": _int(fact.get("GrossWrittenPremium(GWP)")),
            "earned_premium": _int(fact.get("EarnedPremium")),
            "incurred_but_not_reported": _int(fact.get("IncurredButNotReported(IBNR)")),
            "operating_expenses": _int(fact.get("OperatingExpenses")),
            "administrative_expenses": _int(fact.get("AdministrativeExpenses")),
            "profit_margin": _int(fact.get("ProfitMargin")),
            "taxes_and_levies": _int(fact.get("TaxesandLevies")),
            "amount_approved": _int(fact.get("AmountApproved")),
            "ceded_premium": _int(fact.get("CededPremium")),
            "commission_paid": _int(fact.get("CommissionPaid")),
            "ceded_commission": _int(fact.get("CededCommission")),
            "exposure_amount": _int(fact.get("ExposureAmount")),
            "investment_income": _int(fact.get("InvestmentIncome")),
            "underwriting_cycle_time_in_days": _int(fact.get("UnderwritingCycleTime(days)")),
            "underwriting_expenses": _int(fact.get("UnderwritingExpenses")),
            "transaction_date": _date(fact.get("TransactionDate")),
            "record_type": fact.get("RecordType", ""),
            "discount": _int(fact.get("Discount ")),
            "override_commission": _int(fact.get("Override Comission")),
            "partial_recovery_percentage": _int(fact.get("Partial Recovery%")),
            "policy_issue_date": _date(fact.get("InceptionDate")),
        })

    for idx, row in enumerate(tables.get("sat_product", [])):
        sample = _sample(products, idx)
        row.update({
            "product_variant": sample.get("ProductVariant", ""),
            "product_name": sample.get("ProductName", ""),
            "product_launch_date": _date(sample.get("ProductLaunchDate")),
            "product_status": sample.get("ProductStatus", ""),
            "product_line_of_business_code": sample.get("LOBCode", ""),
            "underwriting_group": sample.get("UnderwritingGroup", ""),
            "regulatory_approval_code": sample.get("RegulatoryApprovalCode", ""),
        })

    for idx, row in enumerate(tables.get("sat_quote", [])):
        sample = _sample(quotes, idx)
        row.update({
            "quoted_premium": _int(sample.get("QuotedPremium")),
            "quote_date": _date(sample.get("quote_date")),
            "quote_month_name": sample.get("Month name", ""),
            "risk_score": _int(sample.get("RiskScore")),
            "policy_complexity": sample.get("PolicyComplexity", ""),
            "uw_approval_type": sample.get("UWApprovalType", ""),
            "rejection_reason": sample.get("RejectionReason", ""),
        })


def _settings(cfg: dict | None) -> dict:
    defaults = {
        "broker_count": 20,
        "campaign_count": 10,
        "channel_count": 4,
        "claim_policy_rate": 0.08,
        "complaint_customer_rate": 0.02,
        "override_policy_rate": 0.05,
        "regulation_count": 100,
    }
    if cfg:
        defaults.update(cfg.get("enhanced_settings", {}))
    return defaults


def _add_enhanced_entities(tables: dict[str, list[dict]], ctx: dict, cfg: dict | None) -> None:
    settings = _settings(cfg)
    rng = random.Random((ctx.get("seed") or 42) + 7001)
    fallback_date = ctx.get("extract_ts") or datetime.now().replace(microsecond=0).isoformat()
    hub_date = ctx.get("hub_load_date") or ctx.get("HUB_DATE") or fallback_date
    link_date = ctx.get("link_load_date") or hub_date
    sat_date = ctx.get("sat_load_date") or link_date

    person_hks = list(ctx.get("person_hks", []))
    policy_hks = [row.get("Policy Hash Key") for row in ctx.get("hub_pol_rows", []) if row.get("Policy Hash Key")]
    customer_hks = [row.get("Customer Hash Key") for row in ctx.get("hub_cust_rows", []) if row.get("Customer Hash Key")]
    policy_sat_by_hk = {
        row.get("Policy Hash Key"): row for row in ctx.get("sat_pol", []) if row.get("Policy Hash Key")
    }
    customer_sat_by_hk = {
        row.get("Customer Hash Key"): row for row in ctx.get("sat_cus", []) if row.get("Customer Hash Key")
    }
    policy_to_product_code = {
        policy_hk: ctx.get("policy_to_product_id", {}).get(policy_hk, "")
        for policy_hk in policy_hks
    }
    quote_hks = [row.get("Quote Hash Key") for row in ctx.get("hub_quo_rows", []) if row.get("Quote Hash Key")]
    policy_to_quote_hk = dict(ctx.get("policy_to_quote_map", {}))
    quote_to_policy_hk = {quote_hk: policy_hk for policy_hk, quote_hk in policy_to_quote_hk.items()}
    fact_policies = _read_example("FactPolicy.csv")
    person_to_customer_hk = {}
    for person_hk, customer_hk_or_hks in ctx.get("person_to_customer", {}).items():
        if isinstance(customer_hk_or_hks, list):
            if customer_hk_or_hks:
                person_to_customer_hk[person_hk] = customer_hk_or_hks[0]
        elif customer_hk_or_hks:
            person_to_customer_hk[person_hk] = customer_hk_or_hks
    policy_to_person_hk = dict(ctx.get("policy_to_person_map", {}))
    if not policy_to_person_hk:
        for person_hk, assigned_policy_hks in ctx.get("policy_person_map", {}).items():
            for assigned_policy_hk in assigned_policy_hks:
                policy_to_person_hk[assigned_policy_hk] = person_hk
    policy_to_customer_hk = {
        policy_hk: person_to_customer_hk.get(policy_to_person_hk.get(policy_hk))
        for policy_hk in policy_hks
    }
    customer_to_policy_hks: dict[str, list[str]] = {}
    for policy_hk, customer_hk in policy_to_customer_hk.items():
        if customer_hk:
            customer_to_policy_hks.setdefault(customer_hk, []).append(policy_hk)
    active_policy_hks = [
        policy_hk
        for policy_hk, row in policy_sat_by_hk.items()
        if row.get("Policy Status") == "ACTIVE"
    ]
    active_customer_hks = [
        customer_hk
        for customer_hk, row in customer_sat_by_hk.items()
        if row.get("Customer Status") in BASE_CUSTOMER_STATUSES
    ]

    agent_policy_persons = sorted({
        policy_to_person_hk[policy_hk]
        for policy_hk in policy_hks
        if policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel") == "AGENT" and policy_to_person_hk.get(policy_hk)
    })

    broker_examples = _read_example("DimBroker.csv")
    broker_target_count = int(settings["broker_count"])
    if agent_policy_persons and broker_target_count <= 0:
        broker_target_count = 1
    broker_count = min(broker_target_count, len(broker_examples) or broker_target_count)
    for idx in range(broker_count):
        sample = _sample(broker_examples, idx)
        agent_id = sample.get("AgentID") or f"BRK{idx + 1:05d}"
        broker_hk = md5_hasher(agent_id)
        tables["hub_broker"].append({
            "broker_hash_key": broker_hk,
            "load_date": hub_date,
            "record_source": RS,
            "agent_id": agent_id,
        })
        tables["sat_broker"].append({
            "broker_hash_key": broker_hk,
            "load_date": sat_date,
            "agent_name": sample.get("AgentName", ""),
            "agent_type": sample.get("AgentType", ""),
            "agent_license_number": sample.get("LicenseNumber", ""),
            "agent_net_promoter_score": _float(sample.get("NPS")),
            "agent_commission_percentage": _float(sample.get("Commission percentage")),
            "agent_status": "ACTIVE",
        })

    broker_hks = [row["broker_hash_key"] for row in tables["hub_broker"]]
    for idx, person_hk in enumerate(agent_policy_persons):
        if broker_hks:
            broker_hk = broker_hks[idx % len(broker_hks)]
            tables["link_broker_person"].append(_link_row(
                "link_broker_person",
                "person_hash_key",
                person_hk,
                "broker_hash_key",
                broker_hk,
                link_date,
            ))
            quote_hks_for_person = ctx.get("person_to_quote", {}).get(person_hk, [])
            for policy_hk in ctx.get("policy_person_map", {}).get(person_hk, []):
                tables["link_policy_broker"].append(_link_row(
                    "link_policy_broker",
                    "broker_hash_key",
                    broker_hk,
                    "policy_hash_key",
                    policy_hk,
                    link_date,
                ))
            for quote_hk in quote_hks_for_person:
                tables["link_quote_broker"].append(_link_row(
                    "link_quote_broker",
                    "quote_hash_key",
                    quote_hk,
                    "broker_hash_key",
                    broker_hk,
                    link_date,
                ))

    campaign_examples = _read_example("DimCampaign.csv")
    quote_examples = _read_example("FactQuote.csv")
    campaign_count = min(int(settings["campaign_count"]), len(campaign_examples) or int(settings["campaign_count"]))
    for idx in range(campaign_count):
        sample = _sample(campaign_examples, idx)
        quote_sample = _sample(quote_examples, idx)
        campaign_id = sample.get("campaign_id") or f"CAM{idx + 1:03d}"
        campaign_hk = md5_hasher(campaign_id)
        tables["hub_campaign"].append({
            "campaign_hash_key": campaign_hk,
            "load_date": hub_date,
            "record_source": RS,
            "campaign_id": campaign_id,
        })
        tables["sat_campaign"].append({
            "campaign_hash_key": campaign_hk,
            "load_date": sat_date,
            "campaign_name": sample.get("campaign_name", ""),
            "campaign_type": sample.get("Campaign_Type", ""),
            "campaign_start_date": _date(sample.get("start_date")),
            "campaign_end_date": _date(sample.get("end_date")),
            "campaign_status": sample.get("Status", ""),
            "campaign_budget": _int(sample.get("Budget")),
            "campaign_target_audience": sample.get("Target_Audience", ""),
            "campaign_marketing_source": sample.get("marketing_source", ""),
            "campaign_owner_department": sample.get("Owner_Department", ""),
            "campaign_country": sample.get("Country", ""),
            "campaign_conversion_goal": sample.get("Conversion_Goal", ""),
            "number_of_impressions": _int(quote_sample.get("Impressions")),
            "number_of_clicks": _int(quote_sample.get("Clicks")),
            "number_of_is_active": _int(quote_sample.get("Active")),
            "number_of_visits": _int(quote_sample.get("Visits")),
            "number_of_policy_purchases": _int(quote_sample.get("PolicyPurchases")),
            "number_of_emails_sent": _int(quote_sample.get("EmailsSent")),
            "number_of_email_bounced": _int(quote_sample.get("Bounces")),
            "number_of_emails_delivered": _int(quote_sample.get("EmailsDelivered")),
            "number_of_emails_opened": _int(quote_sample.get("EmailsOpened")),
            "click_through_rate": _float(quote_sample.get("CTR")),
            "spend_amount": _float(quote_sample.get("Spend")),
            "incremental_revenue": _int(quote_sample.get("IncrementalRevenue")),
            "survey_wave": quote_sample.get("SurveyWave", ""),
            "total_number_of_respondents": _int(quote_sample.get("TotalRespondents")),
            "number_of_respondents_aware": _int(quote_sample.get("RespondentsAware")),
            "number_of_promoters": _int(quote_sample.get("Promoters")),
            "number_of_passives": _int(quote_sample.get("Passives")),
            "number_of_detractors": _int(quote_sample.get("Detractors")),
            "number_of_followers": _int(quote_sample.get("Followers")),
            "number_of_likes": _int(quote_sample.get("Likes")),
            "number_of_comments": _int(quote_sample.get("Comments")),
            "number_of_shares": _int(quote_sample.get("Shares")),
            "number_of_brand_mentions": _int(quote_sample.get("BrandMentions")),
            "number_of_category_mentions": _int(quote_sample.get("CategoryMentions")),
        })

    campaign_hks = [row["campaign_hash_key"] for row in tables["hub_campaign"]]
    lead_persons = sorted(ctx.get("person_to_lead", {}).keys())
    for idx, person_hk in enumerate(lead_persons):
        if campaign_hks:
            tables["link_person_campaign"].append(_link_row(
                "link_person_campaign",
                "campaign_hash_key",
                campaign_hks[idx % len(campaign_hks)],
                "person_hash_key",
                person_hk,
                link_date,
            ))

    for idx, channel_name in enumerate(BASE_POLICY_CHANNELS, start=1):
        channel_id = f"CHN{idx:03d}"
        channel_hk = md5_hasher(channel_id)
        tables["hub_channel"].append({
            "channel_hash_key": channel_hk,
            "load_date": hub_date,
            "record_source": RS,
            "channel_id": channel_id,
        })
        tables["sat_channel"].append({
            "channel_hash_key": channel_hk,
            "load_date": sat_date,
            "channel_name": channel_name,
            "channel_type": CHANNEL_TYPE_BY_NAME.get(channel_name, ""),
        })

    channel_hk_by_name = {
        row["channel_name"]: row["channel_hash_key"] for row in tables["sat_channel"]
    }
    for policy_hk in policy_hks:
        sales_channel = policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel", "")
        channel_hk = channel_hk_by_name.get(sales_channel)
        if channel_hk:
            tables["link_policy_channel"].append(_link_row(
                "link_policy_channel",
                "channel_hash_key",
                channel_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))
        quote_hk = policy_to_quote_hk.get(policy_hk)
        if quote_hk:
            tables["link_policy_quote"].append(_link_row(
                "link_policy_quote",
                "quote_hash_key",
                quote_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))

    for idx, quote_hk in enumerate(quote_hks):
        policy_hk = quote_to_policy_hk.get(quote_hk)
        sales_channel = policy_sat_by_hk.get(policy_hk, {}).get("Sales Channel", "") if policy_hk else ""
        if not sales_channel:
            sales_channel = BASE_POLICY_CHANNELS[idx % len(BASE_POLICY_CHANNELS)]
        channel_hk = channel_hk_by_name.get(sales_channel)
        if channel_hk:
            tables["link_quote_channel"].append(_link_row(
                "link_quote_channel",
                "quote_hash_key",
                quote_hk,
                "channel_hash_key",
                channel_hk,
                link_date,
            ))

    insured_idx = 0
    for policy_hk in policy_hks:
        product_code = policy_to_product_code.get(policy_hk, "")
        policy_row = policy_sat_by_hk.get(policy_hk, {})
        asset_pairs = []
        motor_hk = ctx.get("policy_to_motor", {}).get(policy_hk)
        home_hk = ctx.get("policy_to_home", {}).get(policy_hk)
        if motor_hk:
            asset_pairs.append(("MOTOR", motor_hk, "motor"))
        if home_hk:
            asset_pairs.append(("HOME", home_hk, "home"))
        if not asset_pairs:
            continue
        for object_type, asset_hk, asset_kind in asset_pairs:
            insured_idx += 1
            insured_object_id = f"INS_OBJ_{insured_idx:07d}"
            insured_object_hk = md5_hasher(insured_object_id)
            tables["hub_insured_object"].append({
                "insured_object_hash_key": insured_object_hk,
                "load_date": hub_date,
                "record_source": RS,
                "insured_object_id": insured_object_id,
            })
            tables["sat_insured_object"].append({
                "insured_object_hash_key": insured_object_hk,
                "load_date": sat_date,
                "insured_object_type": object_type,
                "insured_object_sub_type": product_code,
                "insured_object_description": f"{object_type} asset for policy",
                "insured_object_current_status": policy_row.get("Policy Status", ""),
                "insured_value": policy_row.get("Policy Sum Insured", "") or policy_row.get("policy_sum_insured", ""),
                "currency_code": "GBP",
                "insured_object_start_date": _date(policy_row.get("Policy Start Date")),
                "insured_object_end_date": _date(policy_row.get("Policy End Date")),
            })
            tables["link_policy_insured_object"].append(_link_row(
                "link_policy_insured_object",
                "insured_object_hash_key",
                insured_object_hk,
                "policy_hash_key",
                policy_hk,
                link_date,
            ))
            if asset_kind == "motor":
                tables["link_insured_object_motor"].append(_link_row_with_pk(
                    "insured_object_home_hash_key",
                    "insured_object_hash_key",
                    insured_object_hk,
                    "motor_hash_key",
                    asset_hk,
                    link_date,
                ))
            else:
                tables["link_insured_object_home"].append(_link_row(
                    "link_insured_object_home",
                    "insured_object_hash_key",
                    insured_object_hk,
                    "home_hash_key",
                    asset_hk,
                    link_date,
                ))

    claim_examples = _read_example("DimClaim.csv")
    claim_pool = active_policy_hks or policy_hks
    claim_count = min(len(claim_pool), max(1, int(len(claim_pool) * float(settings["claim_policy_rate"])))) if claim_pool else 0
    selected_policies = rng.sample(claim_pool, claim_count) if claim_count else []
    for idx, policy_hk in enumerate(selected_policies):
        sample = _sample(claim_examples, idx)
        fact = _sample(fact_policies, idx)
        claim_id = f"ENH_CLM_{idx + 1:07d}"
        claim_hk = md5_hasher(claim_id)
        policy_row = policy_sat_by_hk.get(policy_hk, {})
        product_code = policy_to_product_code.get(policy_hk, "")
        policy_start = _date_obj(policy_row.get("Policy Start Date"))
        policy_end = _date_obj(policy_row.get("Policy End Date"))
        reported = _date_obj(sample.get("ClaimReportedDate")) or policy_start
        if policy_start and reported and reported < policy_start:
            reported = policy_start
        if policy_end and reported and reported > policy_end:
            reported = policy_end
        settlement = _date_obj(sample.get("ClaimSettlementDate"))
        if reported and settlement and settlement < reported:
            settlement = reported
        tables["hub_claim"].append({
            "claim_hash_key": claim_hk,
            "load_date": hub_date,
            "record_source": RS,
            "claim_id": claim_id,
        })
        tables["sat_claim"].append({
            "claim_hash_key": claim_hk,
            "load_date": sat_date,
            "claim_number": sample.get("ClaimNo", claim_id),
            "claim_type": sample.get("ClaimType", ""),
            "claim_status": sample.get("ClaimStatus", ""),
            "claim_reason": sample.get("ClaimReason", ""),
            "claim_channel": policy_row.get("Sales Channel", ""),
            "claim_handler": sample.get("ClaimHandler", ""),
            "claim_reported_date": reported.isoformat() if reported else "",
            "claim_settlement_date": settlement.isoformat() if settlement else "",
            "claim_product": claim_product_for_code(product_code),
            "is_claim_suspicious": sample.get("SuspiciousFlag", ""),
            "is_claim_fraud": sample.get("FraudFlag", ""),
            "claim_fraud_status": sample.get("FraudStatus", ""),
            "claim_fraud_type": sample.get("FraudType", ""),
            "claim_fraud_detection_method": sample.get("FraudDetectionMethod", ""),
            "is_litigation": sample.get("LitigationIndicator", ""),
            "litigation_reason": sample.get("LitigationReason", ""),
            "litigation_start_date": _date(sample.get("LitigationStartDate")),
            "litigation_end_date": _date(sample.get("LitigationEndDate")),
            "litigation_outcome": sample.get("LitigationOutcome", ""),
            "litigation_duration_days": _int(sample.get("LitigationDurationDays")),
            "claim_fraud_detection_time_in_days": _int(sample.get("FraudDetectionTime_Days")),
            "is_recovery_opportunity": sample.get("RecoveryOpportunityFlag", ""),
            "recovery_priority_score": _int(sample.get("RecoveryPriorityScore")),
            "recovery_category": sample.get("RecoveryCategory", ""),
            "recovery_source": sample.get("RecoverySource", ""),
            "first_recovery_date": _date(sample.get("First_Recovery_Date")),
            "last_recovery_date": _date(sample.get("Last_Recovery_Date")),
            "is_recovery_happened": sample.get("Recovery_Happened", ""),
            "days_to_first_recovery": _int(sample.get("Days_to_First_Recovery")),
            "days_to_last_recovery": _int(sample.get("Days_to_Last_Recovery")),
            "avg_days_to_close_claim": _int(sample.get("Avg. Days CloseClaim")),
            "claim_fraud_outcome": sample.get("Fraud_Outcome", ""),
            "recovery_type": sample.get("Recovery type", ""),
            "recovery_band": sample.get("Recovery Band", ""),
            "third_party_involved": sample.get("TPI", ""),
            "third_party_involved_overall_score": _float(sample.get("TPIOverallScore")),
            "solicitor": sample.get("Solicitor", ""),
            "claim_amount": _int(fact.get("ClaimAmount")),
            "claims_paid": _int(fact.get("ClaimsPaid")),
            "outstanding_reserve": _int(fact.get("OutstandingReserve")),
            "claims_expenses": _int(fact.get("Claims Expenses")),
            "recovery_received": _int(fact.get("RecoveryReceived")),
            "compensation_offered": _int(fact.get("CompensationOffered")),
            "remediation_amount": _int(fact.get("RemediationAmount")),
            "suspectd_amount": _int(fact.get("SuspectedAmount")),
            "fraud_amount": _int(fact.get("FraudAmount")),
            "legal_expenses": _int(fact.get("LegalExpenses")),
            "claim_band": fact.get("ClaimBand", ""),
            "claim_band_sort": _int(fact.get("ClaimBand Sort")),
        })
        tables["link_claim_policy"].append(_link_row(
            "link_claim_policy",
            "policy_hash_key",
            policy_hk,
            "claim_hash_key",
            claim_hk,
            link_date,
        ))

    complaint_examples = _read_example("FactComplaints.csv") or _read_example("DimComplaints.csv")
    category_examples = _read_example("DimInsCategory.csv")
    complaint_pool = active_customer_hks or customer_hks
    complaint_count = min(len(complaint_pool), max(1, int(len(complaint_pool) * float(settings["complaint_customer_rate"])))) if complaint_pool else 0
    selected_customers = rng.sample(complaint_pool, complaint_count) if complaint_count else []
    complaint_hks = []
    for idx, customer_hk in enumerate(selected_customers):
        sample = _sample(complaint_examples, idx)
        category = _sample(category_examples, idx)
        complaint_id = sample.get("Complaint ID") or f"CMP{idx + 1:07d}"
        complaint_hk = md5_hasher(complaint_id)
        customer_row = customer_sat_by_hk.get(customer_hk, {})
        customer_since = _date_obj(customer_row.get("Customer Since"))
        linked_policy_hks = customer_to_policy_hks.get(customer_hk, [])
        preferred_policy_hk = next((hk for hk in linked_policy_hks if hk in active_policy_hks), None)
        if not preferred_policy_hk and linked_policy_hks:
            preferred_policy_hk = linked_policy_hks[0]
        policy_row = policy_sat_by_hk.get(preferred_policy_hk, {})
        product_code = policy_to_product_code.get(preferred_policy_hk, "")
        complaint_date = _date_obj(sample.get("Complaint Date")) or customer_since
        if customer_since and complaint_date and complaint_date < customer_since:
            complaint_date = customer_since
        acknowledgement_date = _date_obj(sample.get("Acknowledgement Date"))
        if complaint_date and acknowledgement_date and acknowledgement_date < complaint_date:
            acknowledgement_date = complaint_date
        resolved_date = _date_obj(sample.get("Resolved Date"))
        min_resolved = acknowledgement_date or complaint_date
        if min_resolved and resolved_date and resolved_date < min_resolved:
            resolved_date = min_resolved
        tables["hub_complaint"].append({
            "complaint_hash_key": complaint_hk,
            "load_date": hub_date,
            "record_source": RS,
            "complaint_id": complaint_id,
        })
        tables["sat_complaint"].append({
            "complaint_hash_key": complaint_hk,
            "load_date": sat_date,
            "complaint_date": complaint_date.isoformat() if complaint_date else "",
            "complaint_acknowledgement_date": acknowledgement_date.isoformat() if acknowledgement_date else "",
            "complaint_resolved_date": resolved_date.isoformat() if resolved_date else "",
            "complaint_upheld_status": sample.get("Upheld Status", ""),
            "is_financial_ombudsman_service_referral": sample.get("FOS Referral", ""),
            "complaint_driver": sample.get("Complaint Driver", ""),
            "complaint_channel": policy_row.get("Sales Channel", ""),
            "compensation_amount": _int(sample.get("Compensation Amount")),
            "insurance_category": insurance_category_for_code(product_code) or category.get("Insurance Category", ""),
            "complaint_status": sample.get("Compaint Status", ""),
        })
        complaint_hks.append(complaint_hk)
        if preferred_policy_hk:
            tables["link_complaint_policy"].append(_link_row(
                "link_complaint_policy",
                "complaint_hash_key",
                complaint_hk,
                "policy_hash_key",
                preferred_policy_hk,
                link_date,
            ))

    override_examples = _read_example("DimOverride.csv")
    override_pool = active_policy_hks or policy_hks
    override_count = min(len(override_pool), max(1, int(len(override_pool) * float(settings["override_policy_rate"])))) if override_pool else 0
    selected_override_policies = rng.sample(override_pool, override_count) if override_count else []
    sat_policy_by_hk = {
        row.get("policy_hash_key"): row for row in tables.get("sat_policy", []) if row.get("policy_hash_key")
    }
    original_override_commission = {
        policy_hk: row.get("override_commission", "")
        for policy_hk, row in sat_policy_by_hk.items()
    }
    for row in sat_policy_by_hk.values():
        row["override_commission"] = ""
    for idx, policy_hk in enumerate(selected_override_policies):
        sample = _sample(override_examples, idx)
        base_override_id = (sample.get("over Ride ID ") or "OVR").strip()
        override_id = f"{base_override_id}_{idx + 1:06d}"
        override_hk = md5_hasher(override_id)
        tables["hub_override"].append({
            "override_hash_key": override_hk,
            "load_date": hub_date,
            "record_source": RS,
            "override_id": override_id,
        })
        tables["sat_override"].append({
            "override_hash_key": override_hk,
            "load_date": sat_date,
            "override_reason": sample.get("over ride reason", ""),
        })
        if policy_hk in sat_policy_by_hk:
            sat_policy_by_hk[policy_hk]["override_commission"] = (
                original_override_commission.get(policy_hk)
                or _int(sample.get("Override Comission"))
                or "0"
            )
        tables["link_policy_override"].append(_link_row(
            "link_policy_override",
            "override_hash_key",
            override_hk,
            "policy_hash_key",
            policy_hk,
            link_date,
        ))

    regulation_examples = _read_example("DimRegulations.csv")
    regulation_count = min(int(settings["regulation_count"]), len(regulation_examples) or int(settings["regulation_count"]))
    used_regulation_ids = set()
    for idx in range(regulation_count):
        sample = _sample(regulation_examples, idx)
        base_regulation_id = sample.get("RegulationID") or f"REG{idx + 1:05d}"
        regulation_id = base_regulation_id
        if regulation_id in used_regulation_ids:
            regulation_id = f"{base_regulation_id}_{idx + 1:05d}"
        used_regulation_ids.add(regulation_id)
        regulation_hk = md5_hasher(regulation_id)
        tables["hub_regulation"].append({
            "regulation_hash_key": regulation_hk,
            "load_date": hub_date,
            "record_source": RS,
            "regulation_id": regulation_id,
        })
        tables["sat_regulation"].append({
            "regulation_hash_key": regulation_hk,
            "load_date": sat_date,
            "regulation_number": sample.get("Regulation Number", ""),
            "regulation_name": sample.get("RegulationName", ""),
            "regulation_department": sample.get("Department", ""),
            "regulation_region": sample.get("Region", ""),
            "regulation_risk_level": sample.get("RiskLevel", ""),
            "regulation_compliance_status": sample.get("ComplianceStatus", ""),
            "regulation_date_raised": _date(sample.get("DateRaised")),
            "regulation_date_closed": _date(sample.get("DateClosed")),
            "regulation_owner": sample.get("Owner", ""),
            "regulation_deadline_date": _date(sample.get("Deadline_Date")),
            "is_regulation_on_time": sample.get("On_Time_Flag", ""),
        })
        if complaint_hks:
            tables["link_complaint_regulation"].append(_link_row(
                "link_complaint_regulation",
                "regulation_hash_key",
                regulation_hk,
                "complaint_hash_key",
                complaint_hks[idx % len(complaint_hks)],
                link_date,
            ))


def build_enhanced_synthetic(ctx: dict, output_dir: str, cfg: dict | None = None) -> str:
    ddl = parse_enhanced_ddl()
    schemas = ddl["tables"]
    tables = {table_name: [] for table_name in schemas}
    tables.update(_build_base_tables(ctx))
    tables = {table_name: tables.get(table_name, []) for table_name in schemas}

    _augment_base_satellites(tables)
    _add_enhanced_entities(tables, ctx, cfg)

    os.makedirs(output_dir, exist_ok=True)
    for table_name, columns in schemas.items():
        rows = [_ordered(row, columns) for row in tables.get(table_name, [])]
        write_csv(output_dir, f"{table_name}.csv", rows, fieldnames=columns)

    return output_dir
