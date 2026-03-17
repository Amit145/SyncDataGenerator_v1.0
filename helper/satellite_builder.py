import random
import hashlib
import re
import calendar
from datetime import date, datetime, timedelta
from faker import Faker
from helper.key_factory import get_now_iso

fake = Faker("en_GB")

# ------------------------------------------------------------
# UK Regions / Counties (safe replacement for fake.state())
# ------------------------------------------------------------
UK_REGIONS = [
    "Greater London", "West Midlands", "Greater Manchester", "Merseyside", "South Yorkshire",
    "West Yorkshire", "Tyne and Wear", "Kent", "Essex", "Surrey", "Hampshire", "Devon",
    "Cornwall", "Somerset", "Dorset", "Wiltshire", "Gloucestershire", "Oxfordshire",
    "Cambridgeshire", "Norfolk", "Suffolk", "Lincolnshire", "Nottinghamshire", "Derbyshire",
    "Leicestershire", "Northamptonshire", "Warwickshire", "Staffordshire", "Cheshire",
    "Lancashire", "Cumbria", "North Yorkshire", "East Sussex", "West Sussex",
    "Buckinghamshire", "Berkshire", "Hertfordshire"
]


# ============================================================
# DATE HELPERS (DROP-IN, NO NEW COLUMNS)
# ============================================================
def _as_date(x) -> date:
    """Accepts: None, 'YYYY-MM-DD', date, datetime -> date"""
    if x is None:
        return date.today()
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    # assume string
    return date.fromisoformat(str(x)[:10])


def _rand_date_between(start: date, end: date) -> date:
    """Random date inclusive start..end"""
    if end < start:
        start, end = end, start
    days = (end - start).days
    return start + timedelta(days=random.randint(0, days))


def _random_time() -> tuple[int, int, int]:
    return random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)


def _datetime_from_date_with_random_time(value: date) -> datetime:
    hour, minute, second = _random_time()
    return datetime.combine(value, datetime.min.time()).replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=0,
    )


def _rand_datetime_between(start: date, end: date) -> datetime:
    return _datetime_from_date_with_random_time(_rand_date_between(start, end))


def _coerce_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.fromisoformat(str(value)[:19])


def _cap_datetime_to_load(value: datetime, load_date) -> datetime:
    load_dt = _coerce_datetime(load_date)
    return min(value, load_dt)


def _add_one_year(dt: datetime) -> datetime:
    try:
        return dt.replace(year=dt.year + 1)
    except ValueError:
        last_day = calendar.monthrange(dt.year + 1, dt.month)[1]
        return dt.replace(year=dt.year + 1, day=min(dt.day, last_day))


def pick_gender_and_title():
    gender = random.choice(["M", "F"])  # extend if you want: ["M","F","X"]
    if gender == "M":
        title = random.choices(["Mr", "Dr"], [0.9, 0.1])[0]
    else:
        title = random.choices(["Ms", "Mrs", "Dr"], [0.75, 0.15, 0.10])[0]
    return gender, title


def _pick_uk_region() -> str:
    """Return a UK-style region/county string."""
    return random.choice(UK_REGIONS)


def _sat_common(hk_col: str, hk_val: str):
    """Standard DV sat columns + hash key column."""
    return {hk_col: hk_val, "Load Date": get_now_iso(), "Record Source": "CRM"}


# ------------------------------------------------------------
# HOME ADDRESS CACHE
# sat_home_address populates this.
# sat_home / sat_motor can read from it to ensure state matches.
# ------------------------------------------------------------
HOME_ADDRESS_CACHE: dict[str, dict] = {}


# HOME_ADDRESS_CACHE[address_hk] = {
#   "Street": str, "City": str, "Postcode": str, "State": str, "Country": "UK"
# }


def _stable_idx(key: str, n: int) -> int:
    """
    Deterministic index for mapping any hk -> one address.
    Using hashlib so result is stable across runs (unlike built-in hash()).
    """
    if n <= 0:
        return 0
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h, 16) % n


def _fallback_address() -> dict:
    """Safe UK-like address fallback if cache is empty or mapping not available."""
    return {
        "Street": fake.street_address(),
        "City": fake.city(),
        "Postcode": fake.postcode(),
        "State": _pick_uk_region(),
        "Country": "UK",
    }


def _address_from_cache_by_hk(any_hk: str) -> dict:
    """
    If you DON'T have a direct mapping (home->addr or motor->addr),
    this picks an address deterministically from the cache using hk.
    """
    if not HOME_ADDRESS_CACHE:
        return _fallback_address()

    keys = list(HOME_ADDRESS_CACHE.keys())
    addr_hk = keys[_stable_idx(any_hk, len(keys))]
    return HOME_ADDRESS_CACHE.get(addr_hk, _fallback_address())


# ============================================================
# CONSISTENCY LAYER (DROP-IN, DOES NOT TOUCH HK/PK/CARDINALITY)
# ============================================================
PERSON_PROFILE: dict[str, dict] = {}  # keyed by person_hk
EMAIL_REGISTRY: set[str] = set()

REAL_LIKE_EMAIL_DOMAINS = [
    "gmail.com", "outlook.com", "yahoo.com", "icloud.com", "hotmail.com",
    "proton.me", "live.com", "btinternet.com", "sky.com", "virginmedia.com"
]

COMPANY_DOMAINS = [
    "allianz.co.uk", "examplebank.co.uk", "exampleinsure.co.uk",
    "acme.co.uk", "contoso.co.uk"
]


def _stable_rng(seed_str: str) -> random.Random:
    s = int(hashlib.md5(seed_str.encode("utf-8")).hexdigest(), 16) % (2 ** 32)
    return random.Random(s)


def _slug(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", ".", s)
    s = re.sub(r"\.+", ".", s).strip(".")
    return s or "user"


def _unique_email(base_local: str, domain: str, rng: random.Random) -> str:
    email = f"{base_local}@{domain}"
    if email not in EMAIL_REGISTRY:
        EMAIL_REGISTRY.add(email)
        return email

    for _ in range(50):
        suffix = rng.randint(10, 9999)
        candidate = f"{base_local}{suffix}@{domain}"
        if candidate not in EMAIL_REGISTRY:
            EMAIL_REGISTRY.add(candidate)
            return candidate

    candidate = f"{base_local}{hashlib.md5(email.encode('utf-8')).hexdigest()[:6]}@{domain}"
    EMAIL_REGISTRY.add(candidate)
    return candidate


def pick_title(gender: str, marital_status: str, rng: random.Random | None = None) -> str:
    # Keep your earlier rule, but deterministic if rng provided
    r = rng if rng else random
    if gender == "Male":
        return "Mr"
    if gender == "Female":
        if marital_status == "Married":
            return "Mrs"
        else:
            return "Miss"
    return "Mx"


def _pick_title_uk(gender: str, marital_status: str, rng: random.Random) -> str:
    # Slightly more realistic UK + marital
    if gender == "Male":
        return "Dr" if rng.random() < 0.07 else "Mr"
    if gender == "Female":
        if marital_status == "Married":
            return rng.choices(["Mrs", "Ms", "Dr"], weights=[0.55, 0.38, 0.07])[0]
        return rng.choices(["Ms", "Miss", "Dr"], weights=[0.70, 0.25, 0.05])[0]
    return "Mx"


def _personal_email_for_person(first: str, last: str, dob_year: int, rng: random.Random) -> str:
    domain = rng.choice(REAL_LIKE_EMAIL_DOMAINS)
    patterns = [
        _slug(f"{first}.{last}"),
        _slug(f"{first}{last[:1]}"),
        _slug(f"{first[:1]}.{last}"),
        _slug(f"{first}.{last}{str(dob_year)[-2:]}"),
    ]
    local = rng.choice(patterns)
    return _unique_email(local, domain, rng)


def _company_email_for_person(first: str, last: str, rng: random.Random) -> str:
    domain = rng.choice(COMPANY_DOMAINS)
    local = _slug(f"{first}.{last}")
    return _unique_email(local, domain, rng)


# ---------------- NATURAL PERSON (occupation map + consistent picker) ----------------
OCCUPATION_MAP = {
    "Technology": [
        "Software Engineer",
        "Data Engineer",
        "Cloud Architect",
        "AI Engineer"
    ],
    "Healthcare": [
        "Nurse",
        "Physiotherapist",
        "General Practitioner",
        "Surgeon"
    ],
    "Finance": [
        "Financial Analyst",
        "Risk Manager",
        "Actuary",
        "Accountant"
    ],
    "Education": [
        "Teacher",
        "Lecturer",
        "Professor"
    ],
    "Retail": [
        "Store Manager",
        "Sales Executive",
        "Inventory Supervisor"
    ]
}


def pick_occupation_and_job():
    occupation = random.choice(list(OCCUPATION_MAP.keys()))
    job_title = random.choice(OCCUPATION_MAP[occupation])
    return occupation, job_title


def _pick_occupation_and_job_consistent(rng: random.Random):
    occupation = rng.choice(list(OCCUPATION_MAP.keys()))
    job_title = rng.choice(OCCUPATION_MAP[occupation])
    return occupation, job_title


def get_or_create_person_profile(person_hk: str) -> dict:
    """
    Canonical person info for consistency across satellites.
    DOES NOT change HKs or row counts. Only attribute values.
    """
    if person_hk in PERSON_PROFILE:
        return PERSON_PROFILE[person_hk]

    rng = _stable_rng(person_hk)

    gender = rng.choice(["Male", "Female"])
    marital_status = rng.choice(["Single", "Married"])

    first = fake.first_name_male() if gender == "Male" else fake.first_name_female()
    last = fake.last_name()

    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    title = _pick_title_uk(gender, marital_status, rng)

    occupation, job_title = _pick_occupation_and_job_consistent(rng)

    personal_email = _personal_email_for_person(first, last, dob.year, rng)
    work_email = _company_email_for_person(first, last, rng)

    person_type = rng.choices(["NATURAL", "LEGAL"], weights=[0.92, 0.08])[0]

    PERSON_PROFILE[person_hk] = {
        "gender": gender,
        "marital_status": marital_status,
        "title": title,
        "first": first,
        "last": last,
        "full_name": f"{first} {last}",
        "dob": dob,
        "birth_year": dob.year,
        "nationality": "GB",
        "preferred_language": "EN",
        "occupation": occupation,
        "job_title": job_title,
        "personal_email": personal_email,
        "work_email": work_email,
        "hashed_email": hashlib.md5(personal_email.encode("utf-8")).hexdigest(),
        "person_type": person_type,
    }
    return PERSON_PROFILE[person_hk]


# ============================================================
# DROP-IN FIX: FLATTEN LIST HKs (prevents ["hk1","hk2"] in CSV cells)
# ============================================================
def _as_list(x):
    return x if isinstance(x, list) else [x]


# =========================
# SATELLITE BUILDERS
# =========================

# ---------------- NATURAL PERSON (CONSISTENT) ----------------
def sat_natural_person(person_to_nat_hk, load_date):
    """
    Input can be:
      - {person_hk: natural_person_hk}
      - {person_hk: [natural_person_hk1, natural_person_hk2, ...]}
    """
    rows = []
    for person_hk, hk_or_hks in person_to_nat_hk.items():
        p = get_or_create_person_profile(person_hk)
        birth_dt = _cap_datetime_to_load(_datetime_from_date_with_random_time(p["dob"]), load_date)
        for nat_hk in _as_list(hk_or_hks):
            rows.append({
                "Natural Person Hash Key": nat_hk,
                "Load Date": load_date,
                "First Name": p["first"],
                "Last Name": p["last"],
                "Full Name": p["full_name"],
                "Courtesy Title": p["title"],
                "Occupation": p["occupation"],
                "Birth Date": birth_dt.strftime("%Y-%m-%d"),
                "Birth Year": p["birth_year"],
                "Nationality": p["nationality"],
                "Gender": p["gender"],
                "Marital Status": p["marital_status"],
                "Assesed Disability Degree": random.choice(["NONE", "LOW", "MEDIUM"]),
                "Preferred Language": p["preferred_language"],
                "Role": "CUSTOMER",
                "Job Title": p["job_title"]
            })
    return rows


# ---------------- LEGAL PERSON ----------------
def sat_legal_person(person_to_leg_hk, load_date):
    rows = []
    business_start_date = "2020-01-01"
    biz_start = _as_date(business_start_date)
    upper_dt = _coerce_datetime(load_date)
    upper_date = upper_dt.date()
    for _, hk_or_hks in person_to_leg_hk.items():
        for hk in _as_list(hk_or_hks):
            converted_dt = _cap_datetime_to_load(_rand_datetime_between(biz_start, upper_date), load_date)
            constitution_dt = _cap_datetime_to_load(
                _datetime_from_date_with_random_time(fake.date_this_decade()),
                load_date,
            )
            rows.append({
                "Legal Person Hash Key": hk,
                "Load Date": load_date,
                "Person Score": random.randint(1, 100),
                "Job Title": "",
                "Source Id": "CRM",
                "Source Type": "INTERNAL",
                "Person Status": random.choice(["NEW", "QUALIFIED"]),
                "Converted Date": converted_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "Date of Constitution": constitution_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "Company Name": fake.company()
            })
    return rows


# ---------------- PERSON (CONSISTENT TYPE) ----------------
def sat_person(person_hks, load_date, person_type, person_to_lead=None, person_to_consent=None):
    rows = []
    lead_persons = set(person_to_lead or {})
    consented_persons = set(person_to_consent or {})
    for person_hk in person_hks:
        p = get_or_create_person_profile(person_hk)
        rows.append({
            "Person Hash Key": person_hk,
            "Load Date": load_date,
            "Tenant Id": "ALLIANZ_UK",
            "Is Lead": "Y" if person_hk in lead_persons else "N",
            "Type": person_type[person_hk],
            "Operational Paperless Consent": "Y" if person_hk in consented_persons else "N",
            "Source Id": "CRM",
            "Source Type": "INTERNAL"
        })
    return rows


# ---------------- LEAD ----------------
def sat_lead(
        person_to_lead_hk,
        load_date,
        business_start_date="2020-01-01",
        as_of_date=None,
        policy_holder_persons=None,
        quote_persons=None,
        engaged_persons=None,
):
    """
    No new columns.
    Makes Converted Date realistic within a known window instead of "this decade".
    """
    rows = []
    biz_start = _as_date(business_start_date)
    upper_dt = min(_coerce_datetime(as_of_date), _coerce_datetime(load_date)) if as_of_date else _coerce_datetime(load_date)
    upper_date = upper_dt.date()
    policy_holder_persons = set(policy_holder_persons or [])
    quote_persons = set(quote_persons or [])
    engaged_persons = set(engaged_persons or [])

    for person_hk, hk_or_hks in person_to_lead_hk.items():
        for lead_hk in _as_list(hk_or_hks):
            converted_dt = _cap_datetime_to_load(_rand_datetime_between(biz_start, upper_date), load_date)
            person_score = random.randint(1, 100)

            if person_hk in policy_holder_persons or person_hk in quote_persons:
                interested_level = "HIGH"
            elif person_hk in engaged_persons:
                interested_level = "MEDIUM"
            elif person_score >= 70:
                interested_level = "MEDIUM"
            else:
                interested_level = "LOW"

            rows.append({
                "Lead Hash Key": lead_hk,
                "Load Date": load_date,
                "Interested Level": interested_level,
                "Preferred Contact Method": random.choice(["EMAIL", "SMS", "CALL"]),
                "Person Score": person_score,
                "Person Status": random.choice(["NEW", "QUALIFIED"]),
                "Converted Date": converted_dt.strftime("%Y-%m-%d %H:%M:%S")
            })
    return rows


def apply_lead_interest_levels(
        sat_lead_rows,
        person_to_lead_hk,
        policy_holder_persons=None,
        quote_persons=None,
        engaged_persons=None,
):
    policy_holder_persons = set(policy_holder_persons or [])
    quote_persons = set(quote_persons or [])
    engaged_persons = set(engaged_persons or [])

    lead_to_person = {}
    for person_hk, lead_hks in person_to_lead_hk.items():
        for lead_hk in _as_list(lead_hks):
            lead_to_person[lead_hk] = person_hk

    for row in sat_lead_rows:
        lead_hk = row.get("Lead Hash Key")
        person_hk = lead_to_person.get(lead_hk)
        if not person_hk:
            continue

        person_score = row.get("Person Score", 0)
        if person_hk in policy_holder_persons or person_hk in quote_persons:
            interested_level = "HIGH"
        elif person_hk in engaged_persons:
            interested_level = "MEDIUM"
        elif person_score >= 70:
            interested_level = "MEDIUM"
        else:
            interested_level = "LOW"

        row["Interested Level"] = interested_level

    return sat_lead_rows


# ---------------- CUSTOMER ----------------
def sat_customer(
        person_to_customer_hk,
        load_date,
        business_start_date=datetime.combine(date(2020, 1, 1), datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S"),
        as_of_date=None,
        earliest_policy_start_by_person=None
):
    """
    No new columns.
    If earliest_policy_start_by_person is provided (person_hk -> 'YYYY-MM-DD' or date),
    Customer Since will be <= earliest policy start for that person (more realistic).
    Otherwise it falls back to a general random date in [business_start..as_of-30d].
    """
    rows = []
    biz_start = _as_date(business_start_date)
    load_dt = _coerce_datetime(load_date)
    as_of_dt = min(_coerce_datetime(as_of_date), load_dt) if as_of_date else load_dt
    as_of = as_of_dt.date()

    for person_hk, hk_or_hks in person_to_customer_hk.items():
        # default upper bound
        upper = as_of - timedelta(days=30)

        if earliest_policy_start_by_person and person_hk in earliest_policy_start_by_person:
            eps = _as_date(earliest_policy_start_by_person[person_hk])
            # ensure customer_since is not after earliest policy start
            upper = min(upper, eps)

        # if upper ends up earlier than biz_start, clamp to biz_start
        if upper < biz_start:
            upper = biz_start

        customer_since = _cap_datetime_to_load(_rand_datetime_between(biz_start, upper), load_date)

        for hk in _as_list(hk_or_hks):
            rows.append({
                "Customer Hash Key": hk,
                "Load Date": load_date,
                "Customer Number": random.randint(100000, 999999),
                "Customer Status": random.choice(["ACTIVE", "LAPSED"]),
                "Customer Status Reason": random.choice(["RENEWAL", "PAYMENT", "CUSTOMER_REQUEST"]),
                "Customer Since": customer_since.strftime("%Y-%m-%d %H:%M:%S"),
                "Customer Rating": 3,
                "Customer Segment": "STANDARD",
                "Line Of Business": random.choice(["MOTOR", "HOME"]),
                "NPS Score": random.randint(0, 10)
            })
    return rows


def apply_customer_segments(
        sat_customer_rows,
        person_to_customer_hk,
        person_to_account_hk=None,
        sat_policy_rows=None,
        sat_account_rows=None,
        policy_to_person_map=None,
):
    sat_policy_rows = sat_policy_rows or []
    sat_account_rows = sat_account_rows or []
    policy_to_person_map = policy_to_person_map or {}
    person_to_account_hk = person_to_account_hk or {}

    customer_to_person = {}
    for person_hk, customer_hk_or_hks in person_to_customer_hk.items():
        for customer_hk in _as_list(customer_hk_or_hks):
            customer_to_person[customer_hk] = person_hk

    account_to_person = {}
    for person_hk, account_hk_or_hks in person_to_account_hk.items():
        for account_hk in _as_list(account_hk_or_hks):
            account_to_person[account_hk] = person_hk

    person_account_status = {}
    for row in sat_account_rows:
        account_hk = row.get("Account Hash Key")
        account_status = row.get("Account Status")
        if not account_hk or not account_status:
            continue
        person_hk = account_to_person.get(account_hk)
        if person_hk:
            person_account_status[person_hk] = account_status

    person_policy_summary = {}
    for row in sat_policy_rows:
        policy_hk = row.get("Policy Hash Key")
        person_hk = policy_to_person_map.get(policy_hk)
        if not person_hk:
            continue

        summary = person_policy_summary.setdefault(person_hk, {
            "has_active": False,
            "has_lapsed": False,
            "has_cancelled": False,
            "fraud_flag": False,
            "max_revenue": 0.0,
        })

        status = row.get("Policy Status")
        if status == "ACTIVE":
            summary["has_active"] = True
        elif status == "LAPSED":
            summary["has_lapsed"] = True
        elif status == "CANCELLED":
            summary["has_cancelled"] = True

        summary["fraud_flag"] = summary["fraud_flag"] or (row.get("Fraud Flag") == "Y")
        summary["max_revenue"] = max(summary["max_revenue"], float(row.get("Gross Revenue", 0) or 0))

    for row in sat_customer_rows:
        customer_hk = row.get("Customer Hash Key")
        person_hk = customer_to_person.get(customer_hk)
        if not person_hk:
            row["Customer Segment"] = "STANDARD"
            continue

        segment_score = 0
        account_status = person_account_status.get(person_hk)
        policy_summary = person_policy_summary.get(person_hk, {})
        nps_score = int(row.get("NPS Score", 0))

        if policy_summary.get("has_active"):
            segment_score += 1
        if account_status == "OPEN":
            segment_score += 1
        if nps_score >= 8:
            segment_score += 1
        if policy_summary.get("max_revenue", 0) >= 1200:
            segment_score += 1
        if policy_summary.get("fraud_flag"):
            segment_score -= 2
        if policy_summary.get("has_cancelled"):
            segment_score -= 1
        elif policy_summary.get("has_lapsed"):
            segment_score -= 1

        row["Customer Segment"] = "PREMIUM" if segment_score >= 3 else "STANDARD"

    return sat_customer_rows


def apply_customer_ratings(
        sat_customer_rows,
        person_to_customer_hk,
        person_to_account_hk=None,
        sat_policy_rows=None,
        sat_account_rows=None,
        policy_to_person_map=None,
):
    sat_policy_rows = sat_policy_rows or []
    sat_account_rows = sat_account_rows or []
    policy_to_person_map = policy_to_person_map or {}
    person_to_account_hk = person_to_account_hk or {}

    customer_to_person = {}
    for person_hk, customer_hk_or_hks in person_to_customer_hk.items():
        for customer_hk in _as_list(customer_hk_or_hks):
            customer_to_person[customer_hk] = person_hk

    person_policy_summary = {}
    for row in sat_policy_rows:
        policy_hk = row.get("Policy Hash Key")
        person_hk = policy_to_person_map.get(policy_hk)
        if not person_hk:
            continue

        summary = person_policy_summary.setdefault(person_hk, {
            "has_active": False,
            "has_lapsed": False,
            "has_cancelled": False,
            "fraud_flag": False,
            "declined_claims": 0,
        })

        status = row.get("Policy Status")
        if status == "ACTIVE":
            summary["has_active"] = True
        elif status == "LAPSED":
            summary["has_lapsed"] = True
        elif status == "CANCELLED":
            summary["has_cancelled"] = True

        summary["fraud_flag"] = summary["fraud_flag"] or (row.get("Fraud Flag") == "Y")
        summary["declined_claims"] = max(summary["declined_claims"], int(row.get("Declined Claims", 0)))

    account_to_person = {}
    for person_hk, account_hk_or_hks in person_to_account_hk.items():
        for account_hk in _as_list(account_hk_or_hks):
            account_to_person[account_hk] = person_hk

    person_account_status = {}
    for row in sat_account_rows:
        account_hk = row.get("Account Hash Key")
        account_status = row.get("Account Status")
        if not account_hk or not account_status:
            continue
        person_hk = account_to_person.get(account_hk)
        if person_hk:
            person_account_status[person_hk] = account_status

    for row in sat_customer_rows:
        customer_hk = row.get("Customer Hash Key")
        person_hk = customer_to_person.get(customer_hk)
        if not person_hk:
            row["Customer Rating"] = max(1, min(5, int(row.get("Customer Rating", 3))))
            continue

        score = 3
        customer_status = row.get("Customer Status")
        customer_segment = row.get("Customer Segment")
        nps_score = int(row.get("NPS Score", 0))

        policy_summary = person_policy_summary.get(person_hk, {})
        if customer_status == "ACTIVE":
            score += 1
        if customer_segment == "PREMIUM":
            score += 1
        if nps_score >= 8:
            score += 1
        elif nps_score <= 3:
            score -= 1

        if policy_summary.get("has_active"):
            score += 1
        if policy_summary.get("has_lapsed"):
            score -= 1
        if policy_summary.get("has_cancelled"):
            score -= 2
        account_status = person_account_status.get(person_hk)
        if account_status == "OPEN":
            score += 1
        elif account_status == "SUSPENDED":
            score -= 1
        elif account_status == "CLOSED":
            score -= 2
        if policy_summary.get("fraud_flag"):
            score -= 2
        if policy_summary.get("declined_claims", 0) > 0:
            score -= 1

        row["Customer Rating"] = max(1, min(5, score))

    return sat_customer_rows


# ---------------- IDENTITIES (CONSISTENT HASHED EMAIL) ----------------
def sat_identities(person_to_identity_hk, load_date):
    rows = []
    email_to_ecid = {}
    for person_hk, hk_or_hks in person_to_identity_hk.items():
        p = get_or_create_person_profile(person_hk)
        hashed_email = p["hashed_email"]
        if hashed_email not in email_to_ecid:
            email_to_ecid[hashed_email] = f"ECID-{random.randint(10 ** 9, 10 ** 10 - 1)}"
        ecid = email_to_ecid[hashed_email]
        for hk in _as_list(hk_or_hks):
            rows.append({
                "Identities Hash Key": hk,
                "Load Date": load_date,
                "ECID": ecid,
                "Hashed Email": hashed_email
            })
    return rows


# ---------------- CONTACT (CONSISTENT EMAILS) ----------------
def sat_contact(person_to_contact_hk, load_date):
    rows = []
    for person_hk, hk_or_hks in person_to_contact_hk.items():
        p = get_or_create_person_profile(person_hk)
        for hk in _as_list(hk_or_hks):
            rows.append({
                "Contact Hash Key": hk,
                "Load Date": load_date,
                "Personal Email": p["personal_email"],
                "Work Email": p["work_email"],
                "Work Phone": fake.phone_number(),
                "Home Phone": fake.phone_number()
            })
    return rows


# ---------------- CONSENT ----------------
def sat_consent(person_to_consent_hk, load_date):
    rows = []
    for _, hk_or_hks in person_to_consent_hk.items():
        for hk in _as_list(hk_or_hks):
            rows.append({
                "Consent Hash Key": hk,
                "Load Date": load_date,
                "Opt In Validated": random.choice(["Y", "N"]),
                "Opt In Legitimate Interest": random.choice(["Y", "N"])
            })
    return rows


# ---------------- ACCOUNT ----------------
def sat_account(person_to_account_hk, load_date):
    rows = []
    load_dt = _coerce_datetime(load_date)
    for _, hk_or_hks in person_to_account_hk.items():
        for hk in _as_list(hk_or_hks):
            account_status = random.choices(
                ["OPEN", "SUSPENDED", "CLOSED"],
                weights=[0.70, 0.15, 0.15],
            )[0]

            created_anchor = load_dt - timedelta(days=random.randint(30, 365 * 3))
            last_change_dt = created_anchor + timedelta(days=random.randint(0, max(1, (load_dt - created_anchor).days)))
            last_change_dt = min(last_change_dt, load_dt)

            if account_status == "OPEN":
                access_start = last_change_dt
                access_end = load_dt
            else:
                access_start = created_anchor
                access_end = last_change_dt

            if access_end < access_start:
                access_end = access_start

            span_seconds = int((access_end - access_start).total_seconds()) if access_end > access_start else 0
            last_access_dt = access_start + timedelta(seconds=random.randint(0, span_seconds)) if span_seconds else access_start

            rows.append({
                "Account Hash Key": hk,
                "Load Date": load_date,
                "Account Number": random.randint(1000000, 9999999),
                "Account Type": random.choice(["PERSONAL", "BUSINESS"]),
                "Account Last Access": last_access_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "Account Last Change": last_change_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "Account Creation Type": random.choice(["ONLINE", "BRANCH"]),
                "Account Status": account_status
            })
    return rows


# ---------------- MARKETING PREFERENCE ----------------
def sat_marketing_preference(person_to_mpr_hk, load_date):
    rows = []
    for _, hk_or_hks in person_to_mpr_hk.items():
        for hk in _as_list(hk_or_hks):
            any_col =  random.choice(["Y", "N"])
            if any_col == 'Y':
                choosed_value = any_col
            else:
                choosed_value = random.choice(["Y", "N"])
                
            rows.append({
                "Marketing Preference Hash Key": hk,
                "Load Date": load_date,
                "SMS": choosed_value,
                "Email": choosed_value,
                "Email Subscriptions": choosed_value,
                "Call": choosed_value,
                "Any": any_col,
                "Commercial Email": choosed_value,
                "Postal Mail": choosed_value
            })
    return rows


# ---------------- MARKETING ENGAGEMENT ----------------
def sat_marketing_engagement(person_to_men_hk, load_date):
    rows = []
    for _, hk_or_hks in person_to_men_hk.items():
        for hk in _as_list(hk_or_hks):
            rows.append({
                "Marketing Engagement Hash Key": hk,
                "Load Date": load_date,
                "Promotion Code": random.choice(["SPRING", "WELCOME", "RENEWAL"]),
                "Opened Email": random.choice(["Y", "N"]),
                "Marketing Status": random.choice(["ACTIVE", "INACTIVE"])
            })
    return rows


# ---------------- QUOTE ----------------
def sat_quote(person_to_quote_hk, load_date):
    rows = []
    for _, hk_or_hks in person_to_quote_hk.items():
        for hk in _as_list(hk_or_hks):
            rows.append({
                "Quote Hash Key": hk,
                "Load Date": load_date,
                "Gross Revenue": round(random.uniform(200, 2000), 2),
                "Net Revenue": round(random.uniform(150, 1800), 2),
                "Quote Number": random.randint(100000, 999999),
                "Quote Status": random.choice(["CREATED", "SENT", "EXPIRED"]),
                "Renewal Amt Current Period": round(random.uniform(100, 1000), 2),
                "Renewal Amt Next Period": round(random.uniform(100, 1000), 2)
            })
    return rows


# ---------------- POLICY ----------------
def sat_policy(
        policy_hks,
        load_date,
        business_start_date="2020-01-01",
        as_of_date=None,
        policy_to_person_map=None,
        latest_lead_converted_by_person=None,
        person_account_status_by_person=None,
):
    """
    Drop-in replacement.
    NO new columns.
    Fixes these issues:
      - Policy End Date can no longer be before Policy Start Date
      - Renewal Date aligned to end date
      - Policy Status consistent with dates (ACTIVE/LAPSED/CANCELLED)
      - Policy Length is actually used (treated as months)
    """
    rows = []
    biz_start = _as_date(business_start_date)
    load_dt = _coerce_datetime(load_date)
    as_of_dt = _coerce_datetime(as_of_date) if as_of_date else datetime.now().replace(microsecond=0)
    as_of_dt = min(as_of_dt, load_dt)

    for hk in policy_hks:
        policy_start = None
        person_hk = policy_to_person_map.get(hk) if policy_to_person_map else None
        if policy_to_person_map and latest_lead_converted_by_person:
            if person_hk and person_hk in latest_lead_converted_by_person:
                lead_converted_dt = _coerce_datetime(latest_lead_converted_by_person[person_hk])
                candidate_start = lead_converted_dt + timedelta(days=random.randint(1, 90))
                policy_start = min(candidate_start, load_dt)

        if policy_start is None:
            fallback_start = _rand_datetime_between(biz_start, as_of_dt.date())
            policy_start = min(fallback_start, load_dt)

        policy_length_months = 12
        planned_end = _add_one_year(policy_start)

        account_status = person_account_status_by_person.get(person_hk) if person_account_status_by_person and person_hk else None
        is_cancelled = account_status in {"SUSPENDED", "CLOSED"} or (random.random() < 0.10)
        if is_cancelled:
            status = "CANCELLED"
            min_end = policy_start + timedelta(days=7)
            max_end = min(planned_end - timedelta(days=1), load_dt)
            if max_end >= min_end:
                span_seconds = int((max_end - min_end).total_seconds())
                policy_end = min_end + timedelta(seconds=random.randint(0, span_seconds))
            else:
                policy_end = min_end
        else:
            policy_end = planned_end
            status = "ACTIVE" if policy_end > as_of_dt else "LAPSED"

        renewal_date = policy_end - timedelta(days=random.randint(0, 10))

        renewal_current = round(random.uniform(200, 1500), 2)
        renewal_next = round(renewal_current * 1.01, 2)
        declined_claims = random.randint(0, 3)
        active_claims = random.randint(0, 2)
        previous_claims = random.randint(0, 5)
        gross_revenue = round(random.uniform(300, 2500), 2)
        net_revenue = round(random.uniform(200, 2000), 2)
        sales_channel = random.choice(["ONLINE", "AGENT", "BRANCH"])

        fraud_risk_score = 0
        if declined_claims > 0:
            fraud_risk_score += 2
        if previous_claims >= 3:
            fraud_risk_score += 1
        if active_claims >= 2:
            fraud_risk_score += 1
        if status == "CANCELLED":
            fraud_risk_score += 2
        elif status == "LAPSED":
            fraud_risk_score += 1
        if account_status == "SUSPENDED":
            fraud_risk_score += 2
        elif account_status == "CLOSED":
            fraud_risk_score += 3

        fraud_flag = "Y" if fraud_risk_score >= 3 else "N"

        rows.append({
            "Policy Hash Key": hk,
            "Load Date": load_date,
            "Cover Option": random.choice(["BASIC", "FULL"]),
            "Declined Claims": declined_claims,
            "Fraud Flag": fraud_flag,
            "Gross Revenue": gross_revenue,
            "Net Revenue": net_revenue,
            "Number of Active Claim": active_claims,
            "Number of Previous Claim": previous_claims,
            "Policy Cicle": random.randint(1, 5),
            "Policy End Date": policy_end.strftime("%Y-%m-%d %H:%M:%S"),
            "Policy Length": policy_length_months,
            "Policy Number": random.randint(1000000, 9999999),
            "Policy Start Date": policy_start.strftime("%Y-%m-%d %H:%M:%S"),
            "Policy Status": status,
            "Renewal Amount Current Period": renewal_current,
            "Renewal Amount Next Period": renewal_next,
            "Renewal Date": renewal_date.strftime("%Y-%m-%d %H:%M:%S"),
            "Sales Channel": sales_channel
        })
    return rows


# ---------------- HOME ADDRESS ----------------
def sat_home_address(addr_hks, load_date):
    """
    Generates Sat_Home_Address rows and caches each address by its HK.
    This is the authoritative source for City/Postcode/State for Motor/Home.
    """
    rows = []
    for hk in addr_hks:
        state = _pick_uk_region()

        row = {
            "Home Address Hash Key": hk,
            "Load Date": load_date,
            "Street": fake.street_address().replace('\n', " "),
            "Postcode": fake.postcode(),
            "City": fake.city(),
            "State": state,
            "Country": "UK"
        }
        rows.append(row)

        HOME_ADDRESS_CACHE[hk] = {
            "Street": row["Street"],
            "City": row["City"],
            "Postcode": row["Postcode"],
            "State": row["State"],
            "Country": row["Country"],
        }

    return rows


# ---------------- HOME ----------------
def sat_home(home_hks, load_date, home_to_addr: dict | None = None):
    rows = []
    for home_hk in home_hks:
        addr_hk = home_to_addr.get(home_hk) if home_to_addr else None
        addr = HOME_ADDRESS_CACHE.get(addr_hk) if addr_hk else None

        if not addr:
            addr = _address_from_cache_by_hk(home_hk)

        rows.append({
            "Home Hash Key": home_hk,
            "Load Date": load_date,
            "Wall Construction": random.choice(["BRICK", "CONCRETE"]),
            "Home Risk Address": f'{addr["City"]}, {addr["Postcode"]}',
            "Roof Construction": random.choice(["TILE", "METAL"]),
            "Home Type": random.choice(["HOUSE", "FLAT"]),
            "Home State": addr["State"],
            "Is Existing Home Customer": random.choice(["Y", "N"])
        })
    return rows


# ---------------- MOTOR ----------------
def sat_motor(motor_hks, load_date, motor_to_addr: dict | None = None):
    rows = []
    for motor_hk in motor_hks:
        year = random.randint(2005, 2025)

        addr_hk = motor_to_addr.get(motor_hk) if motor_to_addr else None
        addr = HOME_ADDRESS_CACHE.get(addr_hk) if addr_hk else None

        if not addr:
            addr = _address_from_cache_by_hk(motor_hk)

        rows.append({
            "Motor Hash Key": motor_hk,
            "Load Date": load_date,
            "Auto Decline Vehicle": random.choice(["Y", "N"]),
            "Body Type": random.choice(["SEDAN", "SUV", "HATCH"]),
            "Fuel Type": random.choice(["PETROL", "DIESEL", "EV"]),
            "License Status": random.choice(["VALID", "EXPIRED"]),
            "Is Existing Motor Customer": random.choice(["Y", "N"]),
            "Motor Lapsed Policies": random.randint(0, 2),
            "Motor Risk Address": f'{addr["City"]}, {addr["Postcode"]}',
            "Risk Class Code": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "Variant": random.choice(["BASE", "SPORT"]),
            "Vehicle Owner Type": random.choice(["SELF", "COMPANY"]),
            "Vehicle RegState": addr["State"],
            "Vehicle Class": random.choice(["PRIVATE", "COMMERCIAL"]),
            "Vehicle Model": random.choice(["Focus", "Corsa", "3 Series", "A3", "Corolla", "Qashqai"]),
            "Vehicle Type": random.choice(["CAR", "BIKE"]),
            "Motor Sum Insrd": round(random.uniform(2000, 20000), 2),
            "Vehicle Year": year,
            "Vehicle Age": 2025 - year
        })
    return rows


# ---------------- PRODUCT ----------------
def sat_product(product_rows, load_date, product_code_by_hk=None):
    rows = []
    product_code_by_hk = product_code_by_hk or {}
    for r0 in product_rows:
        product_hk = r0["Product Hash Key"]
        rows.append({
            "Product Hash Key": product_hk,
            "Load Date": load_date,
            "Type": product_code_by_hk.get(product_hk, r0["Product Id"])
        })
    return rows
