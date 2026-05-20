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


def _add_years(dt: datetime, years: int) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        last_day = calendar.monthrange(dt.year + years, dt.month)[1]
        return dt.replace(year=dt.year + years, day=min(dt.day, last_day))


def _weighted_choice_from_mapping(weight_map: dict | None, default_map: dict) -> str:
    weights_by_value = weight_map or default_map
    values = list(weights_by_value.keys())
    weights = [float(weights_by_value[value]) for value in values]
    return random.choices(values, weights=weights)[0]


def _churn_settings(churn_config: dict | None) -> dict:
    return churn_config or {}


def _sample_renewal_premiums(churn_config: dict | None = None) -> tuple[float, float]:
    churn_cfg = _churn_settings(churn_config)
    current_band = _weighted_choice_from_mapping(
        churn_cfg.get("renewal_current_premium_band_weights"),
        {"LOW": 35, "MEDIUM": 35, "HIGH": 20, "VERY_HIGH": 10},
    )
    current_ranges = {
        "LOW": (300, 600),
        "MEDIUM": (601, 900),
        "HIGH": (901, 1200),
        "VERY_HIGH": (1201, 1800),
    }
    low, high = current_ranges[current_band]
    current = round(random.uniform(low, high), 2)

    increase_band = _weighted_choice_from_mapping(
        churn_cfg.get("renewal_movement_band_weights"),
        {"DECREASE": 10, "SMALL": 20, "MEDIUM": 30, "LARGE": 40},
    )
    increase_band_aliases = {
        "0_5": "SMALL",
        "5_10": "MEDIUM",
        "GT_10": "LARGE",
    }
    increase_band = increase_band_aliases.get(increase_band, increase_band)
    rate_ranges = {
        "DECREASE": (-0.08, -0.01),
        "SMALL": (0.00, 0.05),
        "MEDIUM": (0.05, 0.10),
        "LARGE": (0.10, 0.35),
    }
    rate_low, rate_high = rate_ranges[increase_band]
    next_premium = round(current * (1 + random.uniform(rate_low, rate_high)), 2)
    return current, max(0.0, next_premium)


def _sample_policy_claim_counts(churn_config: dict | None = None) -> tuple[int, int, int]:
    churn_cfg = _churn_settings(churn_config)
    total_claims = int(_weighted_choice_from_mapping(
        churn_cfg.get("claim_count_weights"),
        {"0": 18, "1": 25, "2": 25, "3": 18, "4": 9, "5": 5},
    ))
    if total_claims == 0:
        return 0, 0, 0

    active_claims = min(total_claims, int(_weighted_choice_from_mapping(
        churn_cfg.get("active_claim_count_weights"),
        {"0": 45, "1": 40, "2": 15},
    )))
    remaining = total_claims - active_claims
    declined_claims = min(remaining, int(_weighted_choice_from_mapping(
        churn_cfg.get("declined_claim_count_weights"),
        {"0": 70, "1": 25, "2": 5},
    )))
    previous_claims = max(0, total_claims - active_claims - declined_claims)
    return declined_claims, active_claims, previous_claims


def _policy_cycle_from_dates(policy_start: datetime, as_of_dt: datetime) -> int:
    days_active = max(0, (as_of_dt.date() - policy_start.date()).days)
    return days_active // 365


def _sample_policy_cover_option(churn_config: dict | None = None) -> str:
    return _weighted_choice_from_mapping(
        _churn_settings(churn_config).get("cover_option_weights"),
        {"BASE_ONLY": 35, "ONE_ADD_ON": 30, "TWO_ADD_ONS": 22, "THREE_PLUS_ADD_ONS": 13},
    )


def _sample_sales_channel_for_status(policy_status: str, churn_config: dict | None = None) -> str:
    channel_weights = _churn_settings(churn_config).get("sales_channel_by_policy_status") or {
        "ACTIVE": {"ONLINE": 50, "BRANCH": 38, "AGENT": 12},
        "LAPSED": {"ONLINE": 22, "BRANCH": 18, "AGENT": 60},
        "CANCELLED": {"ONLINE": 10, "BRANCH": 8, "AGENT": 82},
    }
    return _weighted_choice_from_mapping(
        channel_weights.get(policy_status),
        {"ONLINE": 35, "BRANCH": 30, "AGENT": 35},
    )


def _tenure_churn_probability(policy_cycle: int, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("tenure_churn_probability") or {}
    if policy_cycle < 1:
        return float(probabilities.get("LT_1", 0.58))
    if policy_cycle <= 2:
        return float(probabilities.get("Y1_2", 0.16))
    if policy_cycle <= 5:
        return float(probabilities.get("Y3_5", 0.04))
    return float(probabilities.get("GT_5", 0.0))


def _sample_policy_status_for_tenure(policy_cycle: int, account_status: str | None, churn_config: dict | None = None) -> str:
    churn_cfg = _churn_settings(churn_config)
    if account_status == "CLOSED":
        return "CANCELLED"
    if account_status == "SUSPENDED":
        if policy_cycle < 1:
            return "CANCELLED"
        return _weighted_choice_from_mapping(
            churn_cfg.get("suspended_policy_status_weights"),
            {"LAPSED": 65, "CANCELLED": 35},
        )

    if random.random() >= _tenure_churn_probability(policy_cycle, churn_cfg):
        return "ACTIVE"

    if policy_cycle < 1:
        return "CANCELLED"
    return _weighted_choice_from_mapping(
        churn_cfg.get("churned_policy_status_weights"),
        {"LAPSED": 70, "CANCELLED": 30},
    )


def _sample_vehicle_profile(churn_config: dict | None = None) -> tuple[str, str, str, str]:
    segment = _weighted_choice_from_mapping(
        _churn_settings(churn_config).get("vehicle_segment_weights"),
        {"STANDARD": 65, "PREMIUM": 25, "HIGH_RISK": 10},
    )
    profiles = {
        "STANDARD": [
            ("SEDAN", "PRIVATE", "Focus", "CAR"),
            ("HATCH", "PRIVATE", "Corsa", "CAR"),
            ("SEDAN", "PRIVATE", "Corolla", "CAR"),
        ],
        "PREMIUM": [
            ("SUV", "PRIVATE", "Qashqai", "CAR"),
            ("SEDAN", "PRIVATE", "3 Series", "CAR"),
            ("HATCH", "PRIVATE", "A3", "CAR"),
        ],
        "HIGH_RISK": [
            ("BIKE", "PRIVATE", "Sport Bike", "BIKE"),
            ("BIKE", "PRIVATE", "Superbike", "BIKE"),
            ("SUV", "COMMERCIAL", "High Performance SUV", "CAR"),
        ],
    }
    return random.choice(profiles[segment])


def _sample_marketing_preference_flags(churn_config: dict | None = None) -> dict[str, str]:
    churn_cfg = _churn_settings(churn_config)
    engagement_band = _weighted_choice_from_mapping(
        churn_cfg.get("marketing_engagement_band_weights"),
        {"HIGH": 15, "MEDIUM": 30, "LOW": 35, "NONE": 20},
    )
    channels = ["SMS", "Email", "Email Subscriptions", "Commercial Email", "Postal Mail"]
    selected_count = {
        "HIGH": random.randint(4, 5),
        "MEDIUM": random.randint(2, 3),
        "LOW": 1,
        "NONE": 0,
    }[engagement_band]
    selected = set(random.sample(channels, selected_count)) if selected_count else set()
    service_call_band = _weighted_choice_from_mapping(
        churn_cfg.get("service_call_band_weights"),
        {"NONE": 45, "LOW": 30, "MEDIUM": 18, "HIGH": 7},
    )
    flags = {channel: "Y" if channel in selected else "N" for channel in channels}
    flags["Call"] = "Y" if service_call_band != "NONE" else "N"
    flags["Any"] = "Y" if any(value == "Y" for value in flags.values()) else "N"
    return flags


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


def _birth_date_for_age_range(rng: random.Random, reference_date, minimum_age: int, maximum_age: int):
    ref_dt = _coerce_datetime(reference_date) if reference_date else datetime.now().replace(microsecond=0)
    latest_birth = _add_years(ref_dt, -minimum_age).date()
    earliest_birth = _add_years(ref_dt, -maximum_age).date()
    return fake.date_between(start_date=earliest_birth, end_date=latest_birth)


def _birth_date_for_driver_experience_proxy(rng: random.Random, reference_date):
    ref_dt = _coerce_datetime(reference_date) if reference_date else datetime.now().replace(microsecond=0)
    band = rng.choices(
        ["LT_2Y", "Y2_5", "Y6_10", "GT_10"],
        weights=[12, 20, 25, 43],
    )[0]
    age_ranges = {
        "LT_2Y": (18, 19),
        "Y2_5": (20, 22),
        "Y6_10": (23, 27),
        "GT_10": (28, 85),
    }
    minimum_age, maximum_age = age_ranges[band]
    latest_birth = _add_years(ref_dt, -minimum_age).date()
    earliest_birth = _add_years(ref_dt, -maximum_age).date()
    return fake.date_between(start_date=earliest_birth, end_date=latest_birth)


def get_or_create_person_profile(person_hk: str, reference_date=None) -> dict:
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

    dob = _birth_date_for_driver_experience_proxy(rng, reference_date)
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
        p = get_or_create_person_profile(person_hk, load_date)
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
            # Bias lead conversions toward recent history so downstream policy
            # timelines produce a more realistic active/lapsed portfolio mix.
            lead_age_roll = random.random()
            if lead_age_roll < 0.2:
                recent_start = max(biz_start, upper_date - timedelta(days=365))
                converted_dt = _cap_datetime_to_load(_rand_datetime_between(recent_start, upper_date), load_date)
            elif lead_age_roll < 0.45:
                historical_start = upper_date - timedelta(days=365 * 7)
                historical_end = min(upper_date, upper_date - timedelta(days=365 * 6))
                converted_dt = _cap_datetime_to_load(_rand_datetime_between(historical_start, historical_end), load_date)
            else:
                converted_dt = _cap_datetime_to_load(_rand_datetime_between(biz_start, upper_date), load_date)
            person_score = random.randint(1, 100)
            interest_signal = person_score
            if person_hk in quote_persons:
                interest_signal += 20
            if person_hk in policy_holder_persons:
                interest_signal += 10
            if person_hk in engaged_persons:
                interest_signal += 5

            if interest_signal >= 85:
                interested_level = "HIGH"
            elif interest_signal >= 55:
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

        person_score = int(row.get("Person Score", 0) or 0)
        interest_signal = person_score
        if person_hk in quote_persons:
            interest_signal += 20
        if person_hk in policy_holder_persons:
            interest_signal += 10
        if person_hk in engaged_persons:
            interest_signal += 5

        if interest_signal >= 85:
            interested_level = "HIGH"
        elif interest_signal >= 55:
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
        earliest_policy_start_by_person=None,
        churn_config=None,
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
                "Customer Status": _weighted_choice_from_mapping(
                    _churn_settings(churn_config).get("customer_status_weights"),
                    {"ACTIVE": 80, "LAPSED": 20},
                ),
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
            segment_score += 2
        if account_status == "OPEN":
            segment_score += 1
        if nps_score >= 9:
            segment_score += 1
        if policy_summary.get("max_revenue", 0) >= 1800:
            segment_score += 1
        if policy_summary.get("fraud_flag"):
            segment_score -= 2
        if policy_summary.get("has_cancelled"):
            segment_score -= 1
        elif policy_summary.get("has_lapsed"):
            segment_score -= 1

        row["Customer Segment"] = "PREMIUM" if segment_score >= 4 else "STANDARD"

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

        score = 2
        customer_status = row.get("Customer Status")
        customer_segment = row.get("Customer Segment")
        nps_score = int(row.get("NPS Score", 0))

        policy_summary = person_policy_summary.get(person_hk, {})
        if customer_status == "ACTIVE":
            score += 1
        if customer_segment == "PREMIUM":
            score += 1
        if nps_score >= 9:
            score += 1
        elif nps_score <= 1:
            score -= 1

        if policy_summary.get("has_active"):
            score += 1
        if policy_summary.get("has_lapsed"):
            score -= 1
        if policy_summary.get("has_cancelled"):
            score -= 1
        account_status = person_account_status.get(person_hk)
        if account_status == "OPEN":
            score += 1
        elif account_status == "SUSPENDED":
            score -= 1
        elif account_status == "CLOSED":
            score -= 2
        if policy_summary.get("fraud_flag"):
            score -= 1
        if policy_summary.get("declined_claims", 0) > 1:
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
def sat_account(person_to_account_hk, load_date, churn_config=None):
    rows = []
    load_dt = _coerce_datetime(load_date)
    for _, hk_or_hks in person_to_account_hk.items():
        for hk in _as_list(hk_or_hks):
            account_status = _weighted_choice_from_mapping(
                _churn_settings(churn_config).get("account_status_weights"),
                {"OPEN": 96, "SUSPENDED": 3, "CLOSED": 1},
            )

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
def sat_marketing_preference(person_to_mpr_hk, load_date, churn_config=None):
    rows = []
    for _, hk_or_hks in person_to_mpr_hk.items():
        for hk in _as_list(hk_or_hks):
            flags = _sample_marketing_preference_flags(churn_config)
                
            rows.append({
                "Marketing Preference Hash Key": hk,
                "Load Date": load_date,
                "SMS": flags["SMS"],
                "Email": flags["Email"],
                "Email Subscriptions": flags["Email Subscriptions"],
                "Call": flags["Call"],
                "Any": flags["Any"],
                "Commercial Email": flags["Commercial Email"],
                "Postal Mail": flags["Postal Mail"]
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
        churn_config=None,
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
    as_of_dt = _coerce_datetime(as_of_date) if as_of_date else load_dt
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

        account_status = person_account_status_by_person.get(person_hk) if person_account_status_by_person and person_hk else None
        policy_length_months = 12
        policy_cycle = _policy_cycle_from_dates(policy_start, as_of_dt)
        current_term_start = _add_years(policy_start, policy_cycle)
        current_term_end = _add_years(policy_start, policy_cycle + 1)
        status = _sample_policy_status_for_tenure(policy_cycle, account_status, churn_config)

        if status == "LAPSED" and current_term_start > load_dt:
            status = "CANCELLED"

        if status == "ACTIVE":
            policy_end = current_term_end
        elif status == "LAPSED":
            policy_end = current_term_start if policy_cycle > 0 else min(as_of_dt, load_dt)
        else:
            min_end = max(policy_start + timedelta(days=7), current_term_start)
            max_end = min(current_term_end - timedelta(days=1), as_of_dt, load_dt)
            if max_end >= min_end:
                span_seconds = int((max_end - min_end).total_seconds())
                policy_end = min_end + timedelta(seconds=random.randint(0, span_seconds))
            else:
                policy_end = min(policy_start + timedelta(days=7), load_dt)

        renewal_date = policy_end - timedelta(days=random.randint(0, 10))

        renewal_current, renewal_next = _sample_renewal_premiums(churn_config)
        declined_claims, active_claims, previous_claims = _sample_policy_claim_counts(churn_config)
        gross_revenue = round(random.uniform(300, 2500), 2)
        net_revenue = round(random.uniform(200, 2000), 2)
        sales_channel = _sample_sales_channel_for_status(status, churn_config)

        fraud_risk_score = 0
        if declined_claims >= 2:
            fraud_risk_score += 2
        if previous_claims >= 4:
            fraud_risk_score += 1
        if active_claims >= 2:
            fraud_risk_score += 1
        if status == "CANCELLED":
            fraud_risk_score += 1
        elif status == "LAPSED":
            fraud_risk_score += 0
        if account_status == "SUSPENDED":
            fraud_risk_score += 1
        elif account_status == "CLOSED":
            fraud_risk_score += 2

        fraud_flag = "Y" if fraud_risk_score >= 4 else "N"

        rows.append({
            "Policy Hash Key": hk,
            "Load Date": load_date,
            "Cover Option": _sample_policy_cover_option(churn_config),
            "Declined Claims": declined_claims,
            "Fraud Flag": fraud_flag,
            "Gross Revenue": gross_revenue,
            "Net Revenue": net_revenue,
            "Number of Active Claim": active_claims,
            "Number of Previous Claim": previous_claims,
            "Policy Cycle": policy_cycle,
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
def sat_motor(motor_hks, load_date, motor_to_addr: dict | None = None, churn_config=None):
    rows = []
    for motor_hk in motor_hks:
        year = random.randint(2005, 2025)

        addr_hk = motor_to_addr.get(motor_hk) if motor_to_addr else None
        addr = HOME_ADDRESS_CACHE.get(addr_hk) if addr_hk else None

        if not addr:
            addr = _address_from_cache_by_hk(motor_hk)

        body_type, vehicle_class, vehicle_model, vehicle_type = _sample_vehicle_profile(churn_config)

        rows.append({
            "Motor Hash Key": motor_hk,
            "Load Date": load_date,
            "Auto Decline Vehicle": random.choice(["Y", "N"]),
            "Body Type": body_type,
            "Fuel Type": random.choice(["PETROL", "DIESEL", "EV"]),
            "License Status": random.choice(["VALID", "EXPIRED"]),
            "Is Existing Motor Customer": random.choice(["Y", "N"]),
            "Motor Lapsed Policies": random.randint(0, 2),
            "Motor Risk Address": f'{addr["City"]}, {addr["Postcode"]}',
            "Risk Class Code": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "Variant": random.choice(["BASE", "SPORT"]),
            "Vehicle Owner Type": random.choice(["SELF", "COMPANY"]),
            "Vehicle RegState": addr["State"],
            "Vehicle Class": vehicle_class,
            "Vehicle Model": vehicle_model,
            "Vehicle Type": vehicle_type,
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
