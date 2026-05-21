import random
import hashlib
import re
import calendar
import math
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


def _premium_abs_increase_band(current_premium: float, next_premium: float) -> str:
    increase = next_premium - current_premium
    if increase <= 0:
        return "LE_0"
    if increase <= 50:
        return "1_50"
    if increase <= 100:
        return "51_100"
    return "GT_100"


def _current_premium_band(current_premium: float) -> str:
    if current_premium <= 600:
        return "LOW"
    if current_premium <= 900:
        return "MEDIUM"
    if current_premium <= 1200:
        return "HIGH"
    return "VERY_HIGH"


def _current_premium_churn_probability(current_premium: float, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("current_premium_churn_probability") or {}
    defaults = {
        "LOW": 0.14,
        "MEDIUM": 0.20,
        "HIGH": 0.32,
        "VERY_HIGH": 0.47,
    }
    band = _current_premium_band(current_premium)
    return float(probabilities.get(band, defaults.get(band, 0.20)))


def _current_premium_churn_factor(current_premium: float, churn_config: dict | None = None) -> float:
    churn_cfg = _churn_settings(churn_config)
    premium_weights = churn_cfg.get("renewal_current_premium_band_weights") or {
        "LOW": 35,
        "MEDIUM": 35,
        "HIGH": 20,
        "VERY_HIGH": 10,
    }
    total_weight = sum(float(weight) for weight in premium_weights.values()) or 1.0
    probabilities = churn_cfg.get("current_premium_churn_probability") or {
        "LOW": 0.14,
        "MEDIUM": 0.20,
        "HIGH": 0.32,
        "VERY_HIGH": 0.47,
    }
    weighted_probability = sum(
        float(weight) * float(probabilities.get(band, 0.20))
        for band, weight in premium_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("current_premium_churn_normalization_scale", 1.0))
    return scale * (_current_premium_churn_probability(current_premium, churn_cfg) / weighted_probability)


def _premium_pct_increase_band(current_premium: float, next_premium: float) -> str:
    if current_premium <= 0:
        return "GT_10"
    pct = ((next_premium - current_premium) / current_premium) * 100
    if pct < 0:
        return "LT_0"
    if pct <= 5:
        return "0_5"
    if pct <= 10:
        return "5_10"
    return "GT_10"


def _premium_pct_churn_probability(
    current_premium: float,
    next_premium: float,
    churn_config: dict | None = None,
) -> float:
    probabilities = _churn_settings(churn_config).get("premium_pct_increase_churn_probability") or {}
    defaults = {
        "LT_0": 0.10,
        "0_5": 0.17,
        "5_10": 0.30,
        "GT_10": 0.52,
    }
    band = _premium_pct_increase_band(current_premium, next_premium)
    return float(probabilities.get(band, defaults.get(band, 0.17)))


def _premium_pct_churn_factor(
    current_premium: float,
    next_premium: float,
    churn_config: dict | None = None,
) -> float:
    churn_cfg = _churn_settings(churn_config)
    movement_weights = churn_cfg.get("renewal_movement_band_weights") or {
        "DECREASE": 10,
        "0_5": 18,
        "5_10": 30,
        "GT_10": 42,
    }
    band_weight_map = {
        "DECREASE": "LT_0",
        "0_5": "0_5",
        "SMALL": "0_5",
        "5_10": "5_10",
        "MEDIUM": "5_10",
        "GT_10": "GT_10",
        "LARGE": "GT_10",
    }
    expected_weights = {"LT_0": 0.0, "0_5": 0.0, "5_10": 0.0, "GT_10": 0.0}
    for movement_band, weight in movement_weights.items():
        expected_weights[band_weight_map.get(movement_band, "GT_10")] += float(weight)
    total_weight = sum(expected_weights.values()) or 1.0
    probabilities = churn_cfg.get("premium_pct_increase_churn_probability") or {
        "LT_0": 0.10,
        "0_5": 0.17,
        "5_10": 0.30,
        "GT_10": 0.52,
    }
    weighted_probability = sum(
        weight * float(probabilities.get(band, 0.17))
        for band, weight in expected_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("premium_pct_increase_churn_normalization_scale", 1.0))
    return scale * (_premium_pct_churn_probability(current_premium, next_premium, churn_cfg) / weighted_probability)


def _premium_abs_churn_probability(
    current_premium: float,
    next_premium: float,
    churn_config: dict | None = None,
) -> float:
    probabilities = _churn_settings(churn_config).get("premium_abs_increase_churn_probability") or {}
    defaults = {
        "LE_0": 0.10,
        "1_50": 0.18,
        "51_100": 0.31,
        "GT_100": 0.55,
    }
    band = _premium_abs_increase_band(current_premium, next_premium)
    return float(probabilities.get(band, defaults.get(band, 0.18)))


def _premium_abs_churn_factor(
    current_premium: float,
    next_premium: float,
    churn_config: dict | None = None,
) -> float:
    churn_cfg = _churn_settings(churn_config)
    movement_weights = churn_cfg.get("renewal_movement_band_weights") or {
        "DECREASE": 10,
        "0_5": 18,
        "5_10": 30,
        "GT_10": 42,
    }
    # Approximate expected absolute-increase pressure from current configured movement bands.
    band_weight_map = {
        "DECREASE": "LE_0",
        "0_5": "1_50",
        "SMALL": "1_50",
        "5_10": "51_100",
        "MEDIUM": "51_100",
        "GT_10": "GT_100",
        "LARGE": "GT_100",
    }
    expected_weights = {"LE_0": 0.0, "1_50": 0.0, "51_100": 0.0, "GT_100": 0.0}
    for movement_band, weight in movement_weights.items():
        expected_weights[band_weight_map.get(movement_band, "GT_100")] += float(weight)
    total_weight = sum(expected_weights.values()) or 1.0
    probabilities = churn_cfg.get("premium_abs_increase_churn_probability") or {
        "LE_0": 0.10,
        "1_50": 0.18,
        "51_100": 0.31,
        "GT_100": 0.55,
    }
    weighted_probability = sum(
        weight * float(probabilities.get(band, 0.18))
        for band, weight in expected_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("premium_abs_increase_churn_normalization_scale", 1.0))
    return scale * (_premium_abs_churn_probability(current_premium, next_premium, churn_cfg) / weighted_probability)


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


def _addon_churn_probability(cover_option: str, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("addon_churn_probability") or {}
    defaults = {
        "BASE_ONLY": 0.34,
        "ONE_ADD_ON": 0.23,
        "TWO_ADD_ONS": 0.16,
        "THREE_PLUS_ADD_ONS": 0.12,
    }
    return float(probabilities.get(cover_option, defaults.get(cover_option, 0.23)))


def _addon_churn_factor(cover_option: str, churn_config: dict | None = None) -> float:
    churn_cfg = _churn_settings(churn_config)
    cover_weights = churn_cfg.get("cover_option_weights") or {
        "BASE_ONLY": 35,
        "ONE_ADD_ON": 30,
        "TWO_ADD_ONS": 22,
        "THREE_PLUS_ADD_ONS": 13,
    }
    total_weight = sum(float(weight) for weight in cover_weights.values()) or 1.0
    weighted_probability = sum(
        float(weight) * _addon_churn_probability(option, churn_cfg)
        for option, weight in cover_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("addon_churn_normalization_scale", 0.85))
    return scale * (_addon_churn_probability(cover_option, churn_cfg) / weighted_probability)


def _claim_count_band(total_claims: int) -> str:
    if total_claims <= 0:
        return "0"
    if total_claims == 1:
        return "1"
    if total_claims == 2:
        return "2"
    return "3_PLUS"


def _claim_count_churn_probability(total_claims: int, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("claim_count_churn_probability") or {}
    defaults = {
        "0": 0.15,
        "1": 0.25,
        "2": 0.37,
        "3_PLUS": 0.52,
    }
    band = _claim_count_band(total_claims)
    return float(probabilities.get(band, defaults.get(band, 0.25)))


def _claim_count_churn_factor(total_claims: int, churn_config: dict | None = None) -> float:
    churn_cfg = _churn_settings(churn_config)
    claim_weights = churn_cfg.get("claim_count_weights") or {"0": 18, "1": 25, "2": 25, "3": 18, "4": 9, "5": 5}
    band_weights = {"0": 0.0, "1": 0.0, "2": 0.0, "3_PLUS": 0.0}
    for raw_count, weight in claim_weights.items():
        band_weights[_claim_count_band(int(raw_count))] += float(weight)
    total_weight = sum(band_weights.values()) or 1.0
    weighted_probability = sum(
        weight * _claim_count_churn_probability(3 if band == "3_PLUS" else int(band), churn_cfg)
        for band, weight in band_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("claim_count_churn_normalization_scale", 0.95))
    return scale * (_claim_count_churn_probability(total_claims, churn_cfg) / weighted_probability)


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


def _tenure_band(policy_cycle: int) -> str:
    if policy_cycle < 1:
        return "LT_1"
    if policy_cycle <= 2:
        return "Y1_2"
    if policy_cycle <= 5:
        return "Y3_5"
    return "GT_5"


def _sample_policy_status_for_tenure(
    policy_cycle: int,
    account_status: str | None,
    churn_config: dict | None = None,
    cover_option: str | None = None,
    total_claims: int | None = None,
    renewal_current: float | None = None,
    renewal_next: float | None = None,
    vehicle_segment: str | None = None,
    marketing_engagement_band: str | None = None,
    driver_experience_band: str | None = None,
) -> str:
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

    churn_probability = _tenure_churn_probability(policy_cycle, churn_cfg)
    if cover_option:
        churn_probability *= _addon_churn_factor(cover_option, churn_cfg)
    if total_claims is not None:
        churn_probability *= _claim_count_churn_factor(total_claims, churn_cfg)
    if renewal_current is not None and renewal_next is not None:
        churn_probability *= _current_premium_churn_factor(renewal_current, churn_cfg)
        churn_probability *= _premium_abs_churn_factor(renewal_current, renewal_next, churn_cfg)
        churn_probability *= _premium_pct_churn_factor(renewal_current, renewal_next, churn_cfg)
    if vehicle_segment:
        churn_probability *= _vehicle_segment_churn_factor(vehicle_segment, churn_cfg)
    if marketing_engagement_band:
        churn_probability *= _marketing_engagement_churn_factor(marketing_engagement_band, churn_cfg)
    if driver_experience_band:
        churn_probability *= _driver_experience_churn_factor(driver_experience_band, churn_cfg)
    churn_probability = max(0.0, min(0.95, churn_probability))
    if random.random() >= churn_probability:
        return "ACTIVE"

    if policy_cycle < 1:
        return "CANCELLED"
    return _weighted_choice_from_mapping(
        churn_cfg.get("churned_policy_status_weights"),
        {"LAPSED": 70, "CANCELLED": 30},
    )


def _sample_vehicle_segment(churn_config: dict | None = None) -> str:
    return _weighted_choice_from_mapping(
        _churn_settings(churn_config).get("vehicle_segment_weights"),
        {"STANDARD": 65, "PREMIUM": 25, "HIGH_RISK": 10},
    )


def _vehicle_profile_for_segment(segment: str) -> tuple[str, str, str, str]:
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
    return random.choice(profiles.get(segment, profiles["STANDARD"]))


def _sample_vehicle_profile(churn_config: dict | None = None) -> tuple[str, str, str, str]:
    return _vehicle_profile_for_segment(_sample_vehicle_segment(churn_config))


def _vehicle_segment_from_profile(profile: tuple[str, str, str, str] | None) -> str:
    if not profile:
        return ""
    vehicle_model = profile[2]
    if vehicle_model in {"Focus", "Corsa", "Corolla"}:
        return "STANDARD"
    if vehicle_model in {"Qashqai", "3 Series", "A3"}:
        return "PREMIUM"
    if vehicle_model in {"Sport Bike", "Superbike", "High Performance SUV"}:
        return "HIGH_RISK"
    return ""


def _vehicle_segment_churn_probability(vehicle_segment: str, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("vehicle_segment_churn_probability") or {}
    defaults = {
        "STANDARD": 0.17,
        "PREMIUM": 0.27,
        "HIGH_RISK": 0.40,
    }
    return float(probabilities.get(vehicle_segment, defaults.get(vehicle_segment, 0.17)))


def _vehicle_segment_churn_factor(vehicle_segment: str, churn_config: dict | None = None) -> float:
    if not vehicle_segment:
        return 1.0
    churn_cfg = _churn_settings(churn_config)
    segment_weights = churn_cfg.get("vehicle_segment_weights") or {"STANDARD": 65, "PREMIUM": 25, "HIGH_RISK": 10}
    total_weight = sum(float(weight) for weight in segment_weights.values()) or 1.0
    weighted_probability = sum(
        float(weight) * _vehicle_segment_churn_probability(segment, churn_cfg)
        for segment, weight in segment_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("vehicle_segment_churn_normalization_scale", 1.0))
    return scale * (_vehicle_segment_churn_probability(vehicle_segment, churn_cfg) / weighted_probability)


def _policy_churn_band_targets(churn_config: dict | None = None) -> dict:
    churn_cfg = _churn_settings(churn_config)
    return {
        "premium": {
            "order": ["LE_0", "1_50", "51_100", "GT_100"],
            "ranges": churn_cfg.get("premium_abs_increase_churn_expected_ranges") or {
                "LE_0": [0.08, 0.12],
                "1_50": [0.15, 0.22],
                "51_100": [0.25, 0.38],
                "GT_100": [0.45, 0.65],
            },
            "target": {"LE_0": 0.10, "1_50": 0.18, "51_100": 0.31, "GT_100": 0.50},
        },
        "current_premium": {
            "order": ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"],
            "ranges": churn_cfg.get("current_premium_churn_expected_ranges") or {
                "LOW": [0.10, 0.18],
                "MEDIUM": [0.15, 0.25],
                "HIGH": [0.25, 0.40],
                "VERY_HIGH": [0.40, 0.55],
            },
            "target": {"LOW": 0.14, "MEDIUM": 0.20, "HIGH": 0.32, "VERY_HIGH": 0.47},
        },
        "tenure": {
            "order": ["LT_1", "Y1_2", "Y3_5", "GT_5"],
            "ranges": churn_cfg.get("tenure_churn_expected_ranges") or {
                "LT_1": [0.35, 0.50],
                "Y1_2": [0.25, 0.35],
                "Y3_5": [0.15, 0.25],
                "GT_5": [0.08, 0.15],
            },
            "target": {"LT_1": 0.42, "Y1_2": 0.30, "Y3_5": 0.20, "GT_5": 0.12},
        },
        "premium_pct": {
            "order": ["LT_0", "0_5", "5_10", "GT_10"],
            "ranges": churn_cfg.get("premium_pct_increase_churn_expected_ranges") or {
                "LT_0": [0.08, 0.12],
                "0_5": [0.15, 0.20],
                "5_10": [0.25, 0.35],
                "GT_10": [0.45, 0.65],
            },
            "target": {"LT_0": 0.10, "0_5": 0.17, "5_10": 0.30, "GT_10": 0.52},
        },
        "claim": {
            "order": ["0", "1", "2", "3_PLUS"],
            "ranges": churn_cfg.get("claim_count_churn_expected_ranges") or {
                "0": [0.12, 0.18],
                "1": [0.20, 0.30],
                "2": [0.30, 0.45],
                "3_PLUS": [0.45, 0.60],
            },
            "target": {"0": 0.15, "1": 0.25, "2": 0.38, "3_PLUS": 0.52},
        },
        "addon": {
            "order": ["BASE_ONLY", "ONE_ADD_ON", "TWO_ADD_ONS", "THREE_PLUS_ADD_ONS"],
            "ranges": churn_cfg.get("addon_churn_expected_ranges") or {
                "BASE_ONLY": [0.25, 0.40],
                "ONE_ADD_ON": [0.18, 0.28],
                "TWO_ADD_ONS": [0.12, 0.22],
                "THREE_PLUS_ADD_ONS": [0.08, 0.18],
            },
            "target": {
                "BASE_ONLY": 0.34,
                "ONE_ADD_ON": 0.24,
                "TWO_ADD_ONS": 0.17,
                "THREE_PLUS_ADD_ONS": 0.12,
            },
        },
        "marketing": {
            "order": ["HIGH", "MEDIUM", "LOW", "NONE"],
            "ranges": churn_cfg.get("marketing_engagement_churn_expected_ranges") or {
                "HIGH": [0.08, 0.15],
                "MEDIUM": [0.18, 0.30],
                "LOW": [0.35, 0.55],
                "NONE": [0.50, 0.70],
            },
            "target": {"HIGH": 0.12, "MEDIUM": 0.24, "LOW": 0.45, "NONE": 0.60},
        },
        "vehicle": {
            "order": ["STANDARD", "PREMIUM", "HIGH_RISK"],
            "ranges": churn_cfg.get("vehicle_segment_churn_expected_ranges") or {
                "STANDARD": [0.12, 0.22],
                "PREMIUM": [0.20, 0.35],
                "HIGH_RISK": [0.30, 0.50],
            },
            "target": {"STANDARD": 0.17, "PREMIUM": 0.27, "HIGH_RISK": 0.40},
        },
        "driver": {
            "order": ["LT_2Y", "Y2_5", "Y6_10", "GT_10"],
            "ranges": churn_cfg.get("driver_experience_churn_expected_ranges") or {
                "LT_2Y": [0.25, 0.40],
                "Y2_5": [0.18, 0.30],
                "Y6_10": [0.15, 0.25],
                "GT_10": [0.10, 0.18],
            },
            "target": {"LT_2Y": 0.32, "Y2_5": 0.24, "Y6_10": 0.20, "GT_10": 0.14},
        },
    }


def _policy_row_bands(row: dict) -> dict:
    current = float(row["Renewal Amount Current Period"])
    next_amt = float(row["Renewal Amount Next Period"])
    total_claims = (
        int(row["Declined Claims"])
        + int(row["Number of Active Claim"])
        + int(row["Number of Previous Claim"])
    )
    return {
        "current_premium": _current_premium_band(current),
        "premium": _premium_abs_increase_band(current, next_amt),
        "premium_pct": _premium_pct_increase_band(current, next_amt),
        "claim": _claim_count_band(total_claims),
        "addon": str(row["Cover Option"]),
        "marketing": str(row.get("_Marketing Engagement Band") or ""),
        "vehicle": str(row.get("_Vehicle Segment") or ""),
        "driver": str(row.get("_Driver Experience Band") or ""),
        "tenure": _tenure_band(int(row["Policy Cycle"])),
    }


def _policy_churn_score(row: dict, targets: dict) -> float:
    bands = _policy_row_bands(row)
    policy_cycle = int(row["Policy Cycle"])
    tenure_score = targets.get("tenure", {}).get("target", {}).get(_tenure_band(policy_cycle), _tenure_churn_probability(policy_cycle, {}))
    return (
        tenure_score
        + targets["current_premium"]["target"].get(bands["current_premium"], 0.20)
        + targets["premium"]["target"].get(bands["premium"], 0.18)
        + targets["premium_pct"]["target"].get(bands["premium_pct"], 0.17)
        + targets["claim"]["target"].get(bands["claim"], 0.25)
        + targets["addon"]["target"].get(bands["addon"], 0.24)
        + targets["marketing"]["target"].get(bands["marketing"], 0.24)
        + targets["vehicle"]["target"].get(bands["vehicle"], 0.17)
        + targets["driver"]["target"].get(bands["driver"], 0.20)
    )


def _policy_churn_counts(flags: list[bool], bands_by_row: list[dict], targets: dict) -> dict:
    counts = {}
    for dimension, spec in targets.items():
        dimension_counts = {}
        for band in spec["order"]:
            indices = [idx for idx, bands in enumerate(bands_by_row) if bands[dimension] == band]
            if not indices:
                continue
            churned = sum(1 for idx in indices if flags[idx])
            dimension_counts[band] = {
                "indices": indices,
                "count": len(indices),
                "churned": churned,
                "min": math.ceil(float(spec["ranges"][band][0]) * len(indices)),
                "max": math.floor(float(spec["ranges"][band][1]) * len(indices)),
                "target": min(
                    max(round(float(spec["target"][band]) * len(indices)), math.ceil(float(spec["ranges"][band][0]) * len(indices))),
                    math.floor(float(spec["ranges"][band][1]) * len(indices)),
                ),
            }
        counts[dimension] = dimension_counts
    return counts


def _policy_row_in_band_above_max(idx: int, counts: dict, bands_by_row: list[dict]) -> bool:
    for dimension, dimension_counts in counts.items():
        band = bands_by_row[idx].get(dimension)
        band_counts = dimension_counts.get(band)
        if band_counts and band_counts["churned"] > band_counts["max"]:
            return True
    return False


def _policy_row_in_band_below_min(idx: int, counts: dict, bands_by_row: list[dict]) -> bool:
    for dimension, dimension_counts in counts.items():
        band = bands_by_row[idx].get(dimension)
        band_counts = dimension_counts.get(band)
        if band_counts and band_counts["churned"] <= band_counts["min"]:
            return True
    return False


def _set_policy_row_status_dates(row: dict, status: str, load_dt: datetime, as_of_dt: datetime, churn_config: dict | None) -> None:
    policy_start = _coerce_datetime(row["Policy Start Date"])
    policy_cycle = int(row["Policy Cycle"])
    current_term_start = _add_years(policy_start, policy_cycle)
    current_term_end = _add_years(policy_start, policy_cycle + 1)

    if status == "LAPSED" and (policy_cycle < 1 or current_term_start > load_dt):
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

    row["Policy Status"] = status
    row["Policy End Date"] = policy_end.strftime("%Y-%m-%d %H:%M:%S")
    row["Renewal Date"] = (policy_end - timedelta(days=random.randint(0, 10))).strftime("%Y-%m-%d %H:%M:%S")
    row["Sales Channel"] = _sample_sales_channel_for_status(status, churn_config)

    fraud_risk_score = 0
    if int(row["Declined Claims"]) >= 2:
        fraud_risk_score += 2
    if int(row["Number of Previous Claim"]) >= 4:
        fraud_risk_score += 1
    if int(row["Number of Active Claim"]) >= 2:
        fraud_risk_score += 1
    if status == "CANCELLED":
        fraud_risk_score += 1
    account_status = str(row.get("_Account Status") or "").upper()
    if account_status == "SUSPENDED":
        fraud_risk_score += 1
    elif account_status == "CLOSED":
        fraud_risk_score += 2
    row["Fraud Flag"] = "Y" if fraud_risk_score >= 4 else "N"


def _calibrate_policy_churn_rows(rows: list[dict], churn_config: dict | None, load_dt: datetime, as_of_dt: datetime) -> None:
    if len(rows) < 200:
        return

    targets = _policy_churn_band_targets(churn_config)
    flags = [row["Policy Status"] in {"CANCELLED", "LAPSED"} for row in rows]
    locked_churn = [
        str(row.get("_Account Status") or "").upper() in {"CLOSED", "SUSPENDED"}
        for row in rows
    ]
    for idx, locked in enumerate(locked_churn):
        if locked:
            flags[idx] = True
    bands_by_row = [_policy_row_bands(row) for row in rows]
    scores = [_policy_churn_score(row, targets) for row in rows]

    for _ in range(50):
        changed = False
        counts = _policy_churn_counts(flags, bands_by_row, targets)
        for dimension, spec in targets.items():
            for band in spec["order"]:
                band_counts = counts.get(dimension, {}).get(band)
                if not band_counts:
                    continue
                indices = band_counts["indices"]
                if len(indices) < 20:
                    continue
                current = band_counts["churned"]
                min_count = band_counts["min"]
                max_count = band_counts["max"]
                target_count = band_counts["target"]

                if current < target_count:
                    candidates = [idx for idx in indices if not flags[idx]]
                    candidates.sort(
                        key=lambda idx: (
                            _policy_row_in_band_below_min(idx, counts, bands_by_row),
                            scores[idx],
                        ),
                        reverse=True,
                    )
                    for idx in candidates[: target_count - current]:
                        flags[idx] = True
                        changed = True
                elif current > target_count:
                    candidates = [
                        idx
                        for idx in indices
                        if flags[idx]
                        and not locked_churn[idx]
                        and not _policy_row_in_band_below_min(idx, counts, bands_by_row)
                    ]
                    candidates.sort(
                        key=lambda idx: (
                            not _policy_row_in_band_above_max(idx, counts, bands_by_row),
                            scores[idx],
                        )
                    )
                    for idx in candidates[: current - target_count]:
                        flags[idx] = False
                        changed = True
                counts = _policy_churn_counts(flags, bands_by_row, targets)
        if not changed:
            break

    for idx, row in enumerate(rows):
        if locked_churn[idx]:
            status = "CANCELLED" if str(row.get("_Account Status") or "").upper() == "CLOSED" else (
                "CANCELLED" if int(row["Policy Cycle"]) < 1 else _weighted_choice_from_mapping(
                    _churn_settings(churn_config).get("suspended_policy_status_weights"),
                    {"LAPSED": 65, "CANCELLED": 35},
                )
            )
        elif flags[idx]:
            if int(row["Policy Cycle"]) < 1:
                status = "CANCELLED"
            else:
                status = _weighted_choice_from_mapping({"LAPSED": 55, "CANCELLED": 45}, {"LAPSED": 55, "CANCELLED": 45})
        else:
            status = "ACTIVE"
        _set_policy_row_status_dates(row, status, load_dt, as_of_dt, churn_config)


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


def _marketing_engagement_band_from_flags(flags: dict[str, str]) -> str:
    score = sum(
        1
        for channel in ("Email Subscriptions", "Commercial Email", "Email", "SMS")
        if flags.get(channel) == "Y"
    )
    if score == 0:
        return "NONE"
    if score == 1:
        return "LOW"
    if score <= 3:
        return "MEDIUM"
    return "HIGH"


def _marketing_engagement_churn_probability(engagement_band: str, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("marketing_engagement_churn_probability") or {}
    defaults = {
        "HIGH": 0.12,
        "MEDIUM": 0.24,
        "LOW": 0.45,
        "NONE": 0.60,
    }
    return float(probabilities.get(engagement_band, defaults.get(engagement_band, 0.24)))


def _marketing_engagement_churn_factor(engagement_band: str | None, churn_config: dict | None = None) -> float:
    if not engagement_band:
        return 1.0
    churn_cfg = _churn_settings(churn_config)
    band_weights = churn_cfg.get("marketing_engagement_band_weights") or {
        "HIGH": 15,
        "MEDIUM": 30,
        "LOW": 35,
        "NONE": 20,
    }
    total_weight = sum(float(weight) for weight in band_weights.values()) or 1.0
    probabilities = churn_cfg.get("marketing_engagement_churn_probability") or {
        "HIGH": 0.12,
        "MEDIUM": 0.24,
        "LOW": 0.45,
        "NONE": 0.60,
    }
    weighted_probability = sum(
        float(weight) * float(probabilities.get(band, 0.24))
        for band, weight in band_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("marketing_engagement_churn_normalization_scale", 1.0))
    return scale * (_marketing_engagement_churn_probability(engagement_band, churn_cfg) / weighted_probability)


def _driver_experience_band(experience_years: float | int | None) -> str:
    if experience_years is None:
        return ""
    if experience_years < 2:
        return "LT_2Y"
    if experience_years <= 5:
        return "Y2_5"
    if experience_years <= 10:
        return "Y6_10"
    return "GT_10"


def _driver_experience_churn_probability(driver_experience_band: str, churn_config: dict | None = None) -> float:
    probabilities = _churn_settings(churn_config).get("driver_experience_churn_probability") or {}
    defaults = {
        "LT_2Y": 0.32,
        "Y2_5": 0.24,
        "Y6_10": 0.20,
        "GT_10": 0.14,
    }
    return float(probabilities.get(driver_experience_band, defaults.get(driver_experience_band, 0.20)))


def _driver_experience_churn_factor(driver_experience_band: str | None, churn_config: dict | None = None) -> float:
    if not driver_experience_band:
        return 1.0
    churn_cfg = _churn_settings(churn_config)
    band_weights = churn_cfg.get("driver_experience_band_weights") or {
        "LT_2Y": 12,
        "Y2_5": 20,
        "Y6_10": 25,
        "GT_10": 43,
    }
    total_weight = sum(float(weight) for weight in band_weights.values()) or 1.0
    probabilities = churn_cfg.get("driver_experience_churn_probability") or {
        "LT_2Y": 0.32,
        "Y2_5": 0.24,
        "Y6_10": 0.20,
        "GT_10": 0.14,
    }
    weighted_probability = sum(
        float(weight) * float(probabilities.get(band, 0.20))
        for band, weight in band_weights.items()
    ) / total_weight
    if weighted_probability <= 0:
        return 1.0
    scale = float(churn_cfg.get("driver_experience_churn_normalization_scale", 1.0))
    return scale * (_driver_experience_churn_probability(driver_experience_band, churn_cfg) / weighted_probability)


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
def sat_marketing_preference(person_to_mpr_hk, load_date, churn_config=None, person_marketing_engagement_by_person=None):
    rows = []
    for person_hk, hk_or_hks in person_to_mpr_hk.items():
        for hk in _as_list(hk_or_hks):
            flags = _sample_marketing_preference_flags(churn_config)
            if person_marketing_engagement_by_person is not None:
                person_marketing_engagement_by_person[person_hk] = _marketing_engagement_band_from_flags(flags)
                
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
        policy_to_motor=None,
        motor_vehicle_profiles=None,
        person_marketing_engagement_by_person=None,
        person_driver_experience_by_person=None,
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
        cover_option = _sample_policy_cover_option(churn_config)
        declined_claims, active_claims, previous_claims = _sample_policy_claim_counts(churn_config)
        total_claims = declined_claims + active_claims + previous_claims
        renewal_current, renewal_next = _sample_renewal_premiums(churn_config)
        vehicle_segment = ""
        if policy_to_motor and hk in policy_to_motor:
            vehicle_segment = _sample_vehicle_segment(churn_config)
            motor_hks = policy_to_motor.get(hk)
            if not isinstance(motor_hks, list):
                motor_hks = [motor_hks]
            profile = _vehicle_profile_for_segment(vehicle_segment)
            if motor_vehicle_profiles is not None:
                for motor_hk in motor_hks:
                    motor_vehicle_profiles[motor_hk] = profile
        marketing_engagement_band = (
            person_marketing_engagement_by_person.get(person_hk)
            if person_marketing_engagement_by_person and person_hk
            else None
        )
        driver_experience_band = (
            person_driver_experience_by_person.get(person_hk)
            if person_driver_experience_by_person and person_hk
            else None
        )
        status = _sample_policy_status_for_tenure(
            policy_cycle,
            account_status,
            churn_config,
            cover_option,
            total_claims,
            renewal_current,
            renewal_next,
            vehicle_segment,
            marketing_engagement_band,
            driver_experience_band,
        )

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
            "Cover Option": cover_option,
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
            "Sales Channel": sales_channel,
            "_Account Status": account_status,
            "_Marketing Engagement Band": marketing_engagement_band or "",
            "_Vehicle Segment": vehicle_segment or "",
            "_Driver Experience Band": driver_experience_band or "",
        })
    _calibrate_policy_churn_rows(rows, churn_config, load_dt, as_of_dt)
    for row in rows:
        row.pop("_Account Status", None)
        row.pop("_Marketing Engagement Band", None)
        row.pop("_Vehicle Segment", None)
        row.pop("_Driver Experience Band", None)
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
def sat_motor(motor_hks, load_date, motor_to_addr: dict | None = None, churn_config=None, motor_vehicle_profiles=None):
    rows = []
    for motor_hk in motor_hks:
        year = random.randint(2005, 2025)

        addr_hk = motor_to_addr.get(motor_hk) if motor_to_addr else None
        addr = HOME_ADDRESS_CACHE.get(addr_hk) if addr_hk else None

        if not addr:
            addr = _address_from_cache_by_hk(motor_hk)

        body_type, vehicle_class, vehicle_model, vehicle_type = (
            motor_vehicle_profiles.get(motor_hk)
            if motor_vehicle_profiles and motor_hk in motor_vehicle_profiles
            else _sample_vehicle_profile(churn_config)
        )

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
