import hashlib
from datetime import datetime


def get_run_id(seed: int):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{seed}"


def get_folder_run_id():
    return datetime.now().strftime("%Y%m%d%H%M%S")


def md5_hasher(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def make_business_id(prefix: str, run_id: str, seq: int) -> str:
    return f"{prefix}_{run_id}_{str(seq).zfill(6)}"


def get_now_iso():
    return datetime.now().isoformat()
