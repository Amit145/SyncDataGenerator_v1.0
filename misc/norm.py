from helper.csv_writer import normalize_csv
from validators.integrity_checker import _latest_run


normalize_csv(_latest_run('output'), './synthetic_data')