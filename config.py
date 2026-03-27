from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

SUPPORTED_EXTENSIONS = [".xlsx", ".xls"]

DEFAULT_NUMERIC_CANDIDATES = [
    "Total Capacity Purchased",
    "Capacity in Use",
    "Usage in KW",
    "Billable Additional Capacity",
    "Additional Capacity Charges (MRC)",
    "Subscription",
    "In Use",
    "SQ.FT",
    "No Of Units",
    "Unit Rate (per KW-HR)",
    "Value",
    "Actual Load KVA",
    "Contract Demand",
    "Utility Consump",
    "IT Consump",
    "PUE",
    "Design",
    "Sold",
    "Available",
    "Consumed",
]
