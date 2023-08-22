DB_ARGS = {
    "host": "localhost",
    "port": 5432,
    "database": "usgs"
}

PAD_SCHEMA = "pad_us"
PAD_REQUESTS_SCHEMA = "aoi_requests"
PAD_COMPUTE_SCHEMA = "percent_coverage"

PAD_TABLES = [
    "padus3_0combined_dod_trib_fee_designation_easement",
    "padus3_0combined_proclamation_marine_fee_designation_easement",
    "padus3_0designation",
    "padus3_0easement",
    "padus3_0fee",
    "padus3_0marine",
    "padus3_0proclamation",
]

GROUPBY_OPTIONS = {"featclass", "mang_type", "des_tp"}
