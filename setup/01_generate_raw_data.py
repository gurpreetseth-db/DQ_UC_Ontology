# Databricks notebook source
# NexusRetail Analytics — Synthetic Raw Data Generation
# Schema: online_retail_raw | Volume: raw_data
#
# Story: Global e-commerce retailer "NexusRetail" spans 7 regions, 65 countries.
# A faulty batch of 8 electronics SKUs shipped in Q3 2025 drives an anomalous
# return spike — DQX catches it, governance reveals the APAC-East exposure.

# COMMAND ----------
# MAGIC %pip install faker==24.11.0 holidays==0.46 python-dateutil
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import math

# ── Configuration ──────────────────────────────────────────────────────────
# catalog is injected by the DAB job via base_parameters (set in databricks.local.yml).
# The widget default is a fallback for manual notebook runs only.
dbutils.widgets.text("catalog", "your_catalog_name")
CATALOG       = dbutils.widgets.get("catalog")
RAW_SCHEMA    = "online_retail_raw"
VOLUME        = "raw_data"
BASE_PATH     = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"

END_DATE      = datetime(2026, 9, 7)
START_DATE    = END_DATE - relativedelta(months=24)   # Sep 2024

N_CUSTOMERS   = 600
N_PRODUCTS    = 200
N_ORDERS      = 1200
N_REVIEWS     = 2000
N_TICKETS     = 1500
N_RETURNS     = 120
N_PROMOTIONS  = 30

# 8 faulty electronics products (product_idx 192-199) shipped Q3 2025
FAULTY_IDX    = list(range(192, 200))
FAULTY_START  = datetime(2025, 7, 1)
FAULTY_END    = datetime(2025, 9, 30)

print(f"▶ Generating NexusRetail dataset")
print(f"  Catalog : {CATALOG}.{RAW_SCHEMA}")
print(f"  Volume  : {BASE_PATH}")
print(f"  Range   : {START_DATE.date()} → {END_DATE.date()}")
print(f"  Orders  : {N_ORDERS}  |  Customers: {N_CUSTOMERS}  |  Products: {N_PRODUCTS}")

# COMMAND ----------
# ── Infrastructure — all schemas created here so downstream jobs never hit "schema not found"
ALL_SCHEMAS = {
    f"`{CATALOG}`.`online_retail_raw`":
        "NexusRetail raw source data — synthetic 24-month online retail dataset. "
        "Landing zone for SDP Auto Loader ingestion into bronze layer.",
    f"`{CATALOG}`.`online_retail_bronze`":
        "NexusRetail bronze layer — Auto Loader streaming tables from UC Volume. "
        "Raw fidelity, schema enforced, metadata columns added.",
    f"`{CATALOG}`.`online_retail_silver`":
        "NexusRetail silver layer — cleansed, DQX-validated streaming tables. "
        "PII masked via UC column masks. Quarantine table captures quality violations.",
    f"`{CATALOG}`.`online_retail_gold`":
        "NexusRetail gold layer — business-ready Materialized View aggregations. No PII.",
    f"`{CATALOG}`.`online_retail_metrics`":
        "NexusRetail semantic/metrics layer — UC Materialized Views and Metric Views "
        "(WITH METRICS LANGUAGE YAML). Genie One data sources. No PII.",
}
for schema_fqn, comment in ALL_SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn} COMMENT '{comment}'")
    print(f"✓ Schema ready: {schema_fqn}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{RAW_SCHEMA}`.`{VOLUME}`")
print(f"✓ Volume ready: /Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}")

# Drop any staging tables from a previous (partial) run — ensures idempotency
STAGING_TABLES = ["stg_products","stg_customers","stg_orders","stg_order_items",
                  "stg_invoices","stg_returns","stg_promotions"]
for _tbl in STAGING_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{RAW_SCHEMA}`.`{_tbl}`")
print(f"✓ Staging tables cleared (idempotent re-run)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — REFERENCE DATA  (small, hardcoded → createDataFrame)
# ═══════════════════════════════════════════════════════════════════════════

# ── ref_regions ─────────────────────────────────────────────────────────────
regions_data = [
    ("REG-001", "AMER-North",  "North America",                "Americas",     "USD"),
    ("REG-002", "AMER-South",  "South & Central America",      "Americas",     "USD"),
    ("REG-003", "EMEA-West",   "Western Europe",               "EMEA",         "EUR"),
    ("REG-004", "EMEA-East",   "Eastern Europe & Balkans",     "EMEA",         "EUR"),
    ("REG-005", "APAC-East",   "East Asia & Australia",        "Asia Pacific", "USD"),
    ("REG-006", "APAC-South",  "South & Southeast Asia",       "Asia Pacific", "USD"),
    ("REG-007", "MENA",        "Middle East & North Africa",   "EMEA",         "USD"),
]
regions_schema = T.StructType([
    T.StructField("region_id",          T.StringType(),  False),
    T.StructField("region_name",        T.StringType(),  False),
    T.StructField("region_description", T.StringType(),  True),
    T.StructField("super_region",       T.StringType(),  False),
    T.StructField("primary_currency",   T.StringType(),  False),
])
df_regions = spark.createDataFrame(regions_data, schema=regions_schema)
df_regions.write.mode("overwrite").parquet(f"{BASE_PATH}/ref_regions")
print(f"✓ ref_regions: {df_regions.count()} rows")

# ── ref_countries ────────────────────────────────────────────────────────────
countries_data = [
    # AMER-North
    ("US", "United States",        "REG-001", "USD", "en", 331_000_000),
    ("CA", "Canada",               "REG-001", "CAD", "en", 38_000_000),
    ("MX", "Mexico",               "REG-001", "MXN", "es", 126_000_000),
    # AMER-South
    ("BR", "Brazil",               "REG-002", "BRL", "pt", 214_000_000),
    ("AR", "Argentina",            "REG-002", "ARS", "es", 45_000_000),
    ("CO", "Colombia",             "REG-002", "COP", "es", 50_000_000),
    ("CL", "Chile",                "REG-002", "CLP", "es", 19_000_000),
    ("PE", "Peru",                 "REG-002", "PEN", "es", 32_000_000),
    # EMEA-West
    ("GB", "United Kingdom",       "REG-003", "GBP", "en", 67_000_000),
    ("DE", "Germany",              "REG-003", "EUR", "de", 83_000_000),
    ("FR", "France",               "REG-003", "EUR", "fr", 67_000_000),
    ("ES", "Spain",                "REG-003", "EUR", "es", 47_000_000),
    ("IT", "Italy",                "REG-003", "EUR", "it", 60_000_000),
    ("NL", "Netherlands",          "REG-003", "EUR", "nl", 17_000_000),
    ("SE", "Sweden",               "REG-003", "SEK", "sv", 10_000_000),
    ("NO", "Norway",               "REG-003", "NOK", "no", 5_000_000),
    ("DK", "Denmark",              "REG-003", "DKK", "da", 6_000_000),
    ("FI", "Finland",              "REG-003", "EUR", "fi", 5_500_000),
    ("CH", "Switzerland",          "REG-003", "CHF", "de", 8_500_000),
    ("AT", "Austria",              "REG-003", "EUR", "de", 9_000_000),
    ("BE", "Belgium",              "REG-003", "EUR", "nl", 11_500_000),
    ("PT", "Portugal",             "REG-003", "EUR", "pt", 10_000_000),
    ("IE", "Ireland",              "REG-003", "EUR", "en", 5_000_000),
    # EMEA-East
    ("PL", "Poland",               "REG-004", "PLN", "pl", 38_000_000),
    ("CZ", "Czech Republic",       "REG-004", "CZK", "cs", 11_000_000),
    ("RO", "Romania",              "REG-004", "RON", "ro", 19_000_000),
    ("HU", "Hungary",              "REG-004", "HUF", "hu", 10_000_000),
    ("BG", "Bulgaria",             "REG-004", "BGN", "bg", 7_000_000),
    ("RS", "Serbia",               "REG-004", "RSD", "sr", 7_000_000),
    ("HR", "Croatia",              "REG-004", "EUR", "hr", 4_000_000),
    ("GR", "Greece",               "REG-004", "EUR", "el", 11_000_000),
    ("SK", "Slovakia",             "REG-004", "EUR", "sk", 5_500_000),
    ("UA", "Ukraine",              "REG-004", "UAH", "uk", 44_000_000),
    # APAC-East
    ("JP", "Japan",                "REG-005", "JPY", "ja", 126_000_000),
    ("CN", "China",                "REG-005", "CNY", "zh", 1_400_000_000),
    ("KR", "South Korea",          "REG-005", "KRW", "ko", 52_000_000),
    ("AU", "Australia",            "REG-005", "AUD", "en", 26_000_000),
    ("NZ", "New Zealand",          "REG-005", "NZD", "en", 5_000_000),
    ("TW", "Taiwan",               "REG-005", "TWD", "zh", 23_500_000),
    ("HK", "Hong Kong",            "REG-005", "HKD", "zh", 7_500_000),
    # APAC-South
    ("IN", "India",                "REG-006", "INR", "hi", 1_380_000_000),
    ("SG", "Singapore",            "REG-006", "SGD", "en", 5_700_000),
    ("MY", "Malaysia",             "REG-006", "MYR", "ms", 32_000_000),
    ("TH", "Thailand",             "REG-006", "THB", "th", 70_000_000),
    ("ID", "Indonesia",            "REG-006", "IDR", "id", 273_000_000),
    ("PH", "Philippines",          "REG-006", "PHP", "fil", 110_000_000),
    ("VN", "Vietnam",              "REG-006", "VND", "vi", 97_000_000),
    ("BD", "Bangladesh",           "REG-006", "BDT", "bn", 165_000_000),
    ("PK", "Pakistan",             "REG-006", "PKR", "ur", 225_000_000),
    ("LK", "Sri Lanka",            "REG-006", "LKR", "si", 22_000_000),
    # MENA
    ("SA", "Saudi Arabia",         "REG-007", "SAR", "ar", 35_000_000),
    ("AE", "United Arab Emirates", "REG-007", "AED", "ar", 10_000_000),
    ("EG", "Egypt",                "REG-007", "EGP", "ar", 100_000_000),
    ("TR", "Turkey",               "REG-007", "TRY", "tr", 84_000_000),
    ("IL", "Israel",               "REG-007", "ILS", "he", 9_000_000),
    ("JO", "Jordan",               "REG-007", "JOD", "ar", 10_000_000),
    ("KW", "Kuwait",               "REG-007", "KWD", "ar", 4_300_000),
    ("QA", "Qatar",                "REG-007", "QAR", "ar", 2_900_000),
    ("NG", "Nigeria",              "REG-007", "NGN", "en", 211_000_000),
    ("ZA", "South Africa",         "REG-007", "ZAR", "en", 59_000_000),
    ("KE", "Kenya",                "REG-007", "KES", "sw", 54_000_000),
    ("MA", "Morocco",              "REG-007", "MAD", "ar", 37_000_000),
    ("GH", "Ghana",                "REG-007", "GHS", "en", 31_000_000),
    ("TN", "Tunisia",              "REG-007", "TND", "ar", 12_000_000),
    ("DZ", "Algeria",              "REG-007", "DZD", "ar", 44_000_000),
]
countries_schema = T.StructType([
    T.StructField("country_code",       T.StringType(),  False),
    T.StructField("country_name",       T.StringType(),  False),
    T.StructField("region_id",          T.StringType(),  False),
    T.StructField("currency_code",      T.StringType(),  False),
    T.StructField("primary_language",   T.StringType(),  False),
    T.StructField("population",         T.LongType(),    True),
])
df_countries = spark.createDataFrame(countries_data, schema=countries_schema)
df_countries.write.mode("overwrite").parquet(f"{BASE_PATH}/ref_countries")
print(f"✓ ref_countries: {df_countries.count()} rows")

# ── product_categories ──────────────────────────────────────────────────────
categories_data = [
    ("CAT-01", "Electronics",              "Devices, gadgets, and accessories",       0.08, 299.99),
    ("CAT-02", "Apparel & Fashion",        "Clothing, footwear, and accessories",     0.06, 59.99),
    ("CAT-03", "Home & Living",            "Furniture, decor, and household items",   0.04, 89.99),
    ("CAT-04", "Sports & Outdoors",        "Equipment and activewear",                0.05, 79.99),
    ("CAT-05", "Beauty & Personal Care",   "Skincare, cosmetics, and grooming",       0.07, 29.99),
    ("CAT-06", "Books & Media",            "Books, music, movies, and games",         0.01, 14.99),
    ("CAT-07", "Toys & Games",             "Toys, board games, and puzzles",          0.06, 34.99),
    ("CAT-08", "Food & Grocery",           "Packaged food, beverages, and snacks",    0.03, 19.99),
    ("CAT-09", "Automotive",               "Car accessories and tools",               0.04, 49.99),
    ("CAT-10", "Garden & Outdoor",         "Plants, tools, and outdoor furniture",    0.03, 39.99),
    ("CAT-11", "Pet Supplies",             "Food, accessories, and healthcare",       0.04, 24.99),
    ("CAT-12", "Office & Stationery",      "Supplies, furniture, and tech accessories", 0.03, 44.99),
]
categories_schema = T.StructType([
    T.StructField("category_id",       T.StringType(),   False),
    T.StructField("category_name",     T.StringType(),   False),
    T.StructField("description",       T.StringType(),   True),
    T.StructField("avg_return_rate",   T.DoubleType(),   False),
    T.StructField("avg_price",         T.DoubleType(),   False),
])
df_categories = spark.createDataFrame(categories_data, schema=categories_schema)
df_categories.write.mode("overwrite").parquet(f"{BASE_PATH}/product_categories")
print(f"✓ product_categories: {df_categories.count()} rows")

# ── product_subcategories ────────────────────────────────────────────────────
subcategories_data = [
    # Electronics (CAT-01)
    ("SUB-0101", "CAT-01", "Smartphones",        "Mobile phones and accessories"),
    ("SUB-0102", "CAT-01", "Laptops",             "Portable computers"),
    ("SUB-0103", "CAT-01", "Tablets",             "Touchscreen tablets and e-readers"),
    ("SUB-0104", "CAT-01", "Audio & Headphones",  "Speakers, headphones, earbuds"),
    ("SUB-0105", "CAT-01", "Cameras & Photo",     "Digital cameras and accessories"),
    # Apparel (CAT-02)
    ("SUB-0201", "CAT-02", "Men's Wear",          "Shirts, trousers, jackets"),
    ("SUB-0202", "CAT-02", "Women's Wear",        "Dresses, tops, trousers"),
    ("SUB-0203", "CAT-02", "Kids' Wear",          "Children's clothing"),
    ("SUB-0204", "CAT-02", "Footwear",            "Shoes, boots, sandals"),
    ("SUB-0205", "CAT-02", "Accessories",         "Bags, belts, jewellery"),
    # Home & Living (CAT-03)
    ("SUB-0301", "CAT-03", "Furniture",           "Sofas, tables, beds"),
    ("SUB-0302", "CAT-03", "Kitchen & Dining",    "Cookware, appliances, tableware"),
    ("SUB-0303", "CAT-03", "Bedding & Bath",      "Sheets, towels, pillows"),
    ("SUB-0304", "CAT-03", "Home Decor",          "Lamps, art, ornaments"),
    ("SUB-0305", "CAT-03", "Storage & Organizers","Shelves, boxes, hangers"),
    # Sports (CAT-04)
    ("SUB-0401", "CAT-04", "Fitness Equipment",  "Weights, yoga mats, resistance bands"),
    ("SUB-0402", "CAT-04", "Outdoor Sports",     "Camping, cycling, hiking"),
    ("SUB-0403", "CAT-04", "Team Sports",        "Balls, nets, protective gear"),
    ("SUB-0404", "CAT-04", "Activewear",         "Sportswear and performance apparel"),
    # Beauty (CAT-05)
    ("SUB-0501", "CAT-05", "Skincare",           "Moisturisers, serums, sunscreen"),
    ("SUB-0502", "CAT-05", "Cosmetics",          "Makeup and colour cosmetics"),
    ("SUB-0503", "CAT-05", "Hair Care",          "Shampoo, conditioners, styling"),
    ("SUB-0504", "CAT-05", "Fragrance",          "Perfumes and body sprays"),
    ("SUB-0505", "CAT-05", "Men's Grooming",     "Shaving, beard care, aftershave"),
    # Books (CAT-06)
    ("SUB-0601", "CAT-06", "Fiction",            "Novels and short stories"),
    ("SUB-0602", "CAT-06", "Non-Fiction",        "Biography, history, science"),
    ("SUB-0603", "CAT-06", "Children's Books",  "Picture books and young adult"),
    ("SUB-0604", "CAT-06", "Digital Media",     "e-books, audiobooks, streaming"),
    # Toys (CAT-07)
    ("SUB-0701", "CAT-07", "Board Games",       "Strategy, trivia, family games"),
    ("SUB-0702", "CAT-07", "Building Sets",     "LEGO, construction toys"),
    ("SUB-0703", "CAT-07", "Dolls & Action",    "Figures, dolls, playsets"),
    ("SUB-0704", "CAT-07", "Outdoor Play",      "Bikes, scooters, water toys"),
    # Food (CAT-08)
    ("SUB-0801", "CAT-08", "Snacks & Confectionery","Chips, chocolates, sweets"),
    ("SUB-0802", "CAT-08", "Beverages",          "Coffee, tea, juices, energy drinks"),
    ("SUB-0803", "CAT-08", "Health Foods",       "Organic, protein, supplements"),
    ("SUB-0804", "CAT-08", "Pantry Staples",     "Pasta, rice, canned goods"),
    # Automotive (CAT-09)
    ("SUB-0901", "CAT-09", "Car Electronics",   "Dash cams, GPS, chargers"),
    ("SUB-0902", "CAT-09", "Car Care",          "Cleaning, polishes, lubricants"),
    ("SUB-0903", "CAT-09", "Tools & Equipment", "Jacks, wrenches, diagnostics"),
    # Garden (CAT-10)
    ("SUB-1001", "CAT-10", "Garden Tools",      "Spades, mowers, hoses"),
    ("SUB-1002", "CAT-10", "Plants & Seeds",    "Seeds, bulbs, potting soil"),
    ("SUB-1003", "CAT-10", "Outdoor Furniture", "Garden chairs, tables, loungers"),
    # Pet Supplies (CAT-11)
    ("SUB-1101", "CAT-11", "Dog Supplies",      "Food, toys, grooming, beds"),
    ("SUB-1102", "CAT-11", "Cat Supplies",      "Food, litter, trees, toys"),
    ("SUB-1103", "CAT-11", "Small Pets",        "Cages, food, accessories"),
    ("SUB-1104", "CAT-11", "Aquatics",          "Fish tanks, filters, food"),
    # Office (CAT-12)
    ("SUB-1201", "CAT-12", "Office Furniture",  "Desks, chairs, shelving"),
    ("SUB-1202", "CAT-12", "Stationery",        "Pens, paper, notebooks"),
    ("SUB-1203", "CAT-12", "Printers & Ink",    "Printers, scanners, cartridges"),
    ("SUB-1204", "CAT-12", "Computer Peripherals","Keyboards, mice, monitors"),
]
sub_schema = T.StructType([
    T.StructField("subcategory_id",   T.StringType(), False),
    T.StructField("category_id",      T.StringType(), False),
    T.StructField("subcategory_name", T.StringType(), False),
    T.StructField("description",      T.StringType(), True),
])
df_subcategories = spark.createDataFrame(subcategories_data, schema=sub_schema)
df_subcategories.write.mode("overwrite").parquet(f"{BASE_PATH}/product_subcategories")
print(f"✓ product_subcategories: {df_subcategories.count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — PRODUCTS + PRICING
# ═══════════════════════════════════════════════════════════════════════════

# Sub-category distribution: 200 products across 47 subcategories
# Electronics gets ~40 products (high-value, high-return story)
sub_counts = {
    "SUB-0101": 12,  # Smartphones — includes 8 faulty ones
    "SUB-0102": 10, "SUB-0103": 8,  "SUB-0104": 8,  "SUB-0105": 6,
    "SUB-0201": 6,  "SUB-0202": 6,  "SUB-0203": 4,  "SUB-0204": 5,  "SUB-0205": 4,
    "SUB-0301": 5,  "SUB-0302": 5,  "SUB-0303": 4,  "SUB-0304": 4,  "SUB-0305": 3,
    "SUB-0401": 4,  "SUB-0402": 4,  "SUB-0403": 3,  "SUB-0404": 4,
    "SUB-0501": 4,  "SUB-0502": 4,  "SUB-0503": 3,  "SUB-0504": 2,  "SUB-0505": 2,
    "SUB-0601": 3,  "SUB-0602": 3,  "SUB-0603": 2,  "SUB-0604": 2,
    "SUB-0701": 3,  "SUB-0702": 3,  "SUB-0703": 2,  "SUB-0704": 2,
    "SUB-0801": 2,  "SUB-0802": 2,  "SUB-0803": 2,  "SUB-0804": 2,
    "SUB-0901": 2,  "SUB-0902": 2,  "SUB-0903": 2,
    "SUB-1001": 2,  "SUB-1002": 2,  "SUB-1003": 2,
    "SUB-1101": 2,  "SUB-1102": 2,  "SUB-1103": 1,  "SUB-1104": 1,
    "SUB-1201": 2,  "SUB-1202": 2,  "SUB-1203": 1,  "SUB-1204": 2,
}

# Build product rows (idx 0-199)
brands = {
    "CAT-01": ["TechNova", "Quantum Edge", "Nexus Pro", "VoltCraft", "PulseTech"],
    "CAT-02": ["Urban Threads", "StyleCraft", "Terrain Co.", "ArcWear", "Zeal"],
    "CAT-03": ["HomeNest", "Comfy Abode", "Crafted Home", "NestWell", "Domero"],
    "CAT-04": ["PeakForm", "ActiveEdge", "TrailBlaze", "ProSport", "VigoFit"],
    "CAT-05": ["LumineSkin", "Glow & Go", "PureBotanica", "AuraSense", "DermRich"],
    "CAT-06": ["PageTurner", "MindPress", "ReadSphere", "InkBound", "Opulence"],
    "CAT-07": ["PlaySpark", "WonderKit", "ToyVault", "FunFactory", "BuildWorld"],
    "CAT-08": ["SnackHive", "NutriCraft", "MunchBox", "OrganicLeaf", "PureBite"],
    "CAT-09": ["AutoElite", "RoadMate", "CarCraft", "SpeedGear", "TrackMaster"],
    "CAT-10": ["GreenThumb", "GardenPro", "OutdoorNest", "BloomCraft", "EarthWorks"],
    "CAT-11": ["PawLove", "ZenPet", "FurFriend", "PetNest", "HappyPaws"],
    "CAT-12": ["WorkSmart", "OfficePro", "DeskCraft", "TaskMate", "ClearDesk"],
}

product_rows = []
idx = 0
for sub_id, count in sub_counts.items():
    cat_id = sub_id[:6].replace("SUB-0", "CAT-0").replace("SUB-1", "CAT-1")
    # Fix cat_id mapping
    cat_prefix = sub_id[4:6]  # e.g. "01", "02"
    cat_id_key = f"CAT-{cat_prefix}"
    brand_list = brands.get(cat_id_key, ["GenericBrand"])
    for i in range(count):
        is_faulty = (idx in FAULTY_IDX)
        product_idx = idx
        sku = f"{cat_prefix}-{sub_id[-4:]}-{str(product_idx).zfill(4)}"
        if is_faulty:
            sku = f"FAULT-{sub_id[-4:]}-{str(product_idx).zfill(4)}"
        brand = brand_list[product_idx % len(brand_list)]
        product_rows.append((
            product_idx,
            f"PROD-{str(product_idx).zfill(5)}",
            sku,
            sub_id,
            cat_id_key,
            brand,
            f"{brand} {sub_id} Model {(product_idx % 20) + 1}",
            is_faulty,  # faulty_batch flag
        ))
        idx += 1

products_schema = T.StructType([
    T.StructField("product_idx",    T.IntegerType(),  False),
    T.StructField("product_id",     T.StringType(),   False),
    T.StructField("sku",            T.StringType(),   False),
    T.StructField("subcategory_id", T.StringType(),   False),
    T.StructField("category_id",    T.StringType(),   False),
    T.StructField("brand",          T.StringType(),   False),
    T.StructField("product_name",   T.StringType(),   False),
    T.StructField("faulty_batch",   T.BooleanType(),  False),
])
df_products_base = spark.createDataFrame(product_rows, schema=products_schema)

# Add faker-generated descriptions and weights
@F.pandas_udf(T.StringType())
def fake_product_name(names: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    fake.seed_instance(42)
    return pd.Series([f"{n.split()[0]} {fake.catch_phrase()[:30]}" for n in names])

@F.pandas_udf(T.DoubleType())
def product_price(idxs: pd.Series) -> pd.Series:
    rng = np.random.default_rng(42)
    # Electronics (idx < 44): high price log-normal
    prices = []
    for i in idxs:
        if i < 44:
            price = float(np.clip(rng.lognormal(5.5, 0.8), 49.99, 1299.99))
        elif i < 100:
            price = float(np.clip(rng.lognormal(4.2, 0.6), 9.99, 199.99))
        else:
            price = float(np.clip(rng.lognormal(3.5, 0.5), 4.99, 99.99))
        prices.append(round(price, 2))
    return pd.Series(prices)

@F.pandas_udf(T.DoubleType())
def product_weight_kg(idxs: pd.Series) -> pd.Series:
    rng = np.random.default_rng(99)
    return pd.Series([round(float(np.clip(rng.lognormal(0.2, 0.8), 0.1, 25.0)), 2) for _ in idxs])

df_products = (
    df_products_base
    .withColumn("product_name",   fake_product_name(F.col("product_name")))
    .withColumn("base_price",     product_price(F.col("product_idx")))
    .withColumn("weight_kg",      product_weight_kg(F.col("product_idx")))
    .withColumn("is_active",      F.lit(True))
    .withColumn("created_at",     F.lit(START_DATE.strftime("%Y-%m-%d")).cast(T.DateType()))
)
df_products.write.mode("overwrite").parquet(f"{BASE_PATH}/products")
print(f"✓ products: {df_products.count()} rows  (8 faulty_batch=true)")

# ── product_pricing: 1-3 price records per product ──────────────────────────
# Write products to Delta first for FK integrity
df_products.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_products")
df_p = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_products").select("product_id", "product_idx", "base_price")

@F.pandas_udf(T.ArrayType(T.StructType([
    T.StructField("effective_from", T.StringType()),
    T.StructField("effective_to",   T.StringType()),
    T.StructField("price",          T.DoubleType()),
    T.StructField("price_change_reason", T.StringType()),
])))
def gen_price_history(idxs: pd.Series, prices: pd.Series) -> pd.Series:
    rng = np.random.default_rng(7)
    reasons = ["seasonal_adjustment", "market_competition", "cost_increase", "promotion_end", "relaunch"]
    results = []
    start_ts = datetime(2024, 9, 1)
    end_ts   = datetime(2026, 9, 7)
    for idx, base_price in zip(idxs, prices):
        n_records = rng.integers(1, 4)  # 1-3 price records
        dates = sorted(rng.choice(
            [(start_ts + timedelta(days=int(d))).strftime("%Y-%m-%d")
             for d in range(0, (end_ts - start_ts).days, 30)],
            size=n_records, replace=False
        ))
        records = []
        for j, d in enumerate(dates):
            eff_to = dates[j+1] if j < len(dates)-1 else "9999-12-31"
            factor = float(rng.uniform(0.85, 1.20))
            price = round(float(base_price) * factor, 2)
            records.append({
                "effective_from": d,
                "effective_to": eff_to,
                "price": price,
                "price_change_reason": rng.choice(reasons),
            })
        results.append(records)
    return pd.Series(results)

df_pricing_raw = (
    df_p
    .withColumn("price_records", gen_price_history(F.col("product_idx"), F.col("base_price")))
    .select("product_id", F.explode("price_records").alias("rec"))
    .select(
        F.monotonically_increasing_id().alias("pricing_id"),
        "product_id",
        F.col("rec.effective_from").cast(T.DateType()).alias("effective_from"),
        F.col("rec.effective_to").cast(T.DateType()).alias("effective_to"),
        "rec.price",
        "rec.price_change_reason",
    )
)
df_pricing_raw.write.mode("overwrite").parquet(f"{BASE_PATH}/product_pricing")
print(f"✓ product_pricing: {df_pricing_raw.count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — CUSTOMERS  (PII-heavy)
# ═══════════════════════════════════════════════════════════════════════════

# Region weights for customer distribution
# AMER-North: 25%, EMEA-West: 22%, APAC-East: 20%, APAC-South: 15%,
# AMER-South: 8%, EMEA-East: 6%, MENA: 4%
REGION_WEIGHTS = {
    "REG-001": 0.25, "REG-002": 0.08, "REG-003": 0.22,
    "REG-004": 0.06, "REG-005": 0.20, "REG-006": 0.15, "REG-007": 0.04,
}

# Country pools per region
REGION_COUNTRIES = {
    "REG-001": ["US","US","US","CA","MX"],
    "REG-002": ["BR","BR","AR","CO","CL","PE"],
    "REG-003": ["GB","DE","FR","ES","IT","NL","SE","CH","AT","BE"],
    "REG-004": ["PL","CZ","RO","HU","GR","UA"],
    "REG-005": ["JP","CN","CN","KR","AU","TW","HK"],
    "REG-006": ["IN","IN","SG","MY","TH","ID","PH","VN"],
    "REG-007": ["SA","AE","EG","TR","NG","ZA","KE","MA"],
}

df_cust_base = spark.range(0, N_CUSTOMERS, numPartitions=8).withColumnRenamed("id", "customer_idx")

@F.pandas_udf(T.StringType())
def fake_full_name(idxs: pd.Series) -> pd.Series:
    from faker import Faker
    locale_pool = ['en_US','en_GB','de_DE','fr_FR','ja_JP','zh_CN','es_ES','pt_BR','ar_SA','hi_IN']
    results = []
    for i in idxs:
        fake = Faker(locale_pool[int(i) % len(locale_pool)])
        fake.seed_instance(int(i))
        results.append(fake.name())
    return pd.Series(results)

@F.pandas_udf(T.StringType())
def fake_email(idxs: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    fake.seed_instance(42)
    results = []
    for i in idxs:
        fake.seed_instance(int(i) + 1000)
        results.append(fake.email())
    return pd.Series(results)

@F.pandas_udf(T.StringType())
def fake_phone(idxs: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    results = []
    for i in idxs:
        fake.seed_instance(int(i) + 2000)
        results.append(fake.phone_number()[:20])
    return pd.Series(results)

@F.pandas_udf(T.StringType())
def assign_region(idxs: pd.Series) -> pd.Series:
    rng = np.random.default_rng(123)
    region_ids = list(REGION_WEIGHTS.keys())
    weights    = list(REGION_WEIGHTS.values())
    return pd.Series([rng.choice(region_ids, p=weights) for _ in idxs])

@F.pandas_udf(T.StringType())
def assign_country(region_ids: pd.Series) -> pd.Series:
    rng = np.random.default_rng(456)
    result = []
    for r in region_ids:
        pool = REGION_COUNTRIES.get(r, ["US"])
        result.append(rng.choice(pool))
    return pd.Series(result)

@F.pandas_udf(T.StringType())
def fake_dob(idxs: pd.Series) -> pd.Series:
    rng = np.random.default_rng(789)
    # Age distribution: mostly 25-54
    age_weights = [0.10, 0.30, 0.25, 0.20, 0.15]  # 18-24, 25-34, 35-44, 45-54, 55+
    age_ranges  = [(18,24),(25,34),(35,44),(45,54),(55,75)]
    result = []
    for _ in idxs:
        bucket = rng.choice(len(age_weights), p=age_weights)
        lo, hi = age_ranges[bucket]
        age = rng.integers(lo, hi+1)
        dob = datetime(2026, 9, 7) - relativedelta(years=int(age)) - timedelta(days=int(rng.integers(0, 365)))
        result.append(dob.strftime("%Y-%m-%d"))
    return pd.Series(result)

df_customers = (
    df_cust_base
    .withColumn("customer_id",
        F.concat(F.lit("CUST-"), F.lpad(F.col("customer_idx").cast("string"), 6, "0")))
    .withColumn("full_name",    fake_full_name(F.col("customer_idx")))
    .withColumn("email",        fake_email(F.col("customer_idx")))
    .withColumn("phone",        fake_phone(F.col("customer_idx")))
    .withColumn("date_of_birth",fake_dob(F.col("customer_idx")).cast(T.DateType()))
    .withColumn("customer_type",
        F.when(F.col("customer_idx") < 500, "B2C").otherwise("B2B"))
    .withColumn("region_id",    assign_region(F.col("customer_idx")))
    .withColumn("country_code", assign_country(F.col("region_id")))
    # Introduce 3 duplicate emails for DQX uniqueness demo
    .withColumn("email",
        F.when(F.col("customer_idx").isin(150, 151, 152),
               F.lit("duplicate.test@example.com"))
         .otherwise(F.col("email")))
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at",
        (F.unix_timestamp(F.lit(START_DATE.strftime("%Y-%m-%d 00:00:00")))
         + F.abs(F.hash(F.col("customer_idx"))) % (365*24*3600)
        ).cast(T.TimestampType()))
)
df_customers.write.mode("overwrite").parquet(f"{BASE_PATH}/customers")
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_customers")
print(f"✓ customers: {df_customers.count()} rows  (incl. 3 duplicate emails for DQX)")

# ── customer_demographics ────────────────────────────────────────────────────
df_cust_ref = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_customers").select("customer_idx","customer_id","region_id","date_of_birth")

@F.pandas_udf(T.StructType([
    T.StructField("age_bracket",          T.StringType(), False),
    T.StructField("income_bracket",       T.StringType(), False),
    T.StructField("loyalty_tier",         T.StringType(), False),
    T.StructField("acquisition_channel",  T.StringType(), False),
    T.StructField("annual_income_usd",    T.DoubleType(), False),
    T.StructField("nps_score",            T.IntegerType(), True),
]))
def fake_demographics(idxs: pd.Series) -> pd.DataFrame:
    rng = np.random.default_rng(321)
    age_brackets   = ["18-24","25-34","35-44","45-54","55+"]
    age_weights    = [0.10, 0.30, 0.25, 0.20, 0.15]
    income_ranges  = {
        "18-24": ("<$30K", 15000, 29999),
        "25-34": ("$30K-$60K", 30000, 59999),
        "35-44": ("$60K-$100K", 60000, 99999),
        "45-54": ("$100K-$200K", 100000, 199999),
        "55+":   ("$200K+", 200000, 399999),
    }
    income_weights = {"18-24": 0.75, "25-34": 0.55, "35-44": 0.40, "45-54": 0.30, "55+": 0.20}
    tiers   = ["Bronze","Silver","Gold","Platinum"]
    t_wt    = [0.40, 0.30, 0.20, 0.10]
    channels= ["organic_search","paid_search","social_media","referral","email_campaign"]
    c_wt    = [0.35, 0.25, 0.20, 0.15, 0.05]
    rows = []
    for i in idxs:
        age_b = rng.choice(age_brackets, p=age_weights)
        inc_label, inc_lo, inc_hi = income_ranges[age_b]
        # Sometimes downgrade income bracket
        if rng.random() < income_weights[age_b]:
            inc_label = "<$30K"; inc_lo = 10000; inc_hi = 29999
        annual_income = float(rng.integers(inc_lo, inc_hi))
        rows.append({
            "age_bracket":         age_b,
            "income_bracket":      inc_label,
            "loyalty_tier":        rng.choice(tiers, p=t_wt),
            "acquisition_channel": rng.choice(channels, p=c_wt),
            "annual_income_usd":   annual_income,
            "nps_score":           int(rng.integers(1, 11)),  # 1-10
        })
    return pd.DataFrame(rows)

df_demographics = (
    df_cust_ref
    .withColumn("demo", fake_demographics(F.col("customer_idx")))
    .select(
        "customer_id",
        F.col("demo.age_bracket"),
        F.col("demo.income_bracket"),
        F.col("demo.loyalty_tier"),
        F.col("demo.acquisition_channel"),
        F.col("demo.annual_income_usd"),
        F.col("demo.nps_score"),
    )
)
df_demographics.write.mode("overwrite").parquet(f"{BASE_PATH}/customer_demographics")
print(f"✓ customer_demographics: {df_demographics.count()} rows")

# ── customer_addresses ────────────────────────────────────────────────────────
@F.pandas_udf(T.ArrayType(T.StructType([
    T.StructField("address_type",  T.StringType()),
    T.StructField("address_line1", T.StringType()),
    T.StructField("city",          T.StringType()),
    T.StructField("state_province",T.StringType()),
    T.StructField("postcode",      T.StringType()),
    T.StructField("is_primary",    T.BooleanType()),
])))
def fake_addresses(idxs: pd.Series, countries: pd.Series) -> pd.Series:
    from faker import Faker
    result = []
    for i, cc in zip(idxs, countries):
        try:
            fake = Faker({"US":"en_US","GB":"en_GB","DE":"de_DE","FR":"fr_FR",
                          "JP":"ja_JP","CN":"zh_CN","IN":"hi_IN","BR":"pt_BR"}.get(cc,"en_US"))
        except Exception:
            fake = Faker("en_US")
        fake.seed_instance(int(i) + 5000)
        n_addrs = 2 if int(i) % 3 == 0 else 1
        addrs = []
        for j in range(n_addrs):
            addrs.append({
                "address_type":   "billing" if j == 0 else "shipping",
                "address_line1":  fake.street_address()[:80],
                "city":           fake.city()[:50],
                "state_province": fake.state()[:50] if hasattr(fake, "state") else "",
                "postcode":       fake.postcode()[:10],
                "is_primary":     j == 0,
            })
        result.append(addrs)
    return pd.Series(result)

df_addresses = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_customers").select("customer_idx","customer_id","country_code")
    .withColumn("addrs", fake_addresses(F.col("customer_idx"), F.col("country_code")))
    .select("customer_id", F.explode("addrs").alias("a"), "country_code")
    .select(
        F.monotonically_increasing_id().alias("address_id"),
        "customer_id",
        "country_code",
        F.col("a.address_type"),
        F.col("a.address_line1"),
        F.col("a.city"),
        F.col("a.state_province"),
        F.col("a.postcode"),
        F.col("a.is_primary"),
    )
)
df_addresses.write.mode("overwrite").parquet(f"{BASE_PATH}/customer_addresses")
print(f"✓ customer_addresses: {df_addresses.count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — ORDERS + ORDER ITEMS
# ═══════════════════════════════════════════════════════════════════════════

# Monthly order volumes with Q4 spike and seasonal variation
# Start: Sep 2024 (month 0) → Sep 2026 (month 24)
import calendar

month_multipliers = {}
base_per_month = N_ORDERS / 24.0
for m in range(24):
    dt = START_DATE + relativedelta(months=m)
    month_num = dt.month
    if month_num in (10, 11, 12):   mult = 1.5   # Q4 spike
    elif month_num in (1,):          mult = 0.75  # January dip
    elif month_num in (6, 7, 8):     mult = 1.15  # Summer peak
    else:                            mult = 1.0
    month_multipliers[m] = round(base_per_month * mult)

# Normalize to exactly N_ORDERS
total = sum(month_multipliers.values())
scale = N_ORDERS / total
for k in month_multipliers:
    month_multipliers[k] = max(1, round(month_multipliers[k] * scale))
# Fine-tune last month
diff = N_ORDERS - sum(month_multipliers.values())
month_multipliers[23] += diff

order_rows = []
order_idx = 0
for m in range(24):
    dt = START_DATE + relativedelta(months=m)
    days_in_month = calendar.monthrange(dt.year, dt.month)[1]
    count = month_multipliers[m]
    import random as pyrandom
    pyrandom.seed(m)
    for _ in range(count):
        day  = pyrandom.randint(1, days_in_month)
        hour = pyrandom.randint(0, 23)
        order_date = datetime(dt.year, dt.month, day, hour, pyrandom.randint(0,59))
        cust_idx   = pyrandom.randint(0, N_CUSTOMERS - 1)
        channel    = pyrandom.choices(
            ["web","mobile","partner_api"],
            weights=[0.55, 0.35, 0.10])[0]
        status     = pyrandom.choices(
            ["delivered","shipped","confirmed","cancelled","pending"],
            weights=[0.65, 0.15, 0.10, 0.08, 0.02])[0]
        est_delivery = order_date + timedelta(days=pyrandom.randint(3, 14))
        order_rows.append((
            order_idx,
            f"ORD-{str(order_idx).zfill(7)}",
            cust_idx,
            order_date,
            est_delivery if status != "cancelled" else None,
            channel,
            status,
            None,  # total_amount set after items
        ))
        order_idx += 1

orders_schema = T.StructType([
    T.StructField("order_idx",             T.IntegerType(),   False),
    T.StructField("order_id",              T.StringType(),    False),
    T.StructField("customer_idx",          T.IntegerType(),   False),
    T.StructField("order_date",            T.TimestampType(), False),
    T.StructField("estimated_delivery",    T.TimestampType(), True),
    T.StructField("channel",               T.StringType(),    False),
    T.StructField("status",                T.StringType(),    False),
    T.StructField("order_total",           T.DoubleType(),    True),
])
df_orders_base = spark.createDataFrame(order_rows, schema=orders_schema)
df_orders_base.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")
print(f"✓ stg_orders staged: {df_orders_base.count()} rows")

# ── order_items ───────────────────────────────────────────────────────────────
# 2-4 items per order; items_per_order distributed as 2(50%), 3(35%), 4(15%)
# Faulty batch products (idx 192-199) injected into Q3 2025 APAC orders
df_orders_ref = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")

@F.pandas_udf(T.ArrayType(T.StructType([
    T.StructField("product_idx",    T.IntegerType()),
    T.StructField("quantity",       T.IntegerType()),
    T.StructField("unit_price",     T.DoubleType()),
    T.StructField("discount_pct",   T.DoubleType()),
])))
def gen_order_items(order_idxs: pd.Series, order_dates: pd.Series, cust_idxs: pd.Series) -> pd.Series:
    rng = np.random.default_rng(999)
    # price map (simplified - use product_idx → approx price)
    def approx_price(pidx):
        if pidx < 44:   return float(rng.uniform(49.99, 1299.99))
        elif pidx < 100: return float(rng.uniform(9.99, 199.99))
        else:            return float(rng.uniform(4.99, 99.99))

    results = []
    for oidx, odate, cidx in zip(order_idxs, order_dates, cust_idxs):
        n_items = rng.choice([2, 3, 4], p=[0.50, 0.35, 0.15])
        items = []
        is_q3_2025 = (datetime(2025,7,1) <= odate.to_pydatetime() <= datetime(2025,9,30))
        is_apac_east = (int(cidx) % 7 == 4)  # ~14% chance of APAC-East
        used_products = set()
        for j in range(int(n_items)):
            # Inject faulty products into Q3 2025 APAC-East orders
            if is_q3_2025 and is_apac_east and j == 0 and rng.random() < 0.4:
                pidx = int(rng.choice(FAULTY_IDX))
            else:
                pidx = int(rng.integers(0, N_PRODUCTS))
            # Avoid duplicates
            while pidx in used_products:
                pidx = int(rng.integers(0, N_PRODUCTS))
            used_products.add(pidx)
            qty = int(rng.choice([1,2,3,4,5], p=[0.60,0.25,0.08,0.05,0.02]))
            items.append({
                "product_idx": pidx,
                "quantity":    qty,
                "unit_price":  round(approx_price(pidx), 2),
                "discount_pct": round(float(rng.choice([0,0,0,0,5,10,15,20], p=[0.4,0.15,0.1,0.05,0.1,0.1,0.05,0.05])), 2),
            })
        results.append(items)
    return pd.Series(results)

df_items_raw = (
    df_orders_ref.select("order_idx","order_id","order_date","customer_idx")
    .withColumn("items", gen_order_items(F.col("order_idx"), F.col("order_date"), F.col("customer_idx")))
    .select("order_id", "order_date", F.posexplode("items").alias("item_pos", "item"))
    .select(
        F.concat(F.col("order_id"), F.lit("-"), (F.col("item_pos") + 1).cast("string")).alias("line_id"),
        "order_id",
        F.col("item.product_idx"),
        F.concat(F.lit("PROD-"), F.lpad(F.col("item.product_idx").cast("string"), 5, "0")).alias("product_id"),
        F.col("item.quantity"),
        F.col("item.unit_price"),
        F.col("item.discount_pct"),
        F.round(
            F.col("item.quantity") * F.col("item.unit_price")
            * (1 - F.col("item.discount_pct") / 100), 2
        ).alias("line_total"),
        # Note: order_date intentionally excluded from stg_order_items
        # to avoid AMBIGUOUS_REFERENCE when joining with stg_orders later.
    )
)
df_items_raw.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items")

# Update order totals
df_order_totals = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items")
    .groupBy("order_id")
    .agg(F.round(F.sum("line_total"), 2).alias("order_total"))
)
df_orders_final = (
    df_orders_base.drop("order_total")
    .join(df_order_totals, "order_id", "left")
)
df_orders_final.write.mode("overwrite").parquet(f"{BASE_PATH}/orders")
df_orders_final.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")

# Write order_items (without staging-only columns)
(
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items")
    .drop("order_date", "product_idx")
    .write.mode("overwrite").parquet(f"{BASE_PATH}/order_items")
)
print(f"✓ orders: {df_orders_final.count()} rows")
print(f"✓ order_items: {spark.table(f'{CATALOG}.{RAW_SCHEMA}.stg_order_items').count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — INVOICES + LINE ITEMS + PAYMENTS
# ═══════════════════════════════════════════════════════════════════════════

# Invoices: one per confirmed/shipped/delivered order
df_invoiceable = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")
    .filter(F.col("status").isin("confirmed","shipped","delivered"))
)
invoice_count = df_invoiceable.count()

df_invoices = (
    df_invoiceable
    .withColumn("invoice_id",
        F.concat(F.lit("INV-"), F.lpad(F.monotonically_increasing_id().cast("string"), 7, "0")))
    .withColumn("invoice_number",
        F.concat(F.lit("NR-"), F.year("order_date").cast("string"),
                 F.lit("-"), F.lpad(F.monotonically_increasing_id().cast("string"), 6, "0")))
    .withColumn("issue_date",     F.date_add(F.to_date("order_date"), 1))
    .withColumn("due_date",       F.date_add(F.to_date("order_date"), 30))
    .withColumn("invoice_status",
        F.when(F.col("status") == "delivered",
               F.when(F.rand(42) < 0.05, "overdue").otherwise("paid"))
         .otherwise("pending"))
    # Introduce 40 NULL total_amounts for DQX demo
    .withColumn("invoice_total",
        F.when(F.abs(F.hash(F.col("order_id"))) % 100 < (40 * 100 / invoice_count),
               F.lit(None).cast(T.DoubleType()))
         .otherwise(F.col("order_total")))
    .select("invoice_id","invoice_number","order_id","issue_date","due_date",
            "invoice_status","invoice_total","order_total")
)
df_invoices.write.mode("overwrite").parquet(f"{BASE_PATH}/invoices")
df_invoices.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_invoices")
print(f"✓ invoices: {df_invoices.count()} rows  (~40 NULL totals for DQX)")

# ── invoice_line_items ────────────────────────────────────────────────────────
df_inv_ref = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_invoices").select("invoice_id","order_id","issue_date")
df_oi_ref  = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items").select("line_id","order_id","product_id","quantity","unit_price","discount_pct","line_total")

# Tax rate by product category (simplified: electronics 10%, food 0%, others 8%)
df_inv_lines = (
    df_inv_ref.join(df_oi_ref, "order_id")
    .withColumn("inv_line_id",
        F.concat(F.col("invoice_id"), F.lit("-"), F.col("line_id")))
    .withColumn("tax_rate",
        F.when(F.col("product_id").startswith("01"), 0.10)  # Electronics
         .when(F.col("product_id").startswith("08"), 0.00)  # Food
         .otherwise(0.08))
    .withColumn("tax_amount",     F.round(F.col("line_total") * F.col("tax_rate"), 2))
    .withColumn("extended_price", F.round(F.col("line_total") + F.col("tax_amount"), 2))
    .select("inv_line_id","invoice_id","line_id","product_id","quantity",
            "unit_price","discount_pct","line_total","tax_rate","tax_amount","extended_price")
)
df_inv_lines.write.mode("overwrite").parquet(f"{BASE_PATH}/invoice_line_items")
print(f"✓ invoice_line_items: {df_inv_lines.count()} rows")

# ── payments ─────────────────────────────────────────────────────────────────
payment_methods = ["credit_card","debit_card","paypal","bank_transfer","buy_now_pay_later"]
payment_weights = [0.40, 0.20, 0.20, 0.15, 0.05]

df_payments = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")
    .withColumn("payment_id",
        F.concat(F.lit("PAY-"), F.lpad(F.monotonically_increasing_id().cast("string"), 7, "0")))
    .withColumn("payment_method",
        F.element_at(
            F.array([F.lit(m) for m in payment_methods]),
            (F.abs(F.hash(F.col("order_id"))) % len(payment_methods) + 1).cast("int")
        ))
    .withColumn("payment_status",
        # 3% failed payments for DQX demo
        F.when(F.abs(F.hash(F.col("order_id"))) % 100 < 3, "failed")
         .when(F.col("status") == "delivered", "completed")
         .when(F.col("status") == "cancelled", "refunded")
         .otherwise("pending"))
    .withColumn("payment_date",  F.date_add(F.to_date("order_date"), 1).cast(T.TimestampType()))
    .withColumn("currency_code",
        F.when(F.col("customer_idx") % 7 == 2, "EUR")   # EMEA-West
         .when(F.col("customer_idx") % 7 == 0, "CAD")   # AMER-North variant
         .otherwise("USD"))
    .withColumn("gateway_ref",
        F.concat(F.lit("GW-"), F.upper(F.substring(F.md5(F.col("order_id")), 1, 12))))
    .select("payment_id","order_id","payment_method","payment_status",
            "payment_date","order_total","currency_code","gateway_ref")
)
df_payments.write.mode("overwrite").parquet(f"{BASE_PATH}/payments")
print(f"✓ payments: {df_payments.count()} rows  (3% failed for DQX)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 — RETURNS + RETURN ITEMS
# ═══════════════════════════════════════════════════════════════════════════
#
# Story: 72 of 120 returns trace to faulty_batch products (SKUs FAULT-*)
# Peak period: Q4 2025 (customers returning Q3 purchases)
# APAC-East region: 45 of the 72 faulty returns
# ─────────────────────────────────────────────────────────────────────────

# Find orders containing faulty batch products (from Q3 2025)
df_faulty_orders = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items")
    .filter(F.col("product_idx").isin(FAULTY_IDX))
    .select("order_id")
    .distinct()
    .withColumn("is_faulty_order", F.lit(True))
)

# Get delivered/shipped orders (returnable)
df_returnable = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")
    .filter(F.col("status").isin("delivered","shipped"))
    .join(df_faulty_orders, "order_id", "left")
    .withColumn("is_faulty_order", F.coalesce(F.col("is_faulty_order"), F.lit(False)))
    .select("order_idx","order_id","order_date","customer_idx","is_faulty_order")
    .orderBy(F.col("is_faulty_order").desc(), F.rand(42))  # faulty orders first
)

# Sample returns: first 72 from faulty orders, then 48 from non-faulty
df_faulty_returns_src = df_returnable.filter(F.col("is_faulty_order")).limit(72)
df_normal_returns_src = df_returnable.filter(~F.col("is_faulty_order")).limit(48)
df_return_src = df_faulty_returns_src.unionByName(df_normal_returns_src)

return_reasons = {
    True:  ["faulty_product","faulty_product","faulty_product","damaged_in_transit","wrong_item"],
    False: ["changed_mind","wrong_item","damaged_in_transit","not_as_described","changed_mind"],
}
return_statuses = ["approved","pending","rejected","completed"]
ret_status_wts  = [0.60, 0.20, 0.10, 0.10]

df_returns = (
    df_return_src
    .withColumn("return_id",
        F.concat(F.lit("RET-"), F.lpad(F.monotonically_increasing_id().cast("string"), 6, "0")))
    .withColumn("return_date",
        F.when(F.col("is_faulty_order"),
               # Q4 2025 returns (faulty products shipped Q3)
               F.lit("2025-10-15").cast(T.DateType()) +
               F.expr("cast(abs(hash(order_id)) % 77 as int)"))
         .otherwise(
               F.to_date("order_date") + F.expr("cast(abs(hash(order_id)) % 30 + 5 as int)")))
    .withColumn("return_reason_code",
        F.when(F.col("is_faulty_order"),
               F.element_at(F.array([F.lit(r) for r in return_reasons[True]]),
                            (F.abs(F.hash(F.col("order_id"))) % 5 + 1).cast("int")))
         .otherwise(
               F.element_at(F.array([F.lit(r) for r in return_reasons[False]]),
                            (F.abs(F.hash(F.col("order_id"))) % 5 + 1).cast("int"))))
    .withColumn("return_status",
        F.element_at(F.array([F.lit(s) for s in return_statuses]),
                     (F.abs(F.hash(F.col("order_id"))) % 4 + 1).cast("int")))
    .withColumn("refund_amount",
        F.when(F.col("return_status") == "approved",
               F.round(F.rand(77) * 150 + 20, 2))
         .otherwise(F.lit(None).cast(T.DoubleType())))
    .select("return_id","order_id","return_date","return_reason_code",
            "return_status","refund_amount","is_faulty_order")
)
df_returns.write.mode("overwrite").parquet(f"{BASE_PATH}/returns")
df_returns.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_returns")
print(f"✓ returns: {df_returns.count()} rows  ({df_returns.filter('is_faulty_order').count()} faulty batch)")

# ── return_items ──────────────────────────────────────────────────────────────
df_return_items = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_returns").select("return_id","order_id","is_faulty_order")
    .join(spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items").select("order_id","line_id","product_id","quantity","unit_price","product_idx"), "order_id")
    # For faulty returns, prefer faulty_batch products; otherwise take first item
    .withColumn("rank",
        F.row_number().over(
            Window.partitionBy("return_id")
            .orderBy(
                F.when(F.col("is_faulty_order") & F.col("product_idx").isin(FAULTY_IDX),
                       F.lit(0)).otherwise(F.lit(1)),
                F.col("line_id")
            )
        )
    )
    .filter(F.col("rank") <= 2)  # 1-2 items per return
    .withColumn("return_item_id",
        F.concat(F.col("return_id"), F.lit("-"), F.col("rank").cast("string")))
    .withColumn("condition",
        F.element_at(
            F.array(F.lit("unopened"), F.lit("opened"), F.lit("damaged"), F.lit("defective")),
            (F.abs(F.hash(F.col("return_id"))) % 4 + 1).cast("int")))
    .select("return_item_id","return_id","line_id","product_id","quantity","unit_price","condition")
)
df_return_items.write.mode("overwrite").parquet(f"{BASE_PATH}/return_items")
print(f"✓ return_items: {df_return_items.count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 — PROMOTIONS + REDEMPTIONS
# ═══════════════════════════════════════════════════════════════════════════

promo_types    = ["percentage_off","buy_X_get_Y","free_shipping","bundle_deal","flash_sale"]
promo_wts      = [0.40, 0.20, 0.20, 0.10, 0.10]
discount_ranges = {
    "percentage_off": (5.0, 30.0),
    "buy_X_get_Y": (0.0, 0.0),
    "free_shipping": (0.0, 0.0),
    "bundle_deal": (10.0, 25.0),
    "flash_sale": (20.0, 50.0),
}

promo_data = []
import random as pyrandom
pyrandom.seed(55)
for i in range(N_PROMOTIONS):
    ptype = pyrandom.choices(promo_types, weights=promo_wts)[0]
    start = START_DATE + timedelta(days=pyrandom.randint(0, 600))
    end   = start + timedelta(days=pyrandom.randint(7, 60))
    lo, hi = discount_ranges[ptype]
    disc   = round(pyrandom.uniform(lo, hi), 2) if hi > 0 else 0.0
    promo_data.append((
        f"PROMO-{str(i).zfill(4)}",
        f"NexusRetail {ptype.replace('_',' ').title()} {i+1}",
        ptype,
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
        disc,
        True if end > END_DATE else pyrandom.random() < 0.7,
    ))
df_promotions = spark.createDataFrame(promo_data, schema=T.StructType([
    T.StructField("promo_id",         T.StringType(), False),
    T.StructField("promo_name",       T.StringType(), False),
    T.StructField("promo_type",       T.StringType(), False),
    T.StructField("start_date",       T.StringType(), False),
    T.StructField("end_date",         T.StringType(), False),
    T.StructField("discount_pct",     T.DoubleType(), False),
    T.StructField("is_active",        T.BooleanType(), False),
]))
df_promotions.write.mode("overwrite").parquet(f"{BASE_PATH}/promotions")
df_promotions.write.mode("overwrite").saveAsTable(f"{CATALOG}.{RAW_SCHEMA}.stg_promotions")

# Promotion redemptions: ~600 (orders × promos)
df_promo_ids = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_promotions").select("promo_id")
df_order_ids = spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders").select("order_id","order_date").limit(600)
df_redemptions = (
    df_order_ids
    .withColumn("promo_id",
        F.element_at(
            F.array([F.lit(r[0]) for r in df_promo_ids.collect()]),
            (F.abs(F.hash(F.col("order_id"))) % N_PROMOTIONS + 1).cast("int")
        ))
    .withColumn("redeemed_at", F.col("order_date").cast(T.TimestampType()))
    .withColumn("redemption_id",
        F.concat(F.lit("REDEM-"), F.lpad(F.monotonically_increasing_id().cast("string"), 6, "0")))
    .select("redemption_id","order_id","promo_id","redeemed_at")
)
df_redemptions.write.mode("overwrite").parquet(f"{BASE_PATH}/promotion_redemptions")
print(f"✓ promotions: {df_promotions.count()} rows")
print(f"✓ promotion_redemptions: {df_redemptions.count()} rows")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 — PRODUCT REVIEWS (NPS/Sentiment)
# ═══════════════════════════════════════════════════════════════════════════
#
# Story: Faulty batch products (ELEC-*) see 1-2 star spike from Q4 2025 onward

@F.pandas_udf(T.StringType())
def fake_review_text(idxs: pd.Series, ratings: pd.Series, is_faultys: pd.Series) -> pd.Series:
    positive_snippets = [
        "Absolutely love this product! Exceeded my expectations.",
        "Great quality and fast delivery. Will buy again!",
        "Five stars — does exactly what it says on the box.",
        "Really pleased with this purchase. Solid build quality.",
        "Best in class for the price. Highly recommended.",
    ]
    negative_snippets = [
        "Stopped working after just 2 weeks. Very disappointed.",
        "Product is defective — does not charge properly.",
        "Screen flickered and died. Returning for a refund.",
        "Total waste of money. Faulty from the start.",
        "Arrived broken. Customer service was slow to respond.",
    ]
    neutral_snippets = [
        "Decent product for the price. Nothing exceptional.",
        "Works as described. Shipping took longer than expected.",
        "It's okay. Not sure I'd buy again but not terrible either.",
    ]
    results = []
    for i, (rating, is_faulty) in enumerate(zip(ratings, is_faultys)):
        import random as r; r.seed(int(i) + 9999)
        if is_faulty and rating <= 2:
            results.append(r.choice(negative_snippets))
        elif rating >= 4:
            results.append(r.choice(positive_snippets))
        elif rating == 3:
            results.append(r.choice(neutral_snippets))
        else:
            results.append(r.choice(negative_snippets))
    return pd.Series(results)

# Get order + product combinations for reviews
df_ordered_products = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_order_items")
    .join(spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders").select("order_id","customer_idx","order_date","status"), "order_id")
    .filter(F.col("status") == "delivered")
    .select("order_id","product_id","product_idx","customer_idx","order_date")
    .limit(N_REVIEWS)
)

df_reviews = (
    df_ordered_products
    .withColumn("review_id",
        F.concat(F.lit("REV-"), F.lpad(F.monotonically_increasing_id().cast("string"), 7, "0")))
    .withColumn("is_faulty_product", F.col("product_idx").isin(FAULTY_IDX))
    .withColumn("review_date",
        F.to_date("order_date") + F.expr("cast(abs(hash(order_id)) % 30 + 5 as int)"))
    # Ratings: faulty products spike at 1-2 stars post-Q4 2025
    .withColumn("rating",
        F.when(F.col("is_faulty_product") & (F.to_date("order_date") >= F.lit("2025-10-01")),
               (F.abs(F.hash(F.col("review_id"))) % 2 + 1).cast("int"))  # 1 or 2
         .otherwise(
               F.element_at(
                   F.array(F.lit(1),F.lit(2),F.lit(3),F.lit(4),F.lit(5)),
                   (F.abs(F.hash(F.col("review_id"))) % 100).cast("int") % 5 + 1
               )))
    .withColumn("verified_purchase", F.lit(True))
    .withColumn("helpful_votes",
        F.when(F.col("is_faulty_product"), (F.abs(F.hash(F.col("review_id"))) % 50 + 5).cast("int"))
         .otherwise((F.abs(F.hash(F.col("review_id"))) % 10).cast("int")))
    .withColumn("review_text",
        fake_review_text(F.monotonically_increasing_id().cast("long"),
                         F.col("rating"), F.col("is_faulty_product").cast("string")))
    .select("review_id","product_id","customer_idx","review_date","rating",
            "review_text","verified_purchase","helpful_votes","is_faulty_product")
)
df_reviews.write.mode("overwrite").parquet(f"{BASE_PATH}/product_reviews")
print(f"✓ product_reviews: {df_reviews.count()} rows  (faulty products have 1-2 star spike)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 9 — CUSTOMER SUPPORT TICKETS
# ═══════════════════════════════════════════════════════════════════════════
#
# Story: Ticket volume spikes 3x in Q4 2025 (returns + complaints from faulty batch)

# Monthly ticket distribution mirrors orders but with Q4 2025 spike
ticket_months = {}
base_tickets = N_TICKETS / 24.0
for m in range(24):
    dt = START_DATE + relativedelta(months=m)
    is_faulty_spike = (datetime(2025,10,1) <= dt <= datetime(2026,1,31))
    mult = 3.0 if is_faulty_spike else (1.5 if dt.month in (10,11,12) else 1.0)
    ticket_months[m] = max(1, round(base_tickets * mult))
# Normalize
total_t = sum(ticket_months.values())
scale_t = N_TICKETS / total_t
for k in ticket_months:
    ticket_months[k] = max(1, round(ticket_months[k] * scale_t))
diff_t = N_TICKETS - sum(ticket_months.values())
ticket_months[23] += diff_t

ticket_categories = ["product_defect","delivery_issue","billing_query","general_enquiry","return_request"]
ticket_weights    = {
    "normal":  [0.10, 0.25, 0.25, 0.30, 0.10],
    "faulty_period": [0.40, 0.15, 0.10, 0.10, 0.25],
}
ticket_priorities = ["low","medium","high","critical"]
ticket_statuses   = ["open","in_progress","resolved","closed"]

import random as pyrandom
ticket_rows = []
t_idx = 0
for m in range(24):
    dt = START_DATE + relativedelta(months=m)
    is_fp = (datetime(2025,10,1) <= dt <= datetime(2026,1,31))
    cats  = ticket_categories
    wts   = ticket_weights["faulty_period"] if is_fp else ticket_weights["normal"]
    days  = calendar.monthrange(dt.year, dt.month)[1]
    for _ in range(ticket_months[m]):
        pyrandom.seed(t_idx * 7 + m)
        created = datetime(dt.year, dt.month, pyrandom.randint(1, days),
                           pyrandom.randint(8, 20), pyrandom.randint(0, 59))
        cat     = pyrandom.choices(cats, weights=wts)[0]
        is_escalated = cat == "product_defect" and is_fp
        priority = pyrandom.choices(ticket_priorities,
                                    weights=[0.10,0.30,0.40,0.20] if is_escalated
                                            else [0.30,0.40,0.20,0.10])[0]
        status   = pyrandom.choices(ticket_statuses, weights=[0.05,0.10,0.60,0.25])[0]
        resolved = (created + timedelta(hours=pyrandom.randint(1, 72))).strftime("%Y-%m-%d %H:%M:%S") \
                   if status in ("resolved","closed") else None
        ticket_rows.append((
            f"TKT-{str(t_idx).zfill(7)}",
            pyrandom.randint(0, N_CUSTOMERS - 1),
            None,  # order_id joined later
            cat, priority, status,
            created.strftime("%Y-%m-%d %H:%M:%S"),
            resolved,
            is_escalated,
        ))
        t_idx += 1

df_tickets = spark.createDataFrame(ticket_rows, schema=T.StructType([
    T.StructField("ticket_id",          T.StringType(),    False),
    T.StructField("customer_idx",       T.IntegerType(),   False),
    T.StructField("order_id",           T.StringType(),    True),
    T.StructField("category",           T.StringType(),    False),
    T.StructField("priority",           T.StringType(),    False),
    T.StructField("status",             T.StringType(),    False),
    T.StructField("created_at",         T.StringType(),    False),
    T.StructField("resolved_at",        T.StringType(),    True),
    T.StructField("is_faulty_escalation", T.BooleanType(), False),
]))

# Join ~30% of tickets to an order (for correlation analysis)
df_order_sample = (
    spark.table(f"{CATALOG}.{RAW_SCHEMA}.stg_orders")
    .select("order_id","customer_idx")
    .withColumn("rn", F.row_number().over(Window.partitionBy("customer_idx").orderBy(F.rand(42))))
    .filter(F.col("rn") == 1)
)
df_tickets_final = (
    df_tickets.drop("order_id")
    .join(df_order_sample.select("customer_idx","order_id"), "customer_idx", "left")
    .withColumn("order_id",
        F.when(F.abs(F.hash(F.col("ticket_id"))) % 100 < 30, F.col("order_id"))
         .otherwise(F.lit(None).cast(T.StringType())))
    .withColumn("customer_id",
        F.concat(F.lit("CUST-"), F.lpad(F.col("customer_idx").cast("string"), 6, "0")))
    .select("ticket_id","customer_id","order_id","category","priority",
            "status","created_at","resolved_at","is_faulty_escalation")
)
df_tickets_final.write.mode("overwrite").parquet(f"{BASE_PATH}/customer_support_tickets")
print(f"✓ customer_support_tickets: {df_tickets_final.count()} rows  (3x Q4 2025 spike)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════════════
# SECTION 10 — CLEANUP STAGING TABLES + VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

# Drop staging tables (no longer needed)
for tbl in ["stg_products","stg_customers","stg_orders","stg_order_items",
            "stg_invoices","stg_returns","stg_promotions"]:
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{RAW_SCHEMA}`.`{tbl}`")

# Quick validation
tables = [
    "ref_regions","ref_countries","product_categories","product_subcategories",
    "products","product_pricing","customers","customer_demographics","customer_addresses",
    "orders","order_items","invoices","invoice_line_items","payments",
    "returns","return_items","promotions","promotion_redemptions",
    "product_reviews","customer_support_tickets",
]
print("\n" + "="*55)
print("VALIDATION — Row counts per table:")
print("="*55)
total_rows = 0
for t in tables:
    cnt = spark.read.parquet(f"{BASE_PATH}/{t}").count()
    total_rows += cnt
    print(f"  {t:<35} {cnt:>8,}")
print(f"\n  {'TOTAL':35} {total_rows:>8,} rows")
print("="*55)
print("\n✅ NexusRetail raw data generation complete!")
print(f"   Location: {BASE_PATH}")
print(f"   Tables  : {len(tables)}")
