import os
import glob
import zipfile
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import io
from tqdm import tqdm

RAW_DATA_DIR = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\raw_data"
OUTPUT_DIR = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\output"
DEFLATOR_FILE = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\deflator\deflator.csv"

# Columns to keep (Standardized USAspending names, removing inconsistent ones)
USE_COLS = [
    'award_id_piid', 'modification_number', 'action_date',
    'federal_action_obligation',
    'recipient_name', 'recipient_uei', 'recipient_duns',
    'product_or_service_code',
    'primary_place_of_performance_country_code',
    'primary_place_of_performance_state_code',
    'extent_competed', 'type_of_contract_pricing'
]

# FIX: Explicitly define problematic columns as STRING to prevent float conversion
DTYPE_MAP = {
    'recipient_uei': str,
    'recipient_duns': str,
    'award_id_piid': str,
    'modification_number': str,
    'recipient_name': str,
    'product_or_service_code': str,
    'primary_place_of_performance_country_code': str,
    'primary_place_of_performance_state_code': str,
    'extent_competed': str,
    'type_of_contract_pricing': str
}

# Rename mapping for cleaner code usage
RENAME_MAP = {
    'award_id_piid': 'piid',
    'primary_place_of_performance_country_code': 'pop_country',
    'primary_place_of_performance_state_code': 'pop_state',
    'product_or_service_code': 'psc'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_deflators():
    """Loads GDP deflators from your CSV, robustly handling quotes and thousands separators."""
    if not os.path.exists(DEFLATOR_FILE):
        logger.warning(f"⚠️ Deflator file not found at: {DEFLATOR_FILE}")
        return {}

    try:
        # FIXED ROBUST READING: Reads file, strips outer quotes, and re-reads as clean CSV
        with open(DEFLATOR_FILE, 'r') as f:
            lines = [line.strip().strip('"') for line in f if line.strip()]

        d = pd.read_csv(io.StringIO('\n'.join(lines)), sep=',')

        d['fiscal_year'] = d['fiscal_year'].astype(str).str.replace(',', '', regex=False).astype(int)
        d['index'] = pd.to_numeric(d['index'], errors='coerce')

        return pd.Series(d['index'].values, index=d['fiscal_year']).to_dict()
    except Exception as e:
        logger.error(f"Error loading deflators: {e}")
        return {}

def process_chunk(chunk, deflators):
    """Cleans a single chunk of data."""
    # 1. Standardize Columns
    current_cols = set(chunk.columns)
    valid_rename_map = {k: v for k, v in RENAME_MAP.items() if k in current_cols}
    chunk = chunk.rename(columns=valid_rename_map)

    # 2. Date Parsing
    chunk['action_date'] = pd.to_datetime(chunk['action_date'], errors='coerce')
    chunk = chunk.dropna(subset=['action_date'])

    if chunk.empty: return chunk

    # 3. Calculate Fiscal Year (If Month >= 10, it is next FY)
    chunk['fiscal_year'] = np.where(
        chunk['action_date'].dt.month >= 10,
        chunk['action_date'].dt.year + 1,
        chunk['action_date'].dt.year
    )
    chunk['fiscal_month'] = chunk['action_date'].dt.month

    # 4. Clean Money & Apply Inflation Deflator
    chunk['federal_action_obligation'] = pd.to_numeric(chunk['federal_action_obligation'], errors='coerce').fillna(0.0)
    base_deflator = deflators.get(2025, 150.32)
    chunk_deflators = chunk['fiscal_year'].map(deflators).fillna(base_deflator)
    chunk['obligation_real'] = (chunk['federal_action_obligation'] / chunk_deflators) * base_deflator

    # 5. Vendor Identity (UEI -> DUNS -> Name)
    chunk['vendor_id'] = chunk['recipient_uei'].fillna(chunk['recipient_duns']).fillna(chunk['recipient_name']).fillna('UNKNOWN')

    # 6. PSC Categorization
    psc_str = chunk.get('psc', pd.Series(dtype=str)).astype(str).str.upper()
    chunk['psc_category'] = np.select(
        [psc_str.str.startswith('A'), psc_str.str[0].str.isalpha(), psc_str.str[0].str.isnumeric()],
        ['R&D', 'Service', 'Product'],
        default='Other'
    )

    return chunk

def process_file(file_path, file_type, deflators, schema):
    clean_dir = os.path.join(OUTPUT_DIR, "parquet_clean")

    try:
        if file_type == 'zip':
            with zipfile.ZipFile(file_path, 'r') as z:
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if not csv_files:
                    logger.warning(f"No CSV found in zip: {file_path}")
                    return

                csv_name = csv_files[0]
                logger.info(f"   -> Extracting stream: {csv_name}")
                with z.open(csv_name) as f:
                    # Applying DTYPE_MAP and usecols during read
                    reader = pd.read_csv(f, usecols=lambda x: x in USE_COLS, dtype=DTYPE_MAP, chunksize=100000, low_memory=False, encoding='utf-8')
                    process_reader(reader, deflators, schema, clean_dir)
        else:
            # For non-zip CSVs
            reader = pd.read_csv(file_path, usecols=lambda x: x in USE_COLS, dtype=DTYPE_MAP, chunksize=100000, low_memory=False)
            process_reader(reader, deflators, schema, clean_dir)

    except Exception as e:
        logger.error(f"FAILED to process {file_path}: {e}")

def process_reader(reader, deflators, schema, clean_dir):
    for chunk in reader:
        clean_chunk = process_chunk(chunk, deflators)
        if clean_chunk.empty: continue

        # Save to partitioned Parquet dataset
        table = pa.Table.from_pandas(clean_chunk, schema=schema, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=clean_dir,
            partition_cols=['fiscal_year'],
            existing_data_behavior='overwrite_or_ignore'
        )

def main():
    logger.info("--- STARTING PIPELINE ---")
    logger.info(f"Scanning for Zips in: {RAW_DATA_DIR}")

    os.makedirs(os.path.join(OUTPUT_DIR, "parquet_clean"), exist_ok=True)

    deflators = load_deflators()

    files = glob.glob(os.path.join(RAW_DATA_DIR, "*"))
    logger.info(f"Found {len(files)} files to process.")

    # Schema for Parquet (must match the columns created/kept in process_chunk)
    schema = pa.schema([
        ('piid', pa.string()), ('modification_number', pa.string()), ('action_date', pa.timestamp('ms')),
        ('federal_action_obligation', pa.float64()), ('obligation_real', pa.float64()),
        ('recipient_name', pa.string()), ('vendor_id', pa.string()),
        ('psc', pa.string()), ('psc_category', pa.string()),
        ('pop_country', pa.string()), ('pop_state', pa.string()),
        ('fiscal_year', pa.int32()), ('fiscal_month', pa.int32()),
        ('extent_competed', pa.string()), ('type_of_contract_pricing', pa.string()),
        ('recipient_uei', pa.string()), ('recipient_duns', pa.string()) # <-- FIXED PARENTHESIS HERE
    ])

    for f in tqdm(files, desc="Processing Files"):
        if f.endswith('.zip'):
            process_file(f, 'zip', deflators, schema)
        elif f.endswith('.csv'):
            process_file(f, 'csv', deflators, schema)

    logger.info("PIPELINE COMPLETE.")

if __name__ == "__main__":
    main()