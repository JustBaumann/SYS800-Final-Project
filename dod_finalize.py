import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

OUTPUT_DIR = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\output"

# Schema to read everything back safely
PYARROW_SCHEMA = pa.schema([
    ('piid', pa.string()), ('modification_number', pa.string()), ('action_date', pa.timestamp('ms')),
    ('federal_action_obligation', pa.float64()), ('product_or_service_code', pa.string()),
    ('recipient_uei', pa.string()), ('recipient_duns', pa.string()), ('recipient_name', pa.string()),
    ('pop_state', pa.string()), ('pop_country', pa.string()), ('extent_competed', pa.string()),
    ('type_of_contract_pricing', pa.string()), ('fiscal_year', pa.int32()), ('fiscal_month', pa.int32()),
    ('vendor_id', pa.string()), ('psc_category', pa.string()), ('obligation_real', pa.float64())
])

def main():
    clean_dir = os.path.join(OUTPUT_DIR, "parquet_clean")
    stats_dir = os.path.join(OUTPUT_DIR, "aggregates")
    os.makedirs(stats_dir, exist_ok=True)

    logger.info("FINALIZING: Generating Summary Tables from cleaned data...")

    try:
        # Read the entire partitioned dataset back in
        df = pd.read_parquet(clean_dir, schema=PYARROW_SCHEMA)
    except Exception as e:
        logger.error(f"Failed to read cleaned data. Did the cleaning pipeline run? {e}")
        return

    # Filter out year 0 artifact from bad dates
    df = df[df['fiscal_year'] > 2000].copy()

    # The column to be aggregated is 'obligation_real'
    REAL_OBLIGATION_COL = 'obligation_real'
    FINAL_COL_NAME = 'obligation_real_fy25' # The name expected by the plot script

    # 1. Annual Totals
    logger.info("Generating Annual Totals...")
    annual = df.groupby('fiscal_year')[['federal_action_obligation', REAL_OBLIGATION_COL]].sum().reset_index()
    annual = annual.rename(columns={REAL_OBLIGATION_COL: FINAL_COL_NAME}) # Rename for final consistency
    annual.to_parquet(os.path.join(stats_dir, 'annual_totals.parquet'), index=False)
    annual.to_csv(os.path.join(stats_dir, 'annual_totals.csv'), index=False)

    # 2. Vendor Totals
    logger.info("Generating Vendor Totals...")
    vendor_cols = ['fiscal_year', 'vendor_id']
    vendor = df.groupby(vendor_cols)[REAL_OBLIGATION_COL].sum().reset_index()
    vendor = vendor.rename(columns={REAL_OBLIGATION_COL: FINAL_COL_NAME})
    vendor.to_parquet(os.path.join(stats_dir, 'vendor_totals.parquet'), index=False)

    # 3. PSC Totals
    logger.info("Generating PSC Totals...")
    psc = df.groupby(['fiscal_year', 'psc_category'])[REAL_OBLIGATION_COL].sum().reset_index()
    psc = psc.rename(columns={REAL_OBLIGATION_COL: FINAL_COL_NAME})
    psc.to_parquet(os.path.join(stats_dir, 'psc_totals.parquet'), index=False)

    # 4. Geo Totals (Country and State)
    logger.info("Generating Geo Totals...")
    geo = df.groupby(['fiscal_year', 'pop_country', 'pop_state'])[REAL_OBLIGATION_COL].sum().reset_index()
    geo = geo.rename(columns={REAL_OBLIGATION_COL: FINAL_COL_NAME})
    geo.to_parquet(os.path.join(stats_dir, 'geography_totals.parquet'), index=False)

    # 5. Monthly Totals
    logger.info("Generating Monthly Totals...")
    monthly = df.groupby(['fiscal_year', 'fiscal_month'])[REAL_OBLIGATION_COL].sum().reset_index()
    monthly = monthly.rename(columns={REAL_OBLIGATION_COL: FINAL_COL_NAME})
    monthly.to_parquet(os.path.join(stats_dir, 'monthly_totals.parquet'), index=False)

    print("\n--- DONE. Summary (Annual Totals) ---")
    print(annual.to_string(index=False))

if __name__ == "__main__":
    main()