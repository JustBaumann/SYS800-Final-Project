import os
import pandas as pd
import glob

OUTPUT_DIR = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\output"
CLEAN_DIR = os.path.join(OUTPUT_DIR, "parquet_clean")

def main():
    print(f"--- DIAGNOSTIC REPORT ---")
    print(f"Scanning: {CLEAN_DIR}")

    if not os.path.exists(CLEAN_DIR):
        print("CRITICAL ERROR: The 'parquet_clean' folder does not exist!")
        return

    # 1. Check Subfolders
    subfolders = [f.name for f in os.scandir(CLEAN_DIR) if f.is_dir()]
    print(f"\n1. Folders Found ({len(subfolders)}):")
    print(sorted(subfolders))

    # 2. Check for Year 0 (Bad Dates)
    if "fiscal_year=0" in subfolders:
        print("\nWARNING: Found 'fiscal_year=0'. This means dates were not parsed correctly.")
        # Count rows in year 0
        try:
            df0 = pd.read_parquet(os.path.join(CLEAN_DIR, "fiscal_year=0"))
            print(f" -> Rows lost in Year 0: {len(df0)}")
        except:
            print(" -> Could not read Year 0 data.")

    # 3. Check for Modern Years
    modern_years = [str(y) for y in range(2019, 2025)]
    found_modern = [y for y in modern_years if f"fiscal_year={y}" in subfolders]

    if found_modern:
        print(f"\nSUCCESS: Found modern folders: {found_modern}")
        # Try reading one file to ensure it's not empty
        test_year = found_modern[0]
        try:
            test_path = os.path.join(CLEAN_DIR, f"fiscal_year={test_year}")
            df_test = pd.read_parquet(test_path)
            print(f" -> Verification: Read {len(df_test)} rows from {test_year}.")
            print(" -> Sample Columns:", list(df_test.columns[:5]))
        except Exception as e:
            print(f" -> CRITICAL: Folder exists but cannot be read: {e}")
    else:
        print(f"\nFAILURE: No modern folders (2019-2024) found.")
        print("The Part 2 script ran, but it did not save the folders to disk.")

if __name__ == "__main__":
    main()