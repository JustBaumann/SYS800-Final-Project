import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = r"C:\Users\minij\OneDrive - stevens.edu\Desktop\SYS 800 Dates\data\output"
AGGREGATES_DIR = os.path.join(OUTPUT_DIR, "aggregates")
PLOT_DIR = os.path.join(OUTPUT_DIR, "plots")
ANNUAL_TOTALS_FILE = os.path.join(AGGREGATES_DIR, 'annual_totals.parquet')

os.makedirs(PLOT_DIR, exist_ok=True)

def generate_annual_spending_plot():
    """Loads annual data, prints the summary, and generates the final comparison plot."""
    if not os.path.exists(ANNUAL_TOTALS_FILE):
        logger.error(f"Required file not found: {ANNUAL_TOTALS_FILE}. Did dod_finalize.py run?")
        return

    try:
        df_annual = pd.read_parquet(ANNUAL_TOTALS_FILE)
    except Exception as e:
        logger.error(f"Error loading annual totals data: {e}")
        return

    # Convert to Billions for plotting and display
    df_annual['federal_action_obligation_B'] = df_annual['federal_action_obligation'] / 1e9
    df_annual['obligation_real_fy25_B'] = df_annual['obligation_real_fy25'] / 1e9

    print("\n--- Final Annual Spending Comparison (Billions of USD) ---")
    print(df_annual[['fiscal_year', 'federal_action_obligation_B', 'obligation_real_fy25_B']].to_string(index=False, float_format=lambda x: f'{x:,.2f}'))

    plt.style.use('ggplot')

    # 1. Initialize the plot
    fig, ax = plt.subplots(figsize=(12, 6))

    # 2. Plot the two series
    ax.plot(
        df_annual['fiscal_year'],
        df_annual['federal_action_obligation_B'],
        label='Nominal Obligation (Then-Year $)',
        marker='o',
        color='blue'
    )
    ax.plot(
        df_annual['fiscal_year'],
        df_annual['obligation_real_fy25_B'],
        label='Real Obligation (FY2025 Constant $)',
        marker='s',
        color='red',
        linestyle='--'
    )

    # 3. Add labels and titles
    ax.set_title('DoD Contract Obligations: Nominal vs. Real (FY2010 - FY2024)', fontsize=16)
    ax.set_xlabel('Fiscal Year', fontsize=12)
    ax.set_ylabel('Obligation (Billions of USD)', fontsize=12)

    # 4. Final Formatting
    ax.legend(loc='upper right')
    ax.grid(True, axis='y', linestyle='--', alpha=0.7)

    # Ensure years are displayed as integers
    ax.set_xticks(df_annual['fiscal_year'])

    # Format Y-axis labels with commas and a '$' sign
    from matplotlib.ticker import FuncFormatter
    formatter = FuncFormatter(lambda x, pos: f'${x:,.0f}')
    ax.yaxis.set_major_formatter(formatter)

    # 5. Save the figure (This will work even if plt.show() fails)
    output_path = os.path.join(PLOT_DIR, 'final_annual_spending_comparison.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')

    logger.info(f"SUCCESS: Plot saved to {output_path}")

    # 6. Attempt to show the figure (This is the interactive part that sometimes fails)
    plt.show()

if __name__ == "__main__":
    generate_annual_spending_plot()