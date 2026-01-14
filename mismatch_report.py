"""
Mismatch Report Script

This script:
1. Reads mismatch.xlsx from input folder
2. Filters rows where ETOF_NUMBER is present in the ETOF processed file
3. Creates a pivot table with Cost type as rows and various columns
"""

import pandas as pd
import os
from pathlib import Path
from part1_etof_file_processing import process_etof_file


def load_mismatch_file(file_path="mismatch_rhenus.xlsx"):
    """Load the mismatch Excel file from input folder."""
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    df = pd.read_excel(full_path)
    print(f"Loaded mismatch file: {full_path}")
    print(f"   Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"   Columns: {list(df.columns)}")
    
    return df


def filter_by_etof_numbers(df_mismatch, df_etof):
    """
    Filter mismatch rows to keep only those where ETOF_NUMBER 
    is present in the ETOF processed dataframe.
    
    Args:
        df_mismatch: Mismatch dataframe
        df_etof: ETOF processed dataframe from part1_etof_file_processing
    
    Returns:
        Filtered mismatch dataframe
    """
    # Find ETOF number column in mismatch file
    etof_col_mismatch = None
    for col in df_mismatch.columns:
        if 'etof' in col.lower() and ('number' in col.lower() or 'num' in col.lower() or '#' in col):
            etof_col_mismatch = col
            break
    
    if etof_col_mismatch is None:
        # Try exact match
        for col in df_mismatch.columns:
            if col.upper() == 'ETOF_NUMBER' or col == 'ETOF #':
                etof_col_mismatch = col
                break
    
    if etof_col_mismatch is None:
        print(f"   [WARNING] Could not find ETOF number column in mismatch file")
        print(f"   Available columns: {list(df_mismatch.columns)}")
        return df_mismatch
    
    print(f"   Found ETOF column in mismatch: '{etof_col_mismatch}'")
    
    # Find ETOF number column in ETOF processed file
    etof_col_etof = None
    for col in df_etof.columns:
        if col == 'ETOF #' or col.upper() == 'ETOF_NUMBER':
            etof_col_etof = col
            break
    
    if etof_col_etof is None:
        for col in df_etof.columns:
            if 'etof' in col.lower() and ('#' in col or 'number' in col.lower()):
                etof_col_etof = col
                break
    
    if etof_col_etof is None:
        print(f"   [WARNING] Could not find ETOF number column in ETOF file")
        print(f"   Available columns: {list(df_etof.columns)}")
        return df_mismatch
    
    print(f"   Found ETOF column in ETOF file: '{etof_col_etof}'")
    
    # Get unique ETOF numbers from the processed ETOF file
    etof_numbers = set(df_etof[etof_col_etof].dropna().astype(str).str.strip())
    print(f"   ETOF numbers in processed file: {len(etof_numbers)}")
    
    # Filter mismatch to keep only rows with matching ETOF numbers
    df_mismatch['_etof_str'] = df_mismatch[etof_col_mismatch].astype(str).str.strip()
    df_filtered = df_mismatch[df_mismatch['_etof_str'].isin(etof_numbers)]
    df_filtered = df_filtered.drop(columns=['_etof_str'])
    
    print(f"   Filtered mismatch: {len(df_filtered)} rows (from {len(df_mismatch)})")
    
    return df_filtered


def create_pivot_report(df_filtered, include_positive_discrepancy=True):
    """
    Create a pivot-style report with Cost type and relevant columns.
    
    Args:
        df_filtered: Filtered mismatch dataframe
        include_positive_discrepancy: If True, keep all discrepancy values (except zero).
                                      If False, remove rows with positive discrepancy values.
    
    Returns:
        DataFrame with pivot structure
    """
    # Define columns to include
    columns_to_include = [
        'Cost type',
        'ETOF_NUMBER',
        'SHIPMENT_ID',
        'DELIVERY_NUMBER',
        'SHIP_DATE',
        'SHIP_COUNTRY_ETOF',
        'SHIP_CITY_ETOF',
        'CUST_COUNTRY_ETOF',
        'CUST_CITY_ETOF',
        'SERVICE_ETOF',
        'Pre-calc. cost (in inv curr)',
        'Invoice statement cost  (in inv curr)',
        'Discrepancy in inv currency  (in inv curr)'
    ]
    
    # Find matching columns (case-insensitive)
    available_columns = []
    column_mapping = {}
    
    for target_col in columns_to_include:
        found = False
        for df_col in df_filtered.columns:
            # Exact match
            if df_col == target_col:
                available_columns.append(df_col)
                column_mapping[df_col] = target_col
                found = True
                break
            # Case-insensitive match
            if df_col.lower().replace(' ', '').replace('_', '') == target_col.lower().replace(' ', '').replace('_', ''):
                available_columns.append(df_col)
                column_mapping[df_col] = target_col
                found = True
                break
        
        if not found:
            # Try partial match
            for df_col in df_filtered.columns:
                target_parts = target_col.lower().replace('_', ' ').split()
                df_parts = df_col.lower().replace('_', ' ').split()
                if len(set(target_parts) & set(df_parts)) >= len(target_parts) * 0.5:
                    available_columns.append(df_col)
                    column_mapping[df_col] = target_col
                    found = True
                    break
        
        if not found:
            print(f"   [WARNING] Column not found: '{target_col}'")
    
    print(f"\n   Available columns for report: {len(available_columns)}")
    
    # Select available columns
    df_report = df_filtered[available_columns].copy()
    
    # Remove rows with empty SHIPMENT_ID if column exists
    shipment_id_col = None
    for col in available_columns:
        if 'shipment' in col.lower() and 'id' in col.lower():
            shipment_id_col = col
            break
    
    # Remove rows with empty DELIVERY_NUMBER if column exists
    delivery_col = None
    for col in available_columns:
        if 'delivery' in col.lower() and 'number' in col.lower():
            delivery_col = col
            break
    
    # Create a cleaner report - filter to show only non-empty key identifiers
    # Keep row if SHIPMENT_ID is not empty OR DELIVERY_NUMBER is not empty
    if shipment_id_col or delivery_col:
        mask = pd.Series([True] * len(df_report))
        # We actually want to keep all rows that have at least one identifier
        # But the user said "if not empty" which I interpret as only including those columns when they have values
        pass  # Keep all rows, the columns will just show empty where applicable
    
    # Remove rows where Discrepancy in inv currency is 0
    # Optionally also remove positive discrepancy values
    discrepancy_col = None
    for col in available_columns:
        if 'discrepancy' in col.lower() and 'inv' in col.lower() and 'curr' in col.lower():
            discrepancy_col = col
            break
    
    if discrepancy_col:
        initial_count = len(df_report)
        # Always remove zero discrepancy
        df_report = df_report[df_report[discrepancy_col] != 0]
        removed_zeros = initial_count - len(df_report)
        print(f"   Removed {removed_zeros} rows with zero discrepancy")
        
        # If include_positive_discrepancy is False, also remove positive values
        if not include_positive_discrepancy:
            count_before = len(df_report)
            df_report = df_report[df_report[discrepancy_col] < 0]
            removed_positive = count_before - len(df_report)
            print(f"   Removed {removed_positive} rows with positive discrepancy (keeping only negative)")
    
    # Sort by Cost type if available
    cost_type_col = None
    for col in available_columns:
        if 'cost' in col.lower() and 'type' in col.lower():
            cost_type_col = col
            break
    
    if cost_type_col:
        df_report = df_report.sort_values(by=cost_type_col)
    
    # Rename columns for cleaner output
    column_renames = {
        'Pre-calc. cost (in inv curr)': 'Pre-calc. cost',
        'Invoice statement cost  (in inv curr)': "Carrier's cost",
        'Discrepancy in inv currency  (in inv curr)': 'Discrepancy'
    }
    df_report = df_report.rename(columns=column_renames)
    
    print(f"   Report created: {len(df_report)} rows x {len(df_report.columns)} columns")
    
    return df_report


def save_report(df_report, output_filename="mismatch_report.xlsx"):
    """Save the report to Excel file in partly_df folder."""
    output_folder = Path(__file__).parent / "partly_df"
    output_folder.mkdir(exist_ok=True)
    
    output_path = output_folder / output_filename
    
    try:
        df_report.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\n   Saved to: {output_path}")
    except PermissionError:
        alt_filename = output_filename.replace('.xlsx', '_new.xlsx')
        alt_path = output_folder / alt_filename
        df_report.to_excel(alt_path, index=False, engine='openpyxl')
        print(f"\n   [WARNING] Original file is open. Saved to: {alt_path}")
        output_path = alt_path
    
    return output_path


def main(include_positive_discrepancy=True):
    """
    Main function to create the mismatch report.
    
    Args:
        include_positive_discrepancy: If True, keep all discrepancy values (except zero).
                                      If False, remove rows with positive discrepancy values
                                      (keep only negative discrepancies).
    """
    print("\n" + "="*80)
    print("MISMATCH REPORT")
    print("="*80)
    
    print(f"\n   include_positive_discrepancy = {include_positive_discrepancy}")
    if include_positive_discrepancy:
        print("   (Keeping all non-zero discrepancies)")
    else:
        print("   (Keeping only NEGATIVE discrepancies)")
    
    # Step 1: Load mismatch file
    print("\n1. Loading mismatch file...")
    df_mismatch = load_mismatch_file("mismatch_rhenus.xlsx")
    
    # Step 2: Load ETOF processed file
    print("\n2. Loading ETOF processed file...")
    df_etof, _ = process_etof_file('etofs_rhenus.xlsx')
    print(f"   ETOF processed: {len(df_etof)} rows")
    
    # Step 3: Filter by ETOF numbers
    print("\n3. Filtering by ETOF numbers...")
    df_filtered = filter_by_etof_numbers(df_mismatch, df_etof)
    
    # Step 4: Create pivot report
    print("\n4. Creating pivot report...")
    df_report = create_pivot_report(df_filtered, include_positive_discrepancy)
    
    # Step 5: Save report
    print("\n5. Saving report...")
    output_path = save_report(df_report)
    
    print("\n" + "="*80)
    print(f"DONE! Report saved to: {output_path}")
    print("="*80)
    
    return df_report


if __name__ == "__main__":
    # Set to True to keep all discrepancies (positive and negative)
    # Set to False to keep only negative discrepancies
    INCLUDE_POSITIVE_DISCREPANCY = False
    
    df_report = main(include_positive_discrepancy=INCLUDE_POSITIVE_DISCREPANCY)

