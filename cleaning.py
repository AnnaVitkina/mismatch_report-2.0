"""
Cleaning Script for Conditions Checked Results

This script processes the conditions_checked.xlsx file from conditions_checking.py
and creates a cleaned result file with:
1. Cost type column - only first appearance (no duplicates per lane)
2. Removed columns: Carrier Agreement #, Comment, Rate By, Applies If
3. Pivot tabs per each data tab with Cost type + Reason pattern summary
4. Applied formatting
"""

import pandas as pd
import re
from pathlib import Path

# Import formatting from result_transforming
from result_transforming import format_result_file


def get_partly_df_folder():
    """Get the path to the partly_df folder."""
    return Path(__file__).parent / "partly_df"


def load_conditions_checked():
    """
    Load the conditions_checked.xlsx file.
    
    Returns:
        dict: {sheet_name: DataFrame, ...}
    """
    partly_df = get_partly_df_folder()
    input_file = partly_df / "conditions_checked.xlsx"
    
    if not input_file.exists():
        raise FileNotFoundError(f"Conditions checked file not found: {input_file}")
    
    print(f"   Loading from: {input_file}")
    
    xlsx = pd.ExcelFile(input_file)
    sheets = {}
    
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        sheets[sheet_name] = df
        print(f"      Tab '{sheet_name}': {len(df)} rows")
    
    return sheets


def extract_reason_pattern(reason):
    """
    Extract a generalized pattern from a reason string.
    
    User-specified patterns:
    - "Cost 'X' not found..." -> "The cost is not covered by rate card"
    - "The cost is pre-calculated by rate card - X flat." -> "The cost is pre-calculated by rate card"
    - "Cost per unit: X, but 'Y' not found in MEASUREMENT..." -> "Measurement is missing for the cost"
    - "MIN price applied - X (Calculated: ...)" -> "Pre-calculated according to the rate card"
    - "Price value is empty for cost 'X' in lane Y" -> "Price is missing for the provided shipment details"
    """
    if not reason or pd.isna(reason):
        return "No reason provided"
    
    reason_str = str(reason).strip()
    reason_lower = reason_str.lower()
    
    # MIN price applied -> pre-calculated according to rate card
    if 'min price applied' in reason_lower:
        return "Pre-calculated according to the rate card"
    
    # MAX price applied -> pre-calculated according to rate card
    if 'max price applied' in reason_lower:
        return "Pre-calculated according to the rate card"
    
    # Cost per unit with successful calculation (contains "Total:" and "=")
    if 'cost per unit' in reason_lower and 'total:' in reason_lower and '=' in reason_lower:
        return "Pre-calculated according to the rate card"
    
    # Cost per unit but measurement/multiplier not found
    # "Cost per unit: 425.0, but 'Condition/ExpressDelivery' not found in MEASUREMENT column..."
    if 'cost per unit' in reason_lower and 'not found' in reason_lower:
        return "Measurement is missing for the cost"
    
    # Pre-calculated flat price
    # "The cost is pre-calculated by rate card - 425.0 flat."
    if 'pre-calculated' in reason_lower and 'flat' in reason_lower:
        return "The cost is pre-calculated by rate card"
    
    # Weight-tiered flat price
    if 'weight-tiered flat price' in reason_lower:
        return "The cost is pre-calculated by rate card"
    
    # Price value is empty
    # "Price value is empty for cost 'EAD Charge' in lane 2993"
    if 'price value is empty' in reason_lower:
        return "Price is missing for the provided shipment details"
    
    # Price per unit column not found
    if "'price per unit' column not found" in reason_lower:
        return "Price is missing for the provided shipment details"
    
    # Cost not found in rate card or accessorial costs
    # "Cost 'AWB Fee' not found in rate card or accessorial costs"
    # "Cost type 'AWB Fee' not found in cost conditions"
    if 'not found' in reason_lower and ('rate card' in reason_lower or 'accessorial' in reason_lower or 'cost conditions' in reason_lower):
        return "The cost is not covered by rate card"
    
    # Cost type not found (generic)
    if 'cost type' in reason_lower and 'not found' in reason_lower:
        return "The cost is not covered by rate card"
    
    # Cost not covered
    if 'not covered' in reason_lower:
        return "The cost is not covered by rate card"
    
    # No rate cost data for agreement
    if 'no rate cost data' in reason_lower:
        return "The cost is not covered by rate card"
    
    # Lane not found in rate data
    if 'lane' in reason_lower and 'not found' in reason_lower:
        return "Price is missing for the provided shipment details"
    
    # Applies If not met
    if 'applies if not met' in reason_lower or 'applies if condition not met' in reason_lower:
        return "Applies If condition not met"
    
    # Column not found in shipment data (for condition checking)
    if 'not found in shipment data' in reason_lower:
        return "Applies If condition not met"
    
    # No comment found for ETOF
    if 'no comment found' in reason_lower:
        return "Lane information missing for shipment"
    
    # Could not extract rate lane
    if 'could not extract rate lane' in reason_lower:
        return "Lane information missing for shipment"
    
    # Multiple rate lanes - manual check required
    if 'multiple rate lanes' in reason_lower:
        return "Multiple lanes - manual check required"
    
    # ETOF not found in mapping
    if 'etof' in reason_lower and 'not found' in reason_lower:
        return "Shipment not found in mapping"
    
    # CHARGE_WEIGHT exceeds max tier
    if 'charge_weight' in reason_lower and 'exceeds' in reason_lower:
        return "Weight exceeds maximum tier"
    
    # Accessorial - no price found
    if 'accessorial' in reason_lower and 'no price found' in reason_lower:
        return "Price is missing for the provided shipment details"
    
    # Generic accessorial with price (flat or calculated)
    if 'accessorial' in reason_lower:
        if 'flat' in reason_lower or 'total:' in reason_lower:
            return "Pre-calculated according to the rate card"
    
    # Fallback
    return "Other"


def deduplicate_cost_type(df):
    """
    Keep Cost type only on first row of each CONSECUTIVE group - blank for subsequent rows.
    
    Example:
    - Row 1: DGR Fee  -> DGR Fee (first in group)
    - Row 2: DGR Fee  -> '' (consecutive duplicate)
    - Row 3: AWB Fee  -> AWB Fee (new cost type)
    - Row 4: AWB Fee  -> '' (consecutive duplicate)
    - Row 5: DGR Fee  -> DGR Fee (new group, even though we saw DGR Fee before)
    
    Args:
        df: DataFrame with Cost type column
    
    Returns:
        DataFrame with deduplicated Cost type column
    """
    df_copy = df.copy()
    
    # Find the Cost type column
    cost_type_col = None
    for col in df_copy.columns:
        if 'cost' in col.lower() and 'type' in col.lower():
            cost_type_col = col
            break
    
    if cost_type_col is None:
        print("      [WARNING] Cost type column not found")
        return df_copy
    
    # Track previous cost type - only blank consecutive duplicates
    prev_cost = None
    new_values = []
    
    for val in df_copy[cost_type_col]:
        if pd.notna(val) and str(val).strip() != '':
            val_str = str(val).strip()
            if val_str == prev_cost:
                new_values.append('')  # Blank for consecutive duplicate
            else:
                new_values.append(val_str)  # Show first occurrence in group
                prev_cost = val_str
        else:
            new_values.append('')  # Keep blank as blank
    
    df_copy[cost_type_col] = new_values
    return df_copy


def remove_columns(df, columns_to_remove):
    """
    Remove specified columns from DataFrame.
    
    Args:
        df: DataFrame
        columns_to_remove: List of column name patterns to remove
    
    Returns:
        DataFrame with columns removed
    """
    df_copy = df.copy()
    
    cols_to_drop = []
    for col in df_copy.columns:
        col_lower = col.lower()
        for pattern in columns_to_remove:
            pattern_lower = pattern.lower()
            if pattern_lower in col_lower or col_lower == pattern_lower:
                cols_to_drop.append(col)
                break
    
    if cols_to_drop:
        print(f"      Removing columns: {cols_to_drop}")
        df_copy = df_copy.drop(columns=cols_to_drop, errors='ignore')
    
    return df_copy


def create_pivot_summary(df):
    """
    Create a pivot summary of Cost type + Reason pattern.
    
    Args:
        df: DataFrame with Cost type and Reason columns
    
    Returns:
        DataFrame with pivot summary
    """
    # Find relevant columns
    cost_type_col = None
    reason_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'cost' in col_lower and 'type' in col_lower:
            cost_type_col = col
        elif 'reason' in col_lower:
            reason_col = col
    
    if cost_type_col is None or reason_col is None:
        print("      [WARNING] Cannot create pivot - Cost type or Reason column not found")
        return pd.DataFrame()
    
    # Get original cost types (before deduplication) by forward-filling blanks
    df_for_pivot = df.copy()
    df_for_pivot[cost_type_col] = df_for_pivot[cost_type_col].replace('', pd.NA)
    df_for_pivot[cost_type_col] = df_for_pivot[cost_type_col].ffill()
    
    # Extract reason patterns
    df_for_pivot['Reason Pattern'] = df_for_pivot[reason_col].apply(extract_reason_pattern)
    
    # Create pivot
    pivot = df_for_pivot.groupby([cost_type_col, 'Reason Pattern']).size().reset_index(name='Count')
    pivot.columns = ['Cost Type', 'Reason Pattern', 'Count']
    
    # Sort by Cost Type, then by Count descending
    pivot = pivot.sort_values(['Cost Type', 'Count'], ascending=[True, False])
    
    return pivot


def clean_sheet_name(name, suffix=""):
    """Clean string to be a valid Excel sheet name (max 31 chars, no invalid chars)."""
    if name is None or pd.isna(name):
        name = "Sheet"
    name = str(name).strip()
    # Replace invalid characters with underscore
    name = re.sub(r'[\\/*?:\[\]]', '_', name)
    
    # Add suffix if provided
    if suffix:
        max_base_len = 31 - len(suffix) - 1
        name = name[:max_base_len] + "_" + suffix
    
    # Truncate to 31 characters
    return name[:31]


def calculate_cost_type_groups(df):
    """
    Calculate row groups based on cost type for coloring.
    
    All consecutive rows with the SAME cost type get the same color.
    When cost type CHANGES, the color alternates.
    
    Returns:
        List of (start_row, end_row, color_index) tuples
        start_row and end_row are Excel row numbers (1-indexed, with header at row 1)
    """
    # Find the Cost type column
    cost_type_col = None
    for col in df.columns:
        if 'cost' in col.lower() and 'type' in col.lower():
            cost_type_col = col
            break
    
    if cost_type_col is None:
        return []
    
    groups = []
    prev_cost = None
    current_start = 2  # Excel row 2 (after header)
    color_index = 0
    
    for idx, val in enumerate(df[cost_type_col]):
        excel_row = idx + 2  # Convert to Excel row (1-indexed, header is row 1)
        
        # Get current cost type value (handle NaN and empty)
        if pd.notna(val) and str(val).strip() != '':
            current_cost = str(val).strip()
        else:
            current_cost = prev_cost  # Inherit from previous if empty
        
        # Check if cost type changed
        if prev_cost is not None and current_cost != prev_cost:
            # Save the previous group
            groups.append((current_start, excel_row - 1, color_index))
            # Start new group with alternating color
            current_start = excel_row
            color_index = 1 - color_index
        elif prev_cost is None and current_cost is not None:
            # First cost type encountered
            current_start = excel_row
        
        prev_cost = current_cost
    
    # Don't forget the last group
    if prev_cost is not None:
        last_row = len(df) + 1  # +1 because Excel is 1-indexed and we have header
        groups.append((current_start, last_row, color_index))
    
    return groups


def get_result_folder():
    """Get the path to the output folder."""
    output_folder = Path(__file__).parent / "output"
    output_folder.mkdir(exist_ok=True)
    return output_folder


def process_and_save(sheets, output_filename="result.xlsx", extra_columns=None):
    """
    Process all sheets and save to output file.
    
    Args:
        sheets: dict {sheet_name: DataFrame}
        output_filename: Output file name
        extra_columns: List of column names to add from lc_etof_with_comments.xlsx
    
    Returns:
        Path to output file
    """
    output_folder = get_result_folder()
    output_path = output_folder / output_filename
    
    # Columns to remove
    columns_to_remove = ['Carrier Agreement', 'Comment', 'Rate By', 'Applies If']
    
    print("\n   Processing sheets...")
    
    # Track cost type groups for coloring
    all_cost_type_groups = {}
    
    # First, write data without formatting
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            if df.empty:
                continue
            
            print(f"\n   Processing: {sheet_name}")
            
            # Store original for pivot (before modifications)
            df_original = df.copy()
            
            # 1. Remove specified columns
            df_cleaned = remove_columns(df, columns_to_remove)
            
            # 2. Deduplicate Cost type
            df_cleaned = deduplicate_cost_type(df_cleaned)
            
            # 3. Calculate cost type groups BEFORE writing (using original Cost type values)
            data_sheet_name = clean_sheet_name(sheet_name)
            cost_groups = calculate_cost_type_groups(df_original)
            all_cost_type_groups[data_sheet_name] = cost_groups
            print(f"      Cost type groups: {len(cost_groups)}")
            
            # 4. Write cleaned data sheet
            df_cleaned.to_excel(writer, sheet_name=data_sheet_name, index=False)
            print(f"      Data tab '{data_sheet_name}': {len(df_cleaned)} rows")
            
            # 5. Create and write pivot summary
            pivot_df = create_pivot_summary(df_original)
            if not pivot_df.empty:
                pivot_sheet_name = clean_sheet_name(sheet_name, "Pivot")
                pivot_df.to_excel(writer, sheet_name=pivot_sheet_name, index=False)
                print(f"      Pivot tab '{pivot_sheet_name}': {len(pivot_df)} patterns")
    
    # Now add extra columns (if any) and apply formatting via result_transforming
    print("\n   Calling result_transforming for extra columns and formatting...")
    format_result_file(output_path, all_cost_type_groups, extra_columns)
    
    print(f"\n   Saved to: {output_path}")
    return output_path


def main(extra_columns=None):
    """
    Main function to run the cleaning process.
    
    Args:
        extra_columns: List of column names to add from lc_etof_with_comments.xlsx
    
    Returns:
        Path to output file
    """
    print("\n" + "="*80)
    print("CLEANING CONDITIONS CHECKED RESULTS")
    print("="*80)
    
    # Step 1: Load the conditions_checked.xlsx
    print("\n1. Loading conditions_checked.xlsx...")
    sheets = load_conditions_checked()
    
    # Step 2: Process and save
    print("\n2. Processing and creating result file...")
    output_path = process_and_save(sheets, extra_columns=extra_columns)
    
    print("\n" + "="*80)
    print(f"DONE! Result saved to: {output_path}")
    print("="*80)
    
    return output_path


if __name__ == "__main__":
    main()
