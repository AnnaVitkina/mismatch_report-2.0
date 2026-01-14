"""
Mismatches Filing Script

This script:
1. Gets the mismatch report DataFrame from mismatch_report.py
2. Gets ETOF # -> Carrier Agreement # mapping from lc_etof_with_comments.xlsx (result of matching.py)
3. Gets the cost columns list from rate_costs.py (per agreement)
4. Gets the cost names from rate_accesorial_costs.py (per agreement)
5. Compares Cost type values with the cost names - check only in the corresponding rate card files
6. Adds a "Comment" column for rows where cost is NOT found in rate card OR accessorial rate cards
7. Saves result with separate tabs per Carrier Agreement #
"""

import pandas as pd
import re
import os
from pathlib import Path
from mismatch_report import main as run_mismatch_report


def get_partly_df_folder():
    """Get the path to the partly_df folder."""
    return Path(__file__).parent / "partly_df"


def get_mismatch_report(include_positive_discrepancy=False):
    """Get the mismatch report DataFrame from mismatch_report.py."""
    print("Running mismatch_report.py...")
    df = run_mismatch_report(include_positive_discrepancy=include_positive_discrepancy)
    print(f"Got mismatch report: {len(df)} rows")
    return df


def get_lc_etof_with_comments():
    """
    Read lc_etof_with_comments.xlsx (result of matching.py).
    
    Returns:
        dict: {sheet_name: DataFrame, ...}
    """
    partly_df = get_partly_df_folder()
    file_path = partly_df / "lc_etof_with_comments.xlsx"
    
    if not file_path.exists():
        print(f"   [WARNING] lc_etof_with_comments.xlsx not found at {file_path}")
        return {}
    
    try:
        xl = pd.ExcelFile(file_path)
        sheets = {}
        for sheet_name in xl.sheet_names:
            sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"   - {sheet_name}: {len(sheets[sheet_name])} rows")
        return sheets
    except Exception as e:
        print(f"   [ERROR] Could not read lc_etof_with_comments.xlsx: {e}")
        return {}


def get_carrier_agreement_mapping_from_lc_etof():
    """
    Get ETOF # -> Carrier Agreement # mapping from lc_etof_with_comments.xlsx.
    
    Returns:
        dict: {etof_number: agreement_number, ...}
    """
    print("Reading lc_etof_with_comments.xlsx...")
    sheets = get_lc_etof_with_comments()
    
    if not sheets:
        return {}
    
    etof_to_agreement = {}
    
    # Process each sheet (each sheet name that looks like an agreement number)
    for sheet_name, df in sheets.items():
        # Skip "All Data" and "No Agreement" sheets
        if sheet_name in ["All Data", "No Agreement"]:
            continue
        
        # Find ETOF # column
        etof_col = None
        for col in df.columns:
            if 'etof' in col.lower() and '#' in col.lower():
                etof_col = col
                break
        
        if not etof_col:
            continue
        
        # Map all ETOFs in this sheet to the sheet name (agreement number)
        for _, row in df.iterrows():
            etof_num = row.get(etof_col)
            if pd.notna(etof_num) and str(etof_num).strip() and str(etof_num).lower() != 'nan':
                etof_to_agreement[str(etof_num).strip()] = sheet_name
    
    print(f"   Created ETOF -> Carrier Agreement mapping: {len(etof_to_agreement)} entries")
    
    # Show unique agreements
    unique_agreements = set(etof_to_agreement.values())
    print(f"   Unique Carrier Agreements: {list(unique_agreements)}")
    
    return etof_to_agreement


def add_carrier_agreement(df, etof_to_agreement):
    """Add 'Carrier Agreement #' column to DataFrame based on ETOF number."""
    # Find ETOF column in mismatch report
    etof_col = None
    for col in df.columns:
        if 'etof' in col.lower() and ('number' in col.lower() or '#' in col.lower()):
            etof_col = col
            break
    
    if etof_col is None:
        print("   [WARNING] ETOF column not found in mismatch report")
        df['Carrier Agreement #'] = None
        return df
    
    print(f"   Using ETOF column: '{etof_col}'")
    
    # Add Carrier Agreement # column
    def get_agreement(row):
        etof_num = row.get(etof_col)
        if pd.notna(etof_num):
            return etof_to_agreement.get(str(etof_num).strip())
        return None
    
    df['Carrier Agreement #'] = df.apply(get_agreement, axis=1)
    
    filled_count = len(df[df['Carrier Agreement #'].notna()])
    print(f"   Filled 'Carrier Agreement #' for {filled_count} of {len(df)} rows")
    
    # Show unique agreements
    unique_agreements = df['Carrier Agreement #'].dropna().unique()
    print(f"   Unique Carrier Agreements: {list(unique_agreements)}")
    
    return df


def get_rate_card_costs_for_agreement(agreement_number):
    """
    Get cost names from rate_costs.py output for a specific agreement.
    
    Args:
        agreement_number: The agreement number (e.g., "RA20220420022")
    
    Returns:
        set: Set of cost names
    """
    partly_df = get_partly_df_folder()
    
    # Look for <agreement>_costs.xlsx
    costs_file = partly_df / f"{agreement_number}_costs.xlsx"
    
    if not costs_file.exists():
        print(f"      [WARNING] {costs_file.name} not found")
        return set()
    
    try:
        # Read Cost Conditions sheet
        df_costs = pd.read_excel(costs_file, sheet_name='Cost Conditions')
        
        # Get cost names from "Cost Name" column
        if 'Cost Name' in df_costs.columns:
            cost_names = set(df_costs['Cost Name'].dropna().astype(str).str.strip())
            print(f"      Rate card costs: {len(cost_names)} cost types")
            return cost_names
        else:
            print(f"      [WARNING] 'Cost Name' column not found in {costs_file.name}")
            return set()
    except Exception as e:
        print(f"      [ERROR] Could not read {costs_file.name}: {e}")
        return set()


def get_accessorial_costs_for_agreement(agreement_number):
    """
    Get cost names from rate_accesorial_costs.py output for a specific agreement.
    
    Args:
        agreement_number: The agreement number (e.g., "RA20220420022")
    
    Returns:
        set: Set of cost names
    """
    partly_df = get_partly_df_folder()
    
    # Look for <agreement>_accessorial_costs.xlsx
    costs_file = partly_df / f"{agreement_number}_accessorial_costs.xlsx"
    
    if not costs_file.exists():
        print(f"      [WARNING] {costs_file.name} not found")
        return set()
    
    try:
        # Read Accessorial Costs sheet
        df_costs = pd.read_excel(costs_file, sheet_name='Accessorial Costs')
        
        # Get cost names from "Cost Name" column
        if 'Cost Name' in df_costs.columns:
            cost_names = set(df_costs['Cost Name'].dropna().astype(str).str.strip())
            print(f"      Accessorial costs: {len(cost_names)} cost types")
            return cost_names
        else:
            print(f"      [WARNING] 'Cost Name' column not found in {costs_file.name}")
            return set()
    except Exception as e:
        print(f"      [ERROR] Could not read {costs_file.name}: {e}")
        return set()


def get_all_costs_for_agreement(agreement_number):
    """
    Get ALL cost names (rate card + accessorial) for a specific agreement.
    
    Args:
        agreement_number: The agreement number
    
    Returns:
        tuple: (rate_card_costs, accessorial_costs, combined_costs)
    """
    print(f"   Loading costs for {agreement_number}...")
    
    rate_card_costs = get_rate_card_costs_for_agreement(agreement_number)
    accessorial_costs = get_accessorial_costs_for_agreement(agreement_number)
    
    combined_costs = rate_card_costs | accessorial_costs
    print(f"      Combined: {len(combined_costs)} cost types")
    
    return rate_card_costs, accessorial_costs, combined_costs


def discover_all_agreements():
    """
    Discover all agreements from lc_etof_with_comments.xlsx.
    
    Returns:
        list: List of agreement numbers
    """
    sheets = get_lc_etof_with_comments()
    
    # Filter out non-agreement sheets
    agreements = [name for name in sheets.keys() 
                  if name not in ["All Data", "No Agreement"]]
    
    return agreements


def load_all_agreement_costs():
    """
    Load costs for all agreements.
    
    Returns:
        dict: {agreement_number: {'rate_card': set, 'accessorial': set, 'combined': set}, ...}
    """
    agreements = discover_all_agreements()
    
    print(f"\n   Found {len(agreements)} agreements: {agreements}")
    
    all_costs = {}
    for agreement in agreements:
        rate_card, accessorial, combined = get_all_costs_for_agreement(agreement)
        all_costs[agreement] = {
            'rate_card': rate_card,
            'accessorial': accessorial,
            'combined': combined
        }
    
    return all_costs


def extract_base_cost_name(cost_name):
    """
    Extract the base cost name without parentheses.
    
    Examples:
        "Delivery Fee (Getafe, Madrid)" -> "Delivery Fee"
        "Pickup Fee" -> "Pickup Fee"
        "AWB Fee (Origin)" -> "AWB Fee"
    
    Returns:
        str: Base cost name without parentheses content
    """
    if not cost_name:
        return cost_name
    
    # Remove text in parentheses and trailing whitespace
    base_name = re.sub(r'\s*\([^)]*\)\s*', '', cost_name).strip()
    return base_name if base_name else cost_name


def find_cost_match(cost_type, combined_costs):
    """
    Try to find a matching cost name using multiple strategies.
    
    Strategies:
    1. Exact match
    2. Base name match (cost_type without parentheses)
    3. Reverse match (rate card cost without parentheses matches cost_type)
    4. Case-insensitive match
    
    Args:
        cost_type: The cost type to find
        combined_costs: Set of cost names from rate card
    
    Returns:
        tuple: (found, matched_cost_name, match_type)
            - found: True if match found
            - matched_cost_name: The actual cost name that matched (or None)
            - match_type: Description of how it matched
    """
    if not cost_type:
        return False, None, None
    
    # Strategy 1: Exact match
    if cost_type in combined_costs:
        return True, cost_type, "exact"
    
    # Strategy 2: Base name match (remove parentheses from cost_type)
    base_cost_type = extract_base_cost_name(cost_type)
    if base_cost_type != cost_type and base_cost_type in combined_costs:
        return True, base_cost_type, "base_name"
    
    # Strategy 3: Reverse match (rate card cost without parentheses matches cost_type)
    for rc_cost in combined_costs:
        base_rc_cost = extract_base_cost_name(rc_cost)
        if base_rc_cost == cost_type:
            return True, rc_cost, "reverse_base"
        if base_rc_cost == base_cost_type:
            return True, rc_cost, "both_base"
    
    # Strategy 4: Case-insensitive match
    cost_type_lower = cost_type.lower()
    base_cost_type_lower = base_cost_type.lower()
    
    for rc_cost in combined_costs:
        rc_cost_lower = rc_cost.lower()
        base_rc_cost_lower = extract_base_cost_name(rc_cost).lower()
        
        if rc_cost_lower == cost_type_lower:
            return True, rc_cost, "case_insensitive"
        if base_rc_cost_lower == cost_type_lower:
            return True, rc_cost, "case_insensitive_base"
        if rc_cost_lower == base_cost_type_lower:
            return True, rc_cost, "case_insensitive_reverse"
        if base_rc_cost_lower == base_cost_type_lower:
            return True, rc_cost, "case_insensitive_both_base"
    
    return False, None, None


def add_comment_for_missing_costs(df_mismatch, agreement_costs):
    """
    Add a 'Comment' column for rows where Cost type is NOT found in the corresponding rate card.
    
    Tries multiple matching strategies:
    1. Exact match
    2. Base name match (without parentheses)
    3. Reverse base match
    4. Case-insensitive variations
    
    Args:
        df_mismatch: Mismatch report DataFrame with 'Carrier Agreement #' column
        agreement_costs: Dict of costs per agreement from load_all_agreement_costs()
    
    Returns:
        DataFrame with 'Comment' column added
    """
    df = df_mismatch.copy()
    
    # Find Cost type column
    cost_type_col = None
    for col in df.columns:
        if 'cost' in col.lower() and 'type' in col.lower():
            cost_type_col = col
            break
    
    if cost_type_col is None:
        print("   [WARNING] 'Cost type' column not found")
        df['Comment'] = ''
        return df
    
    print(f"   Using column: '{cost_type_col}'")
    
    # Get unique cost types from mismatch
    mismatch_cost_types = set(df[cost_type_col].dropna().astype(str).str.strip())
    print(f"   Unique cost types in mismatch: {len(mismatch_cost_types)}")
    
    # Track found/missing costs per agreement
    found_by_agreement = {}
    missing_by_agreement = {}
    match_types_by_agreement = {}  # Track how costs were matched
    
    def get_comment(row):
        cost_type = str(row[cost_type_col]).strip() if pd.notna(row[cost_type_col]) else ''
        agreement = row.get('Carrier Agreement #')
        
        if not cost_type:
            return ''
        
        # If no agreement, can't check - leave empty
        if pd.isna(agreement) or not agreement:
            return 'No Carrier Agreement # - cannot verify cost'
        
        agreement = str(agreement).strip()
        
        # Get costs for this agreement
        if agreement not in agreement_costs:
            return f'Agreement {agreement} not found in cost files'
        
        combined_costs = agreement_costs[agreement]['combined']
        
        # Initialize tracking for this agreement
        if agreement not in found_by_agreement:
            found_by_agreement[agreement] = set()
            missing_by_agreement[agreement] = set()
            match_types_by_agreement[agreement] = {}
        
        # Try to find cost using multiple strategies
        found, matched_cost, match_type = find_cost_match(cost_type, combined_costs)
        
        if found:
            found_by_agreement[agreement].add(cost_type)
            if match_type != "exact":
                match_types_by_agreement[agreement][cost_type] = f"matched as '{matched_cost}' ({match_type})"
            return ''  # Found, no comment needed
        else:
            missing_by_agreement[agreement].add(cost_type)
            return f"Cost '{cost_type}' not found in rate card or accessorial costs"
    
    df['Comment'] = df.apply(get_comment, axis=1)
    
    # Print summary per agreement
    print("\n   Cost verification summary by agreement:")
    for agreement in sorted(set(found_by_agreement.keys()) | set(missing_by_agreement.keys())):
        found = found_by_agreement.get(agreement, set())
        missing = missing_by_agreement.get(agreement, set())
        fuzzy_matches = match_types_by_agreement.get(agreement, {})
        
        print(f"\n   {agreement}:")
        print(f"      Found in rate card/accessorial: {len(found)} cost types")
        
        # Show fuzzy matches (non-exact matches)
        if fuzzy_matches:
            print(f"      Fuzzy matches ({len(fuzzy_matches)}):")
            for cost, match_info in sorted(fuzzy_matches.items()):
                print(f"         [FUZZY] '{cost}' -> {match_info}")
        
        print(f"      Missing: {len(missing)} cost types")
        if missing:
            for cost in sorted(missing):
                print(f"         [MISSING] {cost}")
    
    # Count rows with comments
    rows_with_comment = len(df[df['Comment'].str.contains('not found', na=False)])
    print(f"\n   Total rows with 'cost not found' comment: {rows_with_comment}")
    
    return df


def clean_sheet_name(name):
    """Clean string to be a valid Excel sheet name (max 31 chars, no invalid chars)."""
    if name is None or pd.isna(name):
        return "No Agreement"
    name = str(name).strip()
    # Replace invalid characters with underscore
    name = re.sub(r'[\\/*?:\[\]]', '_', name)
    # Truncate to 31 characters
    return name[:31]


def save_result_with_tabs(df, output_filename="mismatch_filing.xlsx"):
    """Save the result to Excel file with separate tabs per Carrier Agreement #."""
    output_folder = get_partly_df_folder()
    output_folder.mkdir(exist_ok=True)
    
    output_path = output_folder / output_filename
    
    # Find Carrier Agreement # column
    agreement_col = 'Carrier Agreement #'
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if agreement_col in df.columns:
                unique_agreements = df[agreement_col].unique()
                
                sheet_count = 0
                for agreement in unique_agreements:
                    if pd.notna(agreement):
                        sheet_name = clean_sheet_name(agreement)
                        df_agreement = df[df[agreement_col] == agreement].copy()
                        df_agreement.to_excel(writer, sheet_name=sheet_name, index=False)
                        sheet_count += 1
                        print(f"   Tab '{sheet_name}': {len(df_agreement)} rows")
                
                # Handle rows with no agreement
                df_no_agreement = df[df[agreement_col].isna()].copy()
                if not df_no_agreement.empty:
                    sheet_name = "No Agreement"
                    df_no_agreement.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheet_count += 1
                    print(f"   Tab '{sheet_name}': {len(df_no_agreement)} rows")
                
                print(f"\n   Total: {sheet_count} tabs created")
            else:
                # Fallback to single sheet
                df.to_excel(writer, sheet_name='All Data', index=False)
                print(f"   Single tab 'All Data': {len(df)} rows")
        
        print(f"   Saved to: {output_path}")
        
    except PermissionError:
        alt_filename = output_filename.replace('.xlsx', '_new.xlsx')
        alt_path = output_folder / alt_filename
        
        with pd.ExcelWriter(alt_path, engine='openpyxl') as writer:
            if agreement_col in df.columns:
                unique_agreements = df[agreement_col].unique()
                for agreement in unique_agreements:
                    if pd.notna(agreement):
                        sheet_name = clean_sheet_name(agreement)
                        df_agreement = df[df[agreement_col] == agreement].copy()
                        df_agreement.to_excel(writer, sheet_name=sheet_name, index=False)
                
                df_no_agreement = df[df[agreement_col].isna()].copy()
                if not df_no_agreement.empty:
                    df_no_agreement.to_excel(writer, sheet_name="No Agreement", index=False)
            else:
                df.to_excel(writer, sheet_name='All Data', index=False)
        
        print(f"   [WARNING] Original file is open. Saved to: {alt_path}")
        output_path = alt_path
    
    return output_path


def main(include_positive_discrepancy=False):
    """Main function to process mismatches and add comments."""
    print("\n" + "="*80)
    print("MISMATCHES FILING")
    print("="*80)
    
    # Step 1: Get mismatch report from mismatch_report.py
    print("\n1. Getting mismatch report...")
    df_mismatch = get_mismatch_report(include_positive_discrepancy=include_positive_discrepancy)
    
    # Step 2: Get Carrier Agreement # mapping from lc_etof_with_comments.xlsx
    print("\n2. Getting Carrier Agreement # mapping from lc_etof_with_comments.xlsx...")
    etof_to_agreement = get_carrier_agreement_mapping_from_lc_etof()
    df_mismatch = add_carrier_agreement(df_mismatch, etof_to_agreement)
    
    # Step 3: Load costs for all agreements (rate_costs + accessorial_costs)
    print("\n3. Loading costs for all agreements...")
    agreement_costs = load_all_agreement_costs()
    
    # Step 4: Add comments for missing costs (checking corresponding rate card files)
    print("\n4. Adding comments for missing costs...")
    df_result = add_comment_for_missing_costs(df_mismatch, agreement_costs)
    
    # Step 5: Save result with tabs per Carrier Agreement #
    print("\n5. Saving result (separate tabs per Carrier Agreement #)...")
    output_path = save_result_with_tabs(df_result)
    
    print("\n" + "="*80)
    print(f"DONE! Output saved to: {output_path}")
    print("="*80)
    
    return df_result


if __name__ == "__main__":
    # Set to True to keep all discrepancies, False to keep only negative
    INCLUDE_POSITIVE_DISCREPANCY = False
    
    df_result = main(include_positive_discrepancy=INCLUDE_POSITIVE_DISCREPANCY)
