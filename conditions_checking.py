"""
Conditions Checking Script

This script checks mismatch rows against rate card data to determine
if costs are covered and adds a "Reason" column with the result.

Inputs (from xlsx files):
- mismatch_filing.xlsx from mismacthes_filing.py (all tabs)
- lc_etof_with_comments.xlsx from matching.py
- <agreement>_costs.xlsx files from rate_costs.py (all rate card cost files)

Logic:
1. For each row in mismatch filing:
   - If Comment column already has a value, use it as Reason
   - Get Cost type and ETOF_NUMBER
   - Look up Rate By and Applies If from cost conditions based on cost type
   - Check Applies If conditions:
     * Parse conditions like "Column Name equals 'value1', 'value2'"
     * Look up actual values in lc_etof_with_comments
     * Supported conditions: equals, does not equal, starts with, contains
     * If condition not met, set reason and skip
   - If Applies If = "No condition" or conditions are met:
     - Check Rate By Condition:
       a) If "PER SHIPMENT":
          - Find matching ETOF # in lc_etof_with_comments
       - Extract rate lane from "comment" column
       - Find Price Flat for the cost type
          - Reason: "The cost is pre-calculated by rate card - X flat."
       b) All other Rate By cases:
          - Find matching ETOF # in lc_etof_with_comments
          - Extract rate lane from "comment" column
          - Find Price per unit for the cost type
          - Find Price Flat MIN and MAX for the cost type (if exist)
          - Determine multiplier based on Rate By type:
            * Weight-based (contains "weight", "kg", "chargeable") -> use CHARGE_WEIGHT
            * Measurement-based (Quantity/, Condition/) -> look up in MEASUREMENT/UNITS_MEASUREMENT columns
          - Calculate: Price per unit * multiplier
          - Compare with MIN/MAX prices:
            - if calculated < MIN, apply MIN price
            - if calculated > MAX, apply MAX price
          - Reason: "Cost per unit: X, [MULTIPLIER_NAME]: Y, Total: X * Y = Z"
            OR: "MIN price applied - X (Calculated: ... but MIN is higher)"
            OR: "MAX price applied - X (Calculated: ... but MAX is lower)"
"""

import pandas as pd
import re
import sys
from pathlib import Path
from datetime import datetime


class Logger:
    """Logger class to write to both console and file."""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    
    def close(self):
        self.log.close()


def get_partly_df_folder():
    """Get the path to the partly_df folder."""
    return Path(__file__).parent / "partly_df"


def load_mismatch_filing():
    """
    Load mismatch filing result from file (all tabs combined).
    
    Returns:
        DataFrame with all mismatch data from all tabs
    """
    partly_df = get_partly_df_folder()
    mismatch_file = partly_df / "mismatch_filing.xlsx"
    
    if not mismatch_file.exists():
        raise FileNotFoundError(f"Mismatch filing file not found: {mismatch_file}")
    
    print(f"   Loading mismatch filing from: {mismatch_file}")
    
    # Read all sheets
    xlsx = pd.ExcelFile(mismatch_file)
    all_dfs = []
    
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        print(f"      Tab '{sheet_name}': {len(df)} rows")
        all_dfs.append(df)
    
    # Combine all tabs
    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        print(f"   Total rows loaded: {len(df_combined)}")
        return df_combined
    else:
        return pd.DataFrame()


def load_lc_etof_with_comments():
    """
    Load lc_etof_with_comments.xlsx from matching.py (all tabs combined).
    
    Returns:
        DataFrame with all LC-ETOF mapping data with comments
    """
    partly_df = get_partly_df_folder()
    lc_etof_file = partly_df / "lc_etof_with_comments.xlsx"
    
    if not lc_etof_file.exists():
        raise FileNotFoundError(f"LC-ETOF with comments file not found: {lc_etof_file}")
    
    print(f"   Loading LC-ETOF with comments from: {lc_etof_file}")
    
    # Read all sheets
    xlsx = pd.ExcelFile(lc_etof_file)
    all_dfs = []
    
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        print(f"      Tab '{sheet_name}': {len(df)} rows")
        all_dfs.append(df)
    
    # Combine all tabs
    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        # Remove duplicates based on ETOF # if present
        etof_col = None
        for col in df_combined.columns:
            if 'etof' in col.lower() and '#' in col.lower():
                etof_col = col
                break
        if etof_col:
            df_combined = df_combined.drop_duplicates(subset=[etof_col], keep='first')
        print(f"   Total unique rows loaded: {len(df_combined)}")
        return df_combined
    else:
        return pd.DataFrame()


def discover_cost_files():
    """
    Discover all cost files in partly_df/ folder.
    These are files created by rate_costs.py with pattern: <agreement>_costs.xlsx
    Excludes accessorial costs files (*_accessorial_costs.xlsx).
    
    Returns:
        dict: {agreement_number: file_path, ...}
    """
    partly_df = get_partly_df_folder()
    if not partly_df.exists():
        print(f"   [ERROR] partly_df folder not found: {partly_df}")
        return {}
    
    cost_files = {}
    for file in partly_df.glob("*_costs.xlsx"):
        # Skip accessorial costs files
        if "accessorial" in file.stem.lower():
            continue
        # Extract agreement number from filename (e.g., "RA20241129009_costs.xlsx" -> "RA20241129009")
        agreement_number = file.stem.replace("_costs", "")
        cost_files[agreement_number] = file
    
    return cost_files


def load_all_rate_costs():
    """
    Load all rate cost files from rate_costs.py.
    
    Returns:
        dict: {agreement_number: {'rate_data': DataFrame, 'cost_conditions': DataFrame}, ...}
    """
    cost_files = discover_cost_files()
    
    if not cost_files:
        print("   [WARNING] No cost files found in partly_df/")
        return {}
    
    print(f"   Found {len(cost_files)} cost file(s)")
    
    all_rate_costs = {}
    for agreement, file_path in cost_files.items():
        print(f"      Loading: {file_path.name}")
        try:
            xlsx = pd.ExcelFile(file_path)
            
            # Read the Rate Data sheet
            df_rate_data = None
            if 'Rate Data' in xlsx.sheet_names:
                df_rate_data = pd.read_excel(xlsx, sheet_name='Rate Data')
            else:
                df_rate_data = pd.read_excel(xlsx, sheet_name=0)
            
            # Read the Cost Conditions sheet (contains Cost Name, Rate By, Applies If)
            df_cost_conditions = None
            if 'Cost Conditions' in xlsx.sheet_names:
                df_cost_conditions = pd.read_excel(xlsx, sheet_name='Cost Conditions')
                print(f"         -> Rate Data: {len(df_rate_data)} rows, Cost Conditions: {len(df_cost_conditions)} costs")
            else:
                print(f"         -> Rate Data: {len(df_rate_data)} rows (no Cost Conditions sheet)")
            
            all_rate_costs[agreement] = {
                'rate_data': df_rate_data,
                'cost_conditions': df_cost_conditions
            }
        except Exception as e:
            print(f"         -> [ERROR] Failed to load: {e}")
    
    return all_rate_costs


def discover_accessorial_cost_files():
    """
    Discover all accessorial cost files in partly_df/ folder.
    These are files created by rate_accesorial_costs.py with pattern: <agreement>_accessorial_costs.xlsx
    
    Returns:
        dict: {agreement_number: file_path, ...}
    """
    partly_df = get_partly_df_folder()
    if not partly_df.exists():
        return {}
    
    accessorial_files = {}
    for file in partly_df.glob("*_accessorial_costs.xlsx"):
        # Extract agreement number from filename (e.g., "RA20241129009_accessorial_costs.xlsx" -> "RA20241129009")
        agreement_number = file.stem.replace("_accessorial_costs", "")
        accessorial_files[agreement_number] = file
    
    return accessorial_files


def load_all_accessorial_costs():
    """
    Discover all accessorial cost files (lazy loading - files are loaded on-demand).
    
    Returns:
        dict: {agreement_number: file_path, ...} - file paths, not DataFrames
    """
    accessorial_files = discover_accessorial_cost_files()
    
    if not accessorial_files:
        print("   [INFO] No accessorial cost files found in partly_df/")
        return {}
    
    print(f"   Found {len(accessorial_files)} accessorial cost file(s) (will load on-demand)")
    for agreement, file_path in accessorial_files.items():
        print(f"      - {agreement}: {file_path.name}")
    
    # Return file paths, not loaded DataFrames (lazy loading)
    return accessorial_files


# Cache for loaded accessorial cost DataFrames
_accessorial_cache = {}


def clear_accessorial_cache():
    """Clear the accessorial costs cache (call at start of each run)."""
    global _accessorial_cache
    _accessorial_cache = {}


def get_accessorial_data_for_agreement(agreement, all_accessorial_files, debug=False):
    """
    Load accessorial cost data for a specific agreement (with caching).
    
    Args:
        agreement: The agreement number
        all_accessorial_files: dict {agreement_number: file_path} from load_all_accessorial_costs()
        debug: If True, print debug information
    
    Returns:
        DataFrame or None if not found/failed to load
    """
    global _accessorial_cache
    
    # Check cache first
    if agreement in _accessorial_cache:
        return _accessorial_cache[agreement]
    
    # Try to find matching file
    file_path = all_accessorial_files.get(agreement)
    if file_path is None:
        # Try partial match
        for ag_key, fp in all_accessorial_files.items():
            if ag_key in agreement or agreement in ag_key:
                file_path = fp
                agreement = ag_key  # Use the matched key for caching
                break
    
    if file_path is None:
        if debug:
            print(f"      [DEBUG] No accessorial file found for agreement: {agreement}")
        return None
    
    # Load the file
    try:
        if debug:
            print(f"      [DEBUG] Loading accessorial file: {file_path.name}")
        
        xlsx = pd.ExcelFile(file_path, engine='openpyxl')
        
        # Read the first sheet (usually "Accessorial Costs")
        if 'Accessorial Costs' in xlsx.sheet_names:
            df_accessorial = pd.read_excel(xlsx, sheet_name='Accessorial Costs')
        else:
            df_accessorial = pd.read_excel(xlsx, sheet_name=0)
        
        if debug:
            print(f"      [DEBUG] Loaded accessorial data: {len(df_accessorial)} rows")
        
        # Cache the result
        _accessorial_cache[agreement] = df_accessorial
        return df_accessorial
        
    except Exception as e:
        error_msg = str(e)
        if 'zip' in error_msg.lower() or 'corrupt' in error_msg.lower():
            print(f"      [WARNING] Accessorial file may be corrupted: {file_path.name} - {error_msg}")
        else:
            print(f"      [WARNING] Failed to load accessorial file {file_path.name}: {error_msg}")
        
        # Cache the failure to avoid retrying
        _accessorial_cache[agreement] = None
        return None


def get_accessorial_cost_info(cost_type, df_accessorial, lane_number=None, debug=False):
    """
    Look up cost info from accessorial costs data.
    
    The accessorial data has a flat structure where each row contains:
    - Cost Name, Rate By, Multiplier, Lane #, Currency, Price Flat, Price per unit, Applies If
    
    Args:
        cost_type: The cost type name (e.g., "Cancellation Fee")
        df_accessorial: DataFrame from accessorial costs file
        lane_number: Optional lane number to filter by
        debug: If True, print debug information
    
    Returns:
        tuple: (rate_by, applies_if, price_flat, price_per_unit, has_min_flat) 
               or (None, None, None, None, None) if not found
    """
    if df_accessorial is None or df_accessorial.empty:
        return None, None, None, None, None
    
    # Find column names
    cost_name_col = None
    rate_by_col = None
    applies_if_col = None
    lane_col = None
    price_flat_col = None
    price_per_unit_col = None
    has_min_flat_col = None
    
    for col in df_accessorial.columns:
        col_lower = col.lower()
        if 'cost' in col_lower and 'name' in col_lower:
            cost_name_col = col
        elif 'rate' in col_lower and 'by' in col_lower:
            rate_by_col = col
        elif 'applies' in col_lower and 'if' in col_lower:
            applies_if_col = col
        elif 'lane' in col_lower:
            lane_col = col
        elif col_lower == 'price flat' or (col_lower == 'flat' and 'price' not in col_lower):
            price_flat_col = col
        elif 'per unit' in col_lower or 'price per' in col_lower:
            price_per_unit_col = col
        elif 'has min' in col_lower or 'min flat' in col_lower:
            has_min_flat_col = col
    
    if cost_name_col is None:
        if debug:
            print(f"      [DEBUG] Accessorial: No 'Cost Name' column found")
        return None, None, None, None, None
    
    if debug:
        print(f"      [DEBUG] Accessorial columns: Cost Name='{cost_name_col}', Rate By='{rate_by_col}', Lane='{lane_col}', Price Flat='{price_flat_col}', Price per unit='{price_per_unit_col}'")
    
    cost_type_clean = cost_type.strip().lower()
    # Also extract base name without parentheses
    base_cost_type = re.sub(r'\s*\([^)]*\)\s*$', '', cost_type_clean).strip()
    
    # Find matching rows
    matching_rows = []
    for idx, row in df_accessorial.iterrows():
        cost_name = str(row.get(cost_name_col, '')).strip()
        cost_name_lower = cost_name.lower()
        base_cost_name = re.sub(r'\s*\([^)]*\)\s*$', '', cost_name_lower).strip()
        
        # Check if cost matches (use startswith, not substring containment)
        is_match = (
            cost_name_lower == cost_type_clean or
            base_cost_name == base_cost_type or
            cost_name_lower.startswith(cost_type_clean) or
            cost_type_clean.startswith(cost_name_lower)
        )
        
        if is_match:
            # If lane_number is specified, check if it matches
            if lane_number is not None and lane_col is not None:
                row_lane = row.get(lane_col)
                if pd.notna(row_lane):
                    try:
                        if str(int(float(row_lane))).strip() != str(lane_number).strip():
                            continue
                    except (ValueError, TypeError):
                        continue
            matching_rows.append(row)
    
    if not matching_rows:
        if debug:
            print(f"      [DEBUG] Accessorial: No match found for cost type '{cost_type}'" + (f" lane {lane_number}" if lane_number else ""))
        return None, None, None, None, None
    
    # Use the first matching row (or the one matching the lane)
    row = matching_rows[0]
    
    rate_by = str(row.get(rate_by_col, '')).strip() if rate_by_col and pd.notna(row.get(rate_by_col)) else ''
    applies_if = str(row.get(applies_if_col, '')).strip() if applies_if_col and pd.notna(row.get(applies_if_col)) else ''
    
    price_flat = None
    if price_flat_col and pd.notna(row.get(price_flat_col)):
        try:
            val = row.get(price_flat_col)
            if val is not None and str(val).strip() != '':
                price_flat = float(val)
        except (ValueError, TypeError):
            pass
    
    price_per_unit = None
    if price_per_unit_col and pd.notna(row.get(price_per_unit_col)):
        try:
            val = row.get(price_per_unit_col)
            if val is not None and str(val).strip() != '':
                price_per_unit = float(val)
        except (ValueError, TypeError):
            pass
    
    has_min_flat = False
    if has_min_flat_col and pd.notna(row.get(has_min_flat_col)):
        val = str(row.get(has_min_flat_col)).strip().lower()
        has_min_flat = val in ('yes', 'true', '1', 'y')
    
    if debug:
        print(f"      [DEBUG] Accessorial: Found cost '{cost_type}': Rate By='{rate_by[:30]}...', Price Flat={price_flat}, Price per unit={price_per_unit}")
    
    return rate_by, applies_if, price_flat, price_per_unit, has_min_flat


def parse_date_string(date_str):
    """
    Parse a date string into a datetime object.
    Supports formats: DD.MM.YYYY, YYYY-MM-DD, MM/DD/YYYY
    
    Returns:
        datetime object or None if parsing fails
    """
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    formats = [
        '%d.%m.%Y',  # DD.MM.YYYY
        '%Y-%m-%d',  # YYYY-MM-DD
        '%m/%d/%Y',  # MM/DD/YYYY
        '%d/%m/%Y',  # DD/MM/YYYY
        '%Y%m%d',    # YYYYMMDD
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None


def get_ship_date_from_row_data(row_data, debug=False):
    """
    Extract ship date from row data dictionary.
    
    Args:
        row_data: Dict of column -> value from ETOF row
        debug: If True, print debug information
    
    Returns:
        Ship date as string or None if not found
    """
    if not row_data:
        return None
    
    # Patterns for ship date column names
    ship_date_patterns = ['SHIP_DATE', 'ship_date', 'Ship Date', 'Loading date', 'Ship date', 'loading_date']
    
    for key in row_data.keys():
        key_str = str(key)
        for pattern in ship_date_patterns:
            if key_str.lower() == pattern.lower() or key_str.lower().replace(' ', '_') == pattern.lower().replace(' ', '_'):
                val = row_data[key]
                if pd.notna(val) and str(val).strip():
                    # Handle datetime objects
                    if hasattr(val, 'strftime'):
                        result = val.strftime('%d.%m.%Y')
                    else:
                        result = str(val).strip()
                    if debug:
                        print(f"      [DEBUG] Found ship date: '{result}' from column '{key}'")
                    return result
    
    if debug:
        print(f"      [DEBUG] Ship date not found in row data (checked {len(row_data)} columns)")
    return None


def is_date_in_validity_range(ship_date, valid_from, valid_to, debug=False):
    """
    Check if a ship date is within the validity range.
    
    Args:
        ship_date: The ship date (datetime object or string)
        valid_from: Start of validity period (string or None)
        valid_to: End of validity period (string or None)
        debug: If True, print debug information
    
    Returns:
        tuple: (is_valid, reason_if_invalid)
        - is_valid: True if ship_date is within range or no range is defined
        - reason_if_invalid: String explaining why invalid, or None if valid
    """
    # No validity constraints - always valid
    if not valid_from and not valid_to:
        return True, None
    
    # Parse ship date
    if isinstance(ship_date, datetime):
        ship_dt = ship_date
    else:
        ship_dt = parse_date_string(ship_date)
    
    if not ship_dt:
        if debug:
            print(f"      [DEBUG] Could not parse ship date: {ship_date}")
        # Can't validate - assume valid
        return True, None
    
    # Parse validity dates
    from_dt = parse_date_string(valid_from) if valid_from else None
    to_dt = parse_date_string(valid_to) if valid_to else None
    
    if debug:
        print(f"      [DEBUG] Date validity check: ship={ship_dt.strftime('%Y-%m-%d') if ship_dt else None}, from={from_dt.strftime('%Y-%m-%d') if from_dt else None}, to={to_dt.strftime('%Y-%m-%d') if to_dt else None}")
    
    # Check if ship date is before valid_from
    if from_dt and ship_dt < from_dt:
        reason = f"Ship date {ship_dt.strftime('%d.%m.%Y')} is before validity start {from_dt.strftime('%d.%m.%Y')}"
        if debug:
            print(f"      [DEBUG] {reason}")
        return False, reason
    
    # Check if ship date is after valid_to
    if to_dt and ship_dt > to_dt:
        reason = f"Ship date {ship_dt.strftime('%d.%m.%Y')} is after validity end {to_dt.strftime('%d.%m.%Y')}"
        if debug:
            print(f"      [DEBUG] {reason}")
        return False, reason
    
    return True, None


def get_all_matching_accessorial_costs(cost_type, df_accessorial, debug=False):
    """
    Find ALL accessorial cost entries that match the base cost name.
    Similar to get_all_matching_cost_conditions but for accessorial costs.
    
    Args:
        cost_type: The cost type name from mismatch
        df_accessorial: DataFrame from accessorial costs file
        debug: If True, print debug information
    
    Returns:
        List of tuples: [(cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, lane_num, valid_from, valid_to, is_percentage, percentage_value, applied_over), ...]
    """
    if df_accessorial is None or df_accessorial.empty:
        return []
    
    # Find column names
    cost_name_col = None
    rate_by_col = None
    applies_if_col = None
    lane_col = None
    price_flat_col = None
    price_per_unit_col = None
    has_min_flat_col = None
    valid_from_col = None
    valid_to_col = None
    is_percentage_col = None
    percentage_col = None
    applied_over_col = None
    
    for col in df_accessorial.columns:
        col_lower = col.lower()
        if 'cost' in col_lower and 'name' in col_lower:
            cost_name_col = col
        elif 'rate' in col_lower and 'by' in col_lower:
            rate_by_col = col
        elif 'applies' in col_lower and 'if' in col_lower:
            applies_if_col = col
        elif 'lane' in col_lower:
            lane_col = col
        elif 'price flat min' in col_lower or col_lower == 'price flat min':
            # Explicit check for "Price Flat MIN" column first
            price_flat_col = col
        elif col_lower == 'price flat' or 'price flat' in col_lower:
            # Only set if not already set by "Price Flat MIN" check
            if price_flat_col is None:
                price_flat_col = col
        elif 'per unit' in col_lower or 'price per' in col_lower:
            price_per_unit_col = col
        elif 'has min' in col_lower or 'min flat' in col_lower:
            has_min_flat_col = col
        elif 'valid' in col_lower and 'from' in col_lower:
            valid_from_col = col
        elif 'valid' in col_lower and 'to' in col_lower:
            valid_to_col = col
        elif col_lower == 'is percentage' or 'is percentage' in col_lower:
            is_percentage_col = col
        elif col_lower == 'percentage' and 'is' not in col_lower:
            percentage_col = col
        elif 'applied over' in col_lower:
            applied_over_col = col
    
    if debug:
        print(f"      [DEBUG] Accessorial columns detected: cost_name={cost_name_col}, lane={lane_col}, price_flat={price_flat_col}, price_per_unit={price_per_unit_col}, valid_from={valid_from_col}, valid_to={valid_to_col}")
        print(f"      [DEBUG] Percentage columns: is_percentage={is_percentage_col}, percentage={percentage_col}, applied_over={applied_over_col}")
    
    if cost_name_col is None:
        return []
    
    matches = []
    cost_type_clean = cost_type.strip().lower()
    base_cost_type = re.sub(r'\s*\([^)]*\)\s*$', '', cost_type_clean).strip()
    
    for idx, row in df_accessorial.iterrows():
        cost_name = str(row.get(cost_name_col, '')).strip()
        cost_name_lower = cost_name.lower()
        base_cost_name = re.sub(r'\s*\([^)]*\)\s*$', '', cost_name_lower).strip()
        
        # Use startswith, not substring containment (e.g., "DGR Fee" should NOT match "Air DGR Fee")
        is_match = (
            cost_name_lower == cost_type_clean or
            base_cost_name == base_cost_type or
            cost_name_lower.startswith(cost_type_clean) or
            cost_type_clean.startswith(cost_name_lower)
        )
        
        if is_match:
            rate_by = str(row.get(rate_by_col, '')).strip() if rate_by_col and pd.notna(row.get(rate_by_col)) else ''
            applies_if = str(row.get(applies_if_col, '')).strip() if applies_if_col and pd.notna(row.get(applies_if_col)) else ''
            
            lane_num = None
            if lane_col and pd.notna(row.get(lane_col)):
                try:
                    lane_num = int(float(row.get(lane_col)))
                except (ValueError, TypeError):
                    pass
            
            price_flat = None
            if price_flat_col:
                raw_val = row.get(price_flat_col)
                if debug:
                    print(f"      [DEBUG] Price flat raw value for '{cost_name}': {repr(raw_val)} (col={price_flat_col})")
                if pd.notna(raw_val):
                    try:
                        if raw_val is not None and str(raw_val).strip() != '':
                            price_flat = float(raw_val)
                            if debug:
                                print(f"      [DEBUG] Extracted price_flat: {price_flat}")
                    except (ValueError, TypeError) as e:
                        if debug:
                            print(f"      [DEBUG] Failed to convert price_flat: {e}")
            
            price_per_unit = None
            if price_per_unit_col and pd.notna(row.get(price_per_unit_col)):
                try:
                    val = row.get(price_per_unit_col)
                    if val is not None and str(val).strip() != '':
                        price_per_unit = float(val)
                except (ValueError, TypeError):
                    pass
            
            has_min_flat = False
            if has_min_flat_col and pd.notna(row.get(has_min_flat_col)):
                val = str(row.get(has_min_flat_col)).strip().lower()
                has_min_flat = val in ('yes', 'true', '1', 'y')
            
            # Extract Valid From and Valid To dates
            valid_from = None
            valid_to = None
            if valid_from_col and pd.notna(row.get(valid_from_col)):
                val = row.get(valid_from_col)
                if hasattr(val, 'strftime'):
                    valid_from = val.strftime('%d.%m.%Y')
                else:
                    valid_from = str(val).strip() if str(val).strip() else None
            
            if valid_to_col and pd.notna(row.get(valid_to_col)):
                val = row.get(valid_to_col)
                if hasattr(val, 'strftime'):
                    valid_to = val.strftime('%d.%m.%Y')
                else:
                    valid_to = str(val).strip() if str(val).strip() else None
            
            # Extract percentage information
            is_percentage = False
            percentage_value = None
            applied_over = None
            
            if is_percentage_col and pd.notna(row.get(is_percentage_col)):
                val = str(row.get(is_percentage_col)).strip().lower()
                is_percentage = val in ('yes', 'true', '1', 'y')
            
            if percentage_col and pd.notna(row.get(percentage_col)):
                try:
                    val = row.get(percentage_col)
                    if val is not None and str(val).strip() != '':
                        percentage_value = float(val)
                except (ValueError, TypeError):
                    pass
            
            if applied_over_col and pd.notna(row.get(applied_over_col)):
                applied_over = str(row.get(applied_over_col)).strip()
            
            matches.append((cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, lane_num, valid_from, valid_to, is_percentage, percentage_value, applied_over))
            
            if debug and (valid_from or valid_to):
                print(f"      [DEBUG] Accessorial '{cost_name}' lane {lane_num}: Valid From={valid_from}, Valid To={valid_to}")
            if debug and is_percentage:
                print(f"      [DEBUG] Accessorial '{cost_name}' lane {lane_num}: PERCENTAGE={percentage_value}% over '{applied_over}'")
    
    if debug and matches:
        print(f"      [DEBUG] Accessorial: Found {len(matches)} matching entries for '{cost_type}'")
    
    return matches


def find_best_matching_accessorial_cost(cost_type, df_accessorial, lane_number, etof_row_data, debug=False, ship_date=None):
    """
    Find the best matching accessorial cost entry for a given cost type and lane.
    
    Args:
        cost_type: The cost type name from mismatch
        df_accessorial: DataFrame from accessorial costs file
        lane_number: The lane number to match
        etof_row_data: Dict of column -> value for this ETOF's shipment data
        debug: If True, print debug information
        ship_date: The ship date (string or datetime) to check against validity dates
    
    Returns:
        tuple: (cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, is_percentage, percentage_value, applied_over)
               or (None, None, None, None, None, None, None, None, None) if not found
    """
    if debug:
        print(f"      [DEBUG] find_best_matching_accessorial_cost: looking for '{cost_type}', lane={lane_number}, ship_date={ship_date}")
    
    all_matches = get_all_matching_accessorial_costs(cost_type, df_accessorial, debug=debug)
    
    if not all_matches:
        if debug:
            print(f"      [DEBUG] Accessorial: no matches found for '{cost_type}'")
        return None, None, None, None, None, None, None, None, None
    
    if debug:
        print(f"      [DEBUG] Accessorial: found {len(all_matches)} total matches for '{cost_type}'")
    
    # Filter by lane number if provided
    lane_matches = []
    for match in all_matches:
        cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, match_lane, valid_from, valid_to, is_percentage, percentage_value, applied_over = match
        if lane_number is not None and match_lane is not None:
            if str(match_lane) == str(lane_number):
                lane_matches.append(match)
                if debug:
                    print(f"      [DEBUG] Accessorial: lane {match_lane} matches target lane {lane_number}")
        elif match_lane is None:
            # No lane specified in the data - could apply to any lane
            lane_matches.append(match)
    
    if not lane_matches:
        # No lane match - try using all matches
        if debug:
            print(f"      [DEBUG] Accessorial: no lane-specific matches, using all {len(all_matches)} matches")
        lane_matches = all_matches
    else:
        if debug:
            print(f"      [DEBUG] Accessorial: {len(lane_matches)} lane-filtered matches")
    
    # Filter by date validity if ship_date is provided
    if ship_date:
        valid_matches = []
        invalid_matches = []
        for match in lane_matches:
            cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, match_lane, valid_from, valid_to, is_pct, pct_val, applied_over = match
            is_valid, validity_reason = is_date_in_validity_range(ship_date, valid_from, valid_to, debug=debug)
            if is_valid:
                valid_matches.append(match)
                if debug:
                    print(f"      [DEBUG] Accessorial: '{cost_name}' lane {match_lane} VALID for ship_date={ship_date}, valid_from={valid_from}, valid_to={valid_to}")
            else:
                invalid_matches.append((match, validity_reason))
                if debug:
                    print(f"      [DEBUG] Accessorial: '{cost_name}' lane {match_lane} EXCLUDED due to date validity: {validity_reason}")
        
        if valid_matches:
            if debug:
                print(f"      [DEBUG] Accessorial: {len(valid_matches)} matches after date validity filter (excluded {len(invalid_matches)})")
            
            # If multiple valid matches, prioritize by validity period (most recent valid_to date)
            if len(valid_matches) > 1:
                # Parse ship_date once
                if isinstance(ship_date, datetime):
                    ship_dt = ship_date
                else:
                    ship_dt = parse_date_string(ship_date)
                
                # Sort matches by valid_to date (most recent first), then by valid_from (most recent first)
                def get_valid_to_date(match):
                    _, _, _, _, _, _, _, valid_from, valid_to, _, _, _ = match
                    if valid_to:
                        to_dt = parse_date_string(valid_to)
                        return to_dt if to_dt else datetime.min
                    return datetime.min
                
                def get_valid_from_date(match):
                    _, _, _, _, _, _, _, valid_from, valid_to, _, _, _ = match
                    if valid_from:
                        from_dt = parse_date_string(valid_from)
                        return from_dt if from_dt else datetime.min
                    return datetime.min
                
                # Sort by valid_to (descending - most recent first), then by valid_from (descending)
                valid_matches.sort(key=lambda x: (get_valid_to_date(x), get_valid_from_date(x)), reverse=True)
                
                if debug:
                    for i, match in enumerate(valid_matches[:3]):  # Show top 3
                        _, _, _, _, _, _, _, vf, vt, _, _, _ = match
                        print(f"      [DEBUG] Accessorial: Valid match #{i+1}: valid_from={vf}, valid_to={vt}")
            
            lane_matches = valid_matches
        else:
            if debug:
                print(f"      [DEBUG] Accessorial: ALL matches excluded by date validity, keeping original {len(lane_matches)}")
            # Keep original matches if all are invalid (fallback)
    
    if len(lane_matches) == 1:
        match = lane_matches[0]
        if debug:
            print(f"      [DEBUG] Accessorial: single match found: {match[0]}")
        # Return: cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, is_percentage, percentage_value, applied_over
        return match[0], match[1], match[2], match[3], match[4], match[5], match[9], match[10], match[11]
    
    # Multiple matches - check Applies If conditions
    if debug:
        print(f"      [DEBUG] Accessorial: multiple matches ({len(lane_matches)}), checking Applies If conditions...")
    
    matches_with_conditions_met = []
    matches_without_conditions = []
    
    for match in lane_matches:
        cost_name, rate_by, applies_if, price_flat, price_per_unit, has_min_flat, _, valid_from, valid_to, is_pct, pct_val, applied_over = match
        
        parsed_conditions = parse_applies_if_condition(applies_if, debug=False)
        
        if not parsed_conditions:
            matches_without_conditions.append(match)
            if debug:
                print(f"      [DEBUG] Accessorial: '{cost_name}' has no parseable conditions")
            continue
        
        if etof_row_data:
            is_met, reason = check_applies_if_condition(parsed_conditions, "check", etof_row_data, debug=False)
            
            if is_met:
                matches_with_conditions_met.append(match)
                if debug:
                    print(f"      [DEBUG] Accessorial: Conditions MET for: {cost_name}")
            else:
                if debug:
                    print(f"      [DEBUG] Accessorial: Conditions NOT met for: {cost_name} - {reason}")
        else:
            matches_without_conditions.append(match)
    
    # Prefer matches whose conditions are met
    if matches_with_conditions_met:
        best_match = max(matches_with_conditions_met, key=lambda x: len(x[0]))
        if debug:
            print(f"      [DEBUG] Accessorial: selected best match (conditions met): {best_match[0]}")
        return best_match[0], best_match[1], best_match[2], best_match[3], best_match[4], best_match[5], best_match[9], best_match[10], best_match[11]
    
    if matches_without_conditions:
        best_match = min(matches_without_conditions, key=lambda x: len(x[0]))
        if debug:
            print(f"      [DEBUG] Accessorial: selected fallback match (no conditions): {best_match[0]}")
        return best_match[0], best_match[1], best_match[2], best_match[3], best_match[4], best_match[5], best_match[9], best_match[10], best_match[11]
    
    if lane_matches:
        match = lane_matches[0]
        if debug:
            print(f"      [DEBUG] Accessorial: using first match as fallback: {match[0]}")
        return match[0], match[1], match[2], match[3], match[4], match[5], match[9], match[10], match[11]
    
    if debug:
        print(f"      [DEBUG] Accessorial: no suitable match found")
    return None, None, None, None, None, None, None, None, None


def get_cost_conditions_for_cost_type(cost_type, df_cost_conditions, debug=False):
    """
    Look up Rate By and Applies If values for a cost type from the cost conditions.
    
    Args:
        cost_type: The cost type name (e.g., "Air DGR Fee")
        df_cost_conditions: DataFrame with Cost Name, Rate By, Applies If columns
        debug: If True, print debug information
    
    Returns:
        tuple: (rate_by, applies_if) or (None, None) if not found
    """
    if df_cost_conditions is None or df_cost_conditions.empty:
        return None, None
    
    # Find Cost Name column
    cost_name_col = None
    for col in df_cost_conditions.columns:
        if 'cost' in col.lower() and 'name' in col.lower():
            cost_name_col = col
            break
    
    if cost_name_col is None:
        if debug:
            print(f"      [DEBUG] No 'Cost Name' column found in cost conditions")
        return None, None
    
    # Find Rate By column
    rate_by_col = None
    for col in df_cost_conditions.columns:
        if 'rate' in col.lower() and 'by' in col.lower():
            rate_by_col = col
            break
    
    # Find Applies If column
    applies_if_col = None
    for col in df_cost_conditions.columns:
        if 'applies' in col.lower() and 'if' in col.lower():
            applies_if_col = col
            break
    
    if debug:
        print(f"      [DEBUG] Cost conditions columns: Cost Name='{cost_name_col}', Rate By='{rate_by_col}', Applies If='{applies_if_col}'")
    
    # Look for exact match first
    cost_type_clean = cost_type.strip().lower()
    for _, row in df_cost_conditions.iterrows():
        cost_name = str(row.get(cost_name_col, '')).strip()
        if cost_name.lower() == cost_type_clean:
            rate_by = row.get(rate_by_col, '') if rate_by_col else ''
            applies_if = row.get(applies_if_col, '') if applies_if_col else ''
            if debug:
                print(f"      [DEBUG] Found exact match for '{cost_type}': Rate By='{str(rate_by)[:30]}...', Applies If='{str(applies_if)[:30]}...'")
            return rate_by, applies_if
    
    # Try partial match (cost type contained in cost name or vice versa)
    for _, row in df_cost_conditions.iterrows():
        cost_name = str(row.get(cost_name_col, '')).strip()
        if cost_type_clean in cost_name.lower() or cost_name.lower() in cost_type_clean:
            rate_by = row.get(rate_by_col, '') if rate_by_col else ''
            applies_if = row.get(applies_if_col, '') if applies_if_col else ''
            if debug:
                print(f"      [DEBUG] Found partial match for '{cost_type}' -> '{cost_name}': Rate By='{str(rate_by)[:30]}...', Applies If='{str(applies_if)[:30]}...'")
            return rate_by, applies_if
    
    if debug:
        print(f"      [DEBUG] No match found for cost type '{cost_type}'")
    
    return None, None


def get_all_matching_cost_conditions(cost_type, df_cost_conditions, debug=False):
    """
    Find ALL cost types that match the base cost name (e.g., all variations of "Delivery Fee").
    
    This is used when there are multiple cost variations like:
    - Delivery Fee (Getafe, Madrid)
    - Delivery Fee (Illecas)
    - Delivery Fee (Small van to Sevilla)
    
    Args:
        cost_type: The cost type name from mismatch (e.g., "Delivery Fee")
        df_cost_conditions: DataFrame with Cost Name, Rate By, Applies If columns
        debug: If True, print debug information
    
    Returns:
        List of tuples: [(cost_name, rate_by, applies_if), ...]
    """
    if df_cost_conditions is None or df_cost_conditions.empty:
        return []
    
    # Find column names
    cost_name_col = None
    rate_by_col = None
    applies_if_col = None
    
    for col in df_cost_conditions.columns:
        col_lower = col.lower()
        if 'cost' in col_lower and 'name' in col_lower:
            cost_name_col = col
        elif 'rate' in col_lower and 'by' in col_lower:
            rate_by_col = col
        elif 'applies' in col_lower and 'if' in col_lower:
            applies_if_col = col
    
    if cost_name_col is None:
        return []
    
    matches = []
    cost_type_clean = cost_type.strip().lower()
    
    # Extract base name (without parentheses) for matching
    # "Delivery Fee" from "Delivery Fee (Getafe)" 
    base_cost_type = re.sub(r'\s*\([^)]*\)\s*$', '', cost_type_clean).strip()
    
    for _, row in df_cost_conditions.iterrows():
        cost_name = str(row.get(cost_name_col, '')).strip()
        cost_name_lower = cost_name.lower()
        
        # Extract base name from this cost condition too
        base_cost_name = re.sub(r'\s*\([^)]*\)\s*$', '', cost_name_lower).strip()
        
        # Match if:
        # 1. Exact match
        # 2. Base names match (e.g., "Delivery Fee" matches "Delivery Fee (Getafe)")
        # 3. Cost name STARTS WITH cost type (e.g., "DGR Fee (Hazardous)" starts with "DGR Fee")
        # 4. Cost type STARTS WITH cost name (reverse of 3)
        # NOTE: We do NOT use substring containment (e.g., "DGR Fee" in "Air DGR Fee")
        #       because "DGR Fee" and "Air DGR Fee" are DIFFERENT costs
        is_match = (
            cost_name_lower == cost_type_clean or
            base_cost_name == base_cost_type or
            cost_name_lower.startswith(cost_type_clean) or
            cost_type_clean.startswith(cost_name_lower)
        )
        
        if is_match:
            rate_by = row.get(rate_by_col, '') if rate_by_col else ''
            applies_if = row.get(applies_if_col, '') if applies_if_col else ''
            matches.append((cost_name, rate_by, applies_if))
    
    if debug and matches:
        print(f"      [DEBUG] Found {len(matches)} matching costs for '{cost_type}':")
        for name, _, ai in matches:
            print(f"         - {name}: {str(ai)[:40]}...")
    
    return matches


def find_best_matching_cost(cost_type, df_cost_conditions, etof_row_data, debug=False):
    """
    Find the best matching cost type by checking Applies If conditions.
    
    When there are multiple cost variations (e.g., "Delivery Fee (Getafe)", "Delivery Fee (Sevilla)"),
    this function finds the one whose Applies If conditions are met by the shipment data.
    
    Args:
        cost_type: The cost type name from mismatch
        df_cost_conditions: DataFrame with Cost Name, Rate By, Applies If columns
        etof_row_data: Dict of column -> value for this ETOF's shipment data
        debug: If True, print debug information
    
    Returns:
        tuple: (cost_name, rate_by, applies_if) for the best match, or (None, None, None) if not found
    """
    # Get all matching costs
    all_matches = get_all_matching_cost_conditions(cost_type, df_cost_conditions, debug=debug)
    
    if not all_matches:
        return None, None, None
    
    # If only one match, return it
    if len(all_matches) == 1:
        return all_matches[0]
    
    if debug:
        print(f"      [DEBUG] Multiple matches found, checking Applies If conditions...")
    
    # Check each match's Applies If conditions against the shipment data
    matches_with_conditions_met = []
    matches_without_conditions = []
    
    for cost_name, rate_by, applies_if in all_matches:
        # Parse the applies if conditions
        parsed_conditions = parse_applies_if_condition(applies_if, debug=False)
        
        if not parsed_conditions:
            # No conditions to check - this is a fallback option
            matches_without_conditions.append((cost_name, rate_by, applies_if))
            continue
        
        # Check if conditions are met
        if etof_row_data:
            is_met, _ = check_applies_if_condition(parsed_conditions, "check", etof_row_data, debug=False)
            
            if is_met:
                matches_with_conditions_met.append((cost_name, rate_by, applies_if))
                if debug:
                    print(f"      [DEBUG] Conditions MET for: {cost_name}")
            else:
                if debug:
                    print(f"      [DEBUG] Conditions NOT met for: {cost_name}")
        else:
            matches_without_conditions.append((cost_name, rate_by, applies_if))
    
    # Prefer matches whose conditions are met
    if matches_with_conditions_met:
        # If multiple matches have conditions met, prefer the most specific one (longer name usually)
        best_match = max(matches_with_conditions_met, key=lambda x: len(x[0]))
        if debug:
            print(f"      [DEBUG] Selected best match: {best_match[0]}")
        return best_match
    
    # Fall back to matches without conditions
    if matches_without_conditions:
        # Prefer shorter name (base cost without specifics)
        best_match = min(matches_without_conditions, key=lambda x: len(x[0]))
        if debug:
            print(f"      [DEBUG] No conditions met, using fallback: {best_match[0]}")
        return best_match
    
    # Nothing matched - return the first one as fallback
    if debug:
        print(f"      [DEBUG] No conditions met for any match, using first: {all_matches[0][0]}")
    return all_matches[0]


def parse_applies_if_condition(applies_if_text, debug=False):
    """
    Parse an Applies If condition to extract the column name, condition type, and expected values.
    
    Examples:
    - "1. Carrier Name equals 'Bollore DE (EUR)', 'Bollore ES (EUR)'"
    - "1. DANGEROUS_GOODS starts with 'Y' in all items"
    - "1. CONT_LOAD equals 'LTL/STANDARD' in all items"
    - "1. Equipment Type contains 'BCL', 'LCL'"
    
    Returns:
        List of tuples: [(column_name, condition_type, expected_values), ...]
        where condition_type is one of: 'equals', 'starts_with', 'contains', 'does_not_equal'
        and expected_values is a list of strings
    """
    import re
    
    if not applies_if_text or pd.isna(applies_if_text):
        return []
    
    text = str(applies_if_text).strip()
    
    # Skip "No condition" or empty
    if not text or 'no condition' in text.lower():
        return []
    
    # Skip "Applies if invoiced by Carrier" as it's not a real condition
    if 'applies if invoiced' in text.lower() and 'carrier' in text.lower():
        # Check if there are other conditions
        if len(text) < 50 and 'equals' not in text.lower() and 'starts' not in text.lower() and 'contains' not in text.lower():
            return []
    
    conditions = []
    
    # Pattern to match conditions like:
    # "Column Name equals 'value1', 'value2'"
    # "COLUMN_NAME starts with 'value'"
    # "Column Name contains 'value'"
    # "Column Name does not equal 'value'"
    
    # Split by numbered conditions (1., 2., etc.)
    parts = re.split(r'\d+\.\s*', text)
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        # Remove "in all items" suffix
        part = re.sub(r'\s+in all items\s*$', '', part, flags=re.IGNORECASE)
        
        # Split by " and " to handle multiple conditions in same part
        # e.g., "Origin Country does not equal to 'ES' and Destination Country does not equal to 'SG'"
        sub_parts = re.split(r'\s+and\s+', part, flags=re.IGNORECASE)
        
        for sub_part in sub_parts:
            sub_part = sub_part.strip()
            if not sub_part:
                continue
            
            # Try to match different condition types
            # Pattern: COLUMN_NAME condition_type 'value1', 'value2', ...
            # IMPORTANT: Check "does not equal" BEFORE "equals" to avoid partial matching!
            
            # Does not equal pattern - MUST be checked FIRST!
            # Handles "does not equal" and "does not equal to"
            not_equal_match = re.match(r"(.+?)\s+does\s+not\s+equal\s*(?:to\s+)?(.+)", sub_part, re.IGNORECASE)
            if not_equal_match:
                column_name = not_equal_match.group(1).strip()
                values_str = not_equal_match.group(2).strip()
                values = re.findall(r"'([^']*)'", values_str)
                if values:
                    conditions.append((column_name, 'does_not_equal', values))
                    if debug:
                        print(f"      [DEBUG] Parsed condition: {column_name} does not equal {values}")
                continue
            
            # Does not contain pattern - also before "contains"
            not_contain_match = re.match(r"(.+?)\s+does\s+not\s+contain\s*(.+)", sub_part, re.IGNORECASE)
            if not_contain_match:
                column_name = not_contain_match.group(1).strip()
                values_str = not_contain_match.group(2).strip()
                values = re.findall(r"'([^']*)'", values_str)
                if values:
                    conditions.append((column_name, 'does_not_contain', values))
                    if debug:
                        print(f"      [DEBUG] Parsed condition: {column_name} does not contain {values}")
                continue
            
            # Equals pattern - checked AFTER "does not equal"
            equals_match = re.match(r"(.+?)\s+equals?\s*(?:to\s+)?(.+)", sub_part, re.IGNORECASE)
            if equals_match:
                column_name = equals_match.group(1).strip()
                values_str = equals_match.group(2).strip()
                values = re.findall(r"'([^']*)'", values_str)
                if values:
                    conditions.append((column_name, 'equals', values))
                    if debug:
                        print(f"      [DEBUG] Parsed condition: {column_name} equals {values}")
                continue
            
            # Starts with pattern
            starts_match = re.match(r"(.+?)\s+starts?\s+with\s+(.+)", sub_part, re.IGNORECASE)
            if starts_match:
                column_name = starts_match.group(1).strip()
                values_str = starts_match.group(2).strip()
                values = re.findall(r"'([^']*)'", values_str)
                if values:
                    conditions.append((column_name, 'starts_with', values))
                    if debug:
                        print(f"      [DEBUG] Parsed condition: {column_name} starts with {values}")
                continue
            
            # Contains pattern - checked AFTER "does not contain"
            contains_match = re.match(r"(.+?)\s+contains?\s+(.+)", sub_part, re.IGNORECASE)
            if contains_match:
                column_name = contains_match.group(1).strip()
                values_str = contains_match.group(2).strip()
                values = re.findall(r"'([^']*)'", values_str)
                if values:
                    conditions.append((column_name, 'contains', values))
                    if debug:
                        print(f"      [DEBUG] Parsed condition: {column_name} contains {values}")
                continue
    
    return conditions


def check_applies_if_condition(conditions, etof_number, df_lc_etof_row, debug=False):
    """
    Check if the Applies If conditions are met for a given ETOF row.
    
    Args:
        conditions: List of tuples from parse_applies_if_condition()
        etof_number: The ETOF number for error messages
        df_lc_etof_row: Dictionary of column -> value for this ETOF row
        debug: If True, print debug information
    
    Returns:
        Tuple: (is_met, reason_if_not_met)
        - is_met: True if all conditions are met (or no conditions)
        - reason_if_not_met: Explanation of why condition failed (or None if met)
    """
    if not conditions:
        return True, None
    
    # Column name mappings - maps condition column names to actual data column names
    column_mappings = {
        'origin country': ['SHIP_COUNTRY', 'ship_country', 'Origin Country', 'origin_country'],
        'destination country': ['CUST_COUNTRY', 'cust_country', 'Destination Country', 'destination_country', 'dest_country'],
        'origin_country': ['SHIP_COUNTRY', 'ship_country', 'Origin Country'],
        'destination_country': ['CUST_COUNTRY', 'cust_country', 'Destination Country'],
        'ship country': ['SHIP_COUNTRY', 'ship_country'],
        'cust country': ['CUST_COUNTRY', 'cust_country'],
    }
    
    for column_name, condition_type, expected_values in conditions:
        # Find the matching column in the row (try different variations)
        actual_value = None
        matched_column = None
        
        column_name_lower = column_name.lower().replace(' ', '_').replace('-', '_')
        column_name_lower_nospace = column_name.lower().replace(' ', '').replace('_', '')
        
        # First, check if there's a specific mapping for this column name
        mapped_columns = column_mappings.get(column_name.lower(), [])
        if not mapped_columns:
            mapped_columns = column_mappings.get(column_name_lower, [])
        
        # Try mapped columns first
        if mapped_columns:
            for mapped_col in mapped_columns:
                for col, val in df_lc_etof_row.items():
                    if col.lower() == mapped_col.lower() or col == mapped_col:
                        actual_value = val
                        matched_column = col
                        if debug:
                            print(f"      [DEBUG] Used column mapping: '{column_name}' -> '{col}'")
                        break
                if matched_column:
                    break
        
        # If no mapped column found, try standard matching
        if matched_column is None:
            for col, val in df_lc_etof_row.items():
                col_lower = str(col).lower().replace(' ', '_').replace('-', '_')
                col_lower_nospace = str(col).lower().replace(' ', '').replace('_', '')
                
                if (col_lower == column_name_lower or 
                    col_lower_nospace == column_name_lower_nospace or
                    column_name.lower() in col.lower() or
                    col.lower() in column_name.lower()):
                    actual_value = val
                    matched_column = col
                    break
        
        if debug:
            print(f"      [DEBUG] Checking condition: {column_name} {condition_type} {expected_values}")
            print(f"      [DEBUG] Matched column: {matched_column}, Actual value: {actual_value}")
        
        if matched_column is None:
            # Column not found - condition cannot be verified
            return False, f"Column '{column_name}' not found in shipment data for ETOF {etof_number}"
        
        # Convert actual value to string for comparison
        if actual_value is None or (isinstance(actual_value, float) and pd.isna(actual_value)):
            actual_str = ''
        else:
            actual_str = str(actual_value).strip()
        
        actual_str_lower = actual_str.lower()
        
        # Check the condition
        if condition_type == 'equals':
            # Check if actual value equals one of the expected values
            matched = any(actual_str.lower() == ev.lower() for ev in expected_values)
            if not matched:
                return False, f"Applies If not met: {column_name} is '{actual_str}', expected one of {expected_values}"
        
        elif condition_type == 'does_not_equal':
            # Check if actual value does NOT equal any of the expected values
            matched = all(actual_str.lower() != ev.lower() for ev in expected_values)
            if not matched:
                return False, f"Applies If not met: {column_name} is '{actual_str}', should not be one of {expected_values}"
        
        elif condition_type == 'starts_with':
            # Check if actual value starts with one of the expected values
            matched = any(actual_str_lower.startswith(ev.lower()) for ev in expected_values)
            if not matched:
                return False, f"Applies If not met: {column_name} is '{actual_str}', should start with one of {expected_values}"
        
        elif condition_type == 'contains':
            # Check if actual value contains one of the expected values
            matched = any(ev.lower() in actual_str_lower for ev in expected_values)
            if not matched:
                return False, f"Applies If not met: {column_name} is '{actual_str}', should contain one of {expected_values}"
        
        elif condition_type == 'does_not_contain':
            # Check if actual value does NOT contain any of the expected values
            matched = all(ev.lower() not in actual_str_lower for ev in expected_values)
            if not matched:
                return False, f"Applies If not met: {column_name} is '{actual_str}', should not contain any of {expected_values}"
    
    # All conditions met
    return True, None


def parse_rounding_rule(rate_by_text):
    """
    Parse rounding rule from Rate By text.
    
    Examples:
    - "Rate by: Weight/chargeable kg (Rounding: Upper To 100)" -> ("upper", 100)
    - "Rate by: Weight/kg (Rounding: Lower To 50)" -> ("lower", 50)
    - "Rate by: Weight/kg" -> (None, None) - no rounding
    
    Returns:
        tuple: (direction, value) where direction is "upper" or "lower", value is the rounding increment
               Returns (None, None) if no rounding rule found
    """
    if not rate_by_text:
        return None, None
    
    rate_by_str = str(rate_by_text).lower()
    
    # Look for "upper to X" or "lower to X" patterns
    import re
    
    # Try to find "upper to X" pattern
    upper_match = re.search(r'upper\s*to\s*(\d+)', rate_by_str)
    if upper_match:
        return "upper", int(upper_match.group(1))
    
    # Try to find "lower to X" pattern
    lower_match = re.search(r'lower\s*to\s*(\d+)', rate_by_str)
    if lower_match:
        return "lower", int(lower_match.group(1))
    
    return None, None


def apply_rounding_to_weight(weight_value, rounding_direction, rounding_value, debug=False):
    """
    Apply rounding rule to a weight value.
    
    Args:
        weight_value: The original weight (float or int)
        rounding_direction: "upper" (round up) or "lower" (round down)
        rounding_value: The rounding increment (e.g., 100 means round to nearest 100)
        debug: If True, print debug info
    
    Examples:
        - apply_rounding_to_weight(150, "upper", 100) -> 200
        - apply_rounding_to_weight(201, "upper", 100) -> 300
        - apply_rounding_to_weight(150, "lower", 100) -> 100
        - apply_rounding_to_weight(201, "lower", 100) -> 200
    
    Returns:
        float: The rounded weight value
    """
    if rounding_direction is None or rounding_value is None or rounding_value <= 0:
        return weight_value
    
    try:
        weight = float(weight_value)
    except (ValueError, TypeError):
        return weight_value
    
    import math
    
    if rounding_direction == "upper":
        # Round up to the nearest rounding_value
        rounded = math.ceil(weight / rounding_value) * rounding_value
    elif rounding_direction == "lower":
        # Round down to the nearest rounding_value
        rounded = math.floor(weight / rounding_value) * rounding_value
    else:
        rounded = weight
    
    if debug:
        print(f"      [DEBUG] Rounding applied: {weight} -> {rounded} ({rounding_direction} to {rounding_value})")
    
    return rounded


def extract_rate_by_column_keyword(rate_by_text):
    """
    Extract the column keyword from a Rate By text for direct column lookup.
    
    Examples:
    - "Rate by: Area/ldm" -> "ldm"
    - "Rate by: Area/cbm" -> "cbm"
    - "Rate by: Quantity/HAWB" -> "hawb"
    - "Area/ldm" -> "ldm"
    
    Returns:
        str: The keyword to look for in column names, or None if not extractable
    """
    if not rate_by_text:
        return None
    
    rate_by_clean = str(rate_by_text).strip()
    
    # Remove "Rate by:" prefix if present
    if 'rate by:' in rate_by_clean.lower():
        match = re.search(r'rate by:\s*([^\r\n]+)', rate_by_clean, re.IGNORECASE)
        if match:
            rate_by_clean = match.group(1).strip()
    
    # Remove trailing rules/notes
    if '\r' in rate_by_clean:
        rate_by_clean = rate_by_clean.split('\r')[0].strip()
    if '\n' in rate_by_clean:
        rate_by_clean = rate_by_clean.split('\n')[0].strip()
    
    # Try to extract the part after "/" (e.g., "Area/ldm" -> "ldm")
    if '/' in rate_by_clean:
        parts = rate_by_clean.split('/')
        if len(parts) >= 2:
            keyword = parts[-1].strip().lower()
            # Filter out common non-column keywords
            if keyword and keyword not in ['kg', 'chargeable', 'weight']:
                return keyword
    
    return None


def find_value_in_etof_columns(rate_by_text, etof_row_data, debug=False):
    """
    Look for a Rate By value directly in the ETOF row columns.
    
    This handles cases like:
    - "Area/ldm" -> look for LDM column
    - "Area/cbm" -> look for CBM column
    - "Quantity/HAWB" -> look for HAWB column
    
    Args:
        rate_by_text: The Rate By text (e.g., "Rate by: Area/ldm")
        etof_row_data: Dict of column -> value for this ETOF row
        debug: If True, print debug information
    
    Returns:
        Tuple: (column_name, value, found) where found is True if column was found
    """
    if not etof_row_data:
        return None, None, False
    
    # Extract keyword from rate_by_text
    keyword = extract_rate_by_column_keyword(rate_by_text)
    
    if not keyword:
        if debug:
            print(f"      [DEBUG] Could not extract keyword from Rate By: {rate_by_text}")
        return None, None, False
    
    if debug:
        print(f"      [DEBUG] Looking for column matching keyword: '{keyword}'")
    
    # Common mappings for known Rate By types to column names
    keyword_mappings = {
        'ldm': ['LDM', 'ldm', 'LOADING_METERS', 'loading_meters'],
        'cbm': ['CBM', 'cbm', 'CUBIC_METERS', 'cubic_meters', 'VOLUME'],
        'cdm': ['CBM', 'cbm', 'CDM', 'cdm'],  # cdm might be typo for cbm
        'hawb': ['HAWB', 'hawb', 'HOUSE_AWB'],
        'mawb': ['MAWB', 'mawb', 'MASTER_AWB'],
        'pieces': ['PIECES', 'pieces', 'PCS', 'pcs', 'QUANTITY'],
        'pallets': ['PALLETS', 'pallets', 'PALLET_COUNT'],
    }
    
    # Get possible column names for this keyword
    possible_columns = keyword_mappings.get(keyword.lower(), [keyword, keyword.upper(), keyword.lower()])
    
    # Search for matching column
    for col_option in possible_columns:
        for col_name, col_value in etof_row_data.items():
            col_name_lower = str(col_name).lower().replace(' ', '_').replace('-', '_')
            col_option_lower = col_option.lower().replace(' ', '_').replace('-', '_')
            
            # Match if column name contains the keyword
            if (col_name_lower == col_option_lower or 
                col_option_lower in col_name_lower or
                col_name_lower.endswith('_' + col_option_lower) or
                col_name_lower.startswith(col_option_lower + '_')):
                
                if col_value is not None and not (isinstance(col_value, float) and pd.isna(col_value)):
                    if debug:
                        print(f"      [DEBUG] Found column '{col_name}' = {col_value} for keyword '{keyword}'")
                    return col_name, col_value, True
    
    if debug:
        print(f"      [DEBUG] No column found for keyword '{keyword}'")
    
    return keyword, None, False


def extract_measurement_value(rate_by_text, measurement_str, units_measurement_str, debug=False):
    """
    Extract the measurement value for a specific Rate By condition.
    
    MEASUREMENT column format: "Quantity/MAWB;Condition/Delivery Zone 3;Condition/ExpressDelivery;..."
    UNITS_MEASUREMENT column format: "1;1;1;..."
    
    These are semicolon-separated and correspond 1:1.
    
    Args:
        rate_by_text: The Rate By text (e.g., "Rate by: Condition/ExpressDelivery" or "Condition/ExpressDelivery")
        measurement_str: The MEASUREMENT column value
        units_measurement_str: The UNITS_MEASUREMENT column value
        debug: If True, print debug information
    
    Returns:
        Tuple: (measurement_name, units_value, found) where found is True if measurement was found
    """
    if not measurement_str or not units_measurement_str:
        return None, None, False
    
    # Clean up the rate_by_text to extract the measurement type
    # It could be "Rate by: Condition/ExpressDelivery" or just "Condition/ExpressDelivery"
    rate_by_clean = str(rate_by_text).strip()
    if 'rate by:' in rate_by_clean.lower():
        # Extract what comes after "Rate by:"
        import re
        match = re.search(r'rate by:\s*([^\r\n]+)', rate_by_clean, re.IGNORECASE)
        if match:
            rate_by_clean = match.group(1).strip()
    
    # Remove any trailing rules like "Regular rule"
    if '\r' in rate_by_clean:
        rate_by_clean = rate_by_clean.split('\r')[0].strip()
    if '\n' in rate_by_clean:
        rate_by_clean = rate_by_clean.split('\n')[0].strip()
    
    rate_by_lower = rate_by_clean.lower()
    
    if debug:
        print(f"      [DEBUG] Looking for measurement: '{rate_by_clean}'")
        print(f"      [DEBUG] MEASUREMENT: {str(measurement_str)[:80]}...")
        print(f"      [DEBUG] UNITS_MEASUREMENT: {str(units_measurement_str)[:80]}...")
    
    # Parse the measurement and units strings
    measurements = str(measurement_str).split(';')
    units = str(units_measurement_str).split(';')
    
    if debug:
        print(f"      [DEBUG] Found {len(measurements)} measurements and {len(units)} units")
    
    # Try to find the matching measurement
    for i, meas in enumerate(measurements):
        meas_clean = meas.strip()
        meas_lower = meas_clean.lower()
        
        # Try different matching strategies:
        # 1. Exact match
        # 2. Rate By contains measurement name
        # 3. Measurement contains Rate By
        if (meas_lower == rate_by_lower or 
            rate_by_lower in meas_lower or 
            meas_lower in rate_by_lower):
            
            if i < len(units):
                units_value = units[i].strip()
                if debug:
                    print(f"      [DEBUG] Found match: '{meas_clean}' = {units_value}")
                return meas_clean, units_value, True
    
    if debug:
        print(f"      [DEBUG] Measurement '{rate_by_clean}' not found in MEASUREMENT column")
    
    return rate_by_clean, None, False


def parse_weight_range_from_column(col_name):
    """
    Parse weight range from a column name like "Price Flat <=200" or "Price Flat >200 <=500".
    
    Returns:
        tuple: (lower_bound, upper_bound) where:
        - lower_bound is None or a number (exclusive lower bound, i.e., > this value)
        - upper_bound is a number (inclusive upper bound, i.e., <= this value)
        Returns (None, None) if not a weight range column.
    """
    if not col_name:
        return None, None
    
    col_str = str(col_name).lower()
    
    # Check if it contains weight range indicators
    if '<=' not in col_str and '<' not in col_str:
        return None, None
    
    # Try to extract: ">X <=Y" or just "<=Y"
    # Pattern: optional ">X" followed by "<=Y" or "<Y"
    import re
    
    lower_bound = None
    upper_bound = None
    
    # Match ">X" part (lower bound, exclusive)
    lower_match = re.search(r'>(\d+(?:\.\d+)?)', col_str)
    if lower_match:
        lower_bound = float(lower_match.group(1))
    
    # Match "<=Y" or "<Y" part (upper bound)
    upper_match = re.search(r'<=?\s*(\d+(?:\.\d+)?)', col_str)
    if upper_match:
        upper_bound = float(upper_match.group(1))
    
    if upper_bound is not None:
        return lower_bound, upper_bound
    
    return None, None


def find_weight_tiered_price_columns(columns_list, cost_col_idx, price_type="flat"):
    """
    Find all weight-tiered price columns for a cost type.
    
    Args:
        columns_list: List of column names
        cost_col_idx: Index of the cost type column
        price_type: "flat" or "per_unit"
    
    Returns:
        List of tuples: [(col_idx, lower_bound, upper_bound), ...]
        where bounds define the weight range for each column.
        Returns empty list if no weight-tiered columns found.
    """
    tiered_columns = []
    
    # Look at columns after the cost column
    for i in range(cost_col_idx + 1, min(cost_col_idx + 10, len(columns_list))):
        col_name = str(columns_list[i]).lower() if columns_list[i] else ''
        
        # Check if this column matches the price type
        if price_type == "flat" and 'flat' not in col_name:
            continue
        if price_type == "per_unit" and 'per unit' not in col_name:
            continue
        
        # Check if it has a weight range
        lower, upper = parse_weight_range_from_column(columns_list[i])
        if upper is not None:
            tiered_columns.append((i, lower, upper))
        elif price_type == "flat" and 'flat' in col_name and 'min' not in col_name and 'max' not in col_name:
            # Regular flat column without weight range - stop looking for tiered columns
            # (this means the cost doesn't have weight tiers)
            break
        elif price_type == "per_unit" and 'per unit' in col_name:
            break
    
    return tiered_columns


def select_price_column_by_weight(tiered_columns, charge_weight, debug=False):
    """
    Select the correct price column based on charge weight.
    
    Args:
        tiered_columns: List of (col_idx, lower_bound, upper_bound) from find_weight_tiered_price_columns
        charge_weight: The shipment's charge weight
        debug: If True, print debug info
    
    Returns:
        tuple: (col_idx, range_description) or (None, None) if no matching range
    """
    if not tiered_columns or charge_weight is None:
        return None, None
    
    try:
        weight = float(charge_weight)
    except (ValueError, TypeError):
        return None, None
    
    # Sort by upper bound to ensure correct ordering
    sorted_tiers = sorted(tiered_columns, key=lambda x: x[2])
    
    for col_idx, lower, upper in sorted_tiers:
        # Check if weight falls in this range
        # lower is exclusive (> lower), upper is inclusive (<= upper)
        if lower is None:
            # First tier: weight <= upper
            if weight <= upper:
                range_desc = f"<={int(upper)}" if upper == int(upper) else f"<={upper}"
                if debug:
                    print(f"      [DEBUG] Weight {weight} matches range {range_desc}")
                return col_idx, range_desc
        else:
            # Subsequent tiers: lower < weight <= upper
            if lower < weight <= upper:
                lower_str = int(lower) if lower == int(lower) else lower
                upper_str = int(upper) if upper == int(upper) else upper
                range_desc = f">{lower_str} <={upper_str}"
                if debug:
                    print(f"      [DEBUG] Weight {weight} matches range {range_desc}")
                return col_idx, range_desc
    
    # Weight exceeds all tiers - check if it's above the highest tier
    if sorted_tiers:
        _, _, max_upper = sorted_tiers[-1]
        if weight > max_upper:
            if debug:
                print(f"      [DEBUG] Weight {weight} exceeds max tier {max_upper}")
            return None, f"exceeds max tier {max_upper}"
    
    return None, None


def extract_rate_lane(comment):
    """
    Extract rate lane number from comment like "Rate lane: 2464" or "Rate lanes: 2464, 2465".
    
    Args:
        comment: String containing rate lane info
    
    Returns:
        List of lane numbers (as strings), or empty list if not found
    """
    if not comment or pd.isna(comment):
        return []
    
    comment_str = str(comment)
    
    # Try to match "Rate lane: XXXX" or "Rate lanes: XXXX, YYYY"
    match = re.search(r'Rate\s+lanes?:\s*([\d,\s]+)', comment_str, re.IGNORECASE)
    if match:
        lanes_str = match.group(1)
        # Split by comma and clean
        lanes = [l.strip() for l in lanes_str.split(',') if l.strip()]
        return lanes
    
    return []


def find_weight_bracket_match(df_rate_data, lane_number, cost_type, precalc_cost, carrier_cost, debug=False):
    """
    Find weight bracket that matches the Pre-calc. cost value.
    
    Args:
        df_rate_data: DataFrame with rate data
        lane_number: Lane number to look up
        cost_type: Cost type name (e.g., "Transport cost")
        precalc_cost: Pre-calc. cost value from shipment
        carrier_cost: Carrier's cost value from shipment
        debug: If True, print debug info
    
    Returns:
        tuple: (matched_bracket, matched_value, per_unit_price, calculated_units)
               or None if no match found
    """
    if df_rate_data is None or df_rate_data.empty:
        return None
    
    if precalc_cost is None:
        if debug:
            print(f"      [DEBUG] find_weight_bracket_match: precalc_cost is None")
        return None
    
    if debug:
        print(f"      [DEBUG] find_weight_bracket_match: looking for bracket matching {precalc_cost}")
    
    # Find lane column
    lane_col = None
    for col in df_rate_data.columns:
        if 'lane' in col.lower():
            lane_col = col
            break
    
    if lane_col is None:
        return None
    
    # Find row for this lane
    target_row = None
    for idx, row in df_rate_data.iterrows():
        try:
            lane_val = row.get(lane_col)
            if lane_val is not None and str(int(float(lane_val))) == str(lane_number):
                target_row = row
                break
        except:
            continue
    
    if target_row is None:
        if debug:
            print(f"      [DEBUG] find_weight_bracket_match: lane {lane_number} not found")
        return None
    
    # Find cost column
    cost_col_idx = None
    cost_type_lower = cost_type.lower().strip()
    base_cost_type = re.sub(r'\s*\([^)]*\)\s*$', '', cost_type_lower).strip()
    
    for idx, col in enumerate(df_rate_data.columns):
        col_lower = col.lower().strip()
        if col_lower == cost_type_lower or col_lower == base_cost_type:
            cost_col_idx = idx
            break
    
    if cost_col_idx is None:
        if debug:
            print(f"      [DEBUG] find_weight_bracket_match: cost column '{cost_type}' not found")
        return None
    
    # Get all columns after the cost column (these are the weight brackets)
    columns = list(df_rate_data.columns)
    bracket_columns = []
    per_unit_col = None
    per_unit_price = None
    
    for i in range(cost_col_idx + 1, len(columns)):
        col = columns[i]
        col_lower = col.lower()
        
        # Stop if we hit another cost name column (not a bracket)
        if 'cost' in col_lower and ('flat' not in col_lower and 'per' not in col_lower and '>' not in col and '<' not in col):
            break
        
        # Check if this is a weight bracket column (Flat <=100, Flat >100, etc.)
        if 'flat' in col_lower or '<=' in col or '>' in col:
            val = target_row.get(col)
            if val is not None and pd.notna(val):
                try:
                    bracket_val = float(val)
                    bracket_columns.append((col, bracket_val))
                    if debug:
                        print(f"      [DEBUG] Bracket column: '{col}' = {bracket_val}")
                except:
                    pass
        
        # Check for per unit column (e.g., "per unit >1000", "Price per unit")
        if 'per' in col_lower and 'unit' in col_lower:
            val = target_row.get(col)
            if val is not None and pd.notna(val):
                try:
                    per_unit_price = float(val)
                    per_unit_col = col
                    if debug:
                        print(f"      [DEBUG] Per unit column: '{col}' = {per_unit_price}")
                except:
                    pass
    
    if not bracket_columns:
        if debug:
            print(f"      [DEBUG] find_weight_bracket_match: no bracket columns found")
        return None
    
    # Find the bracket that matches precalc_cost (with tolerance)
    matched_bracket = None
    matched_value = None
    tolerance = 0.01  # Allow small rounding differences
    
    for bracket_name, bracket_val in bracket_columns:
        if abs(bracket_val - precalc_cost) <= tolerance:
            matched_bracket = bracket_name
            matched_value = bracket_val
            if debug:
                print(f"      [DEBUG] MATCH! Bracket '{bracket_name}' = {bracket_val} matches precalc_cost {precalc_cost}")
            break
    
    if matched_bracket is None:
        # Try with larger tolerance
        for bracket_name, bracket_val in bracket_columns:
            if abs(bracket_val - precalc_cost) <= 1.0:  # Within 1 unit
                matched_bracket = bracket_name
                matched_value = bracket_val
                if debug:
                    print(f"      [DEBUG] APPROXIMATE MATCH! Bracket '{bracket_name}' = {bracket_val} ≈ precalc_cost {precalc_cost}")
                break
    
    # Calculate units from carrier_cost if we have per_unit_price
    calculated_units = None
    if carrier_cost is not None and per_unit_price is not None and per_unit_price > 0:
        calculated_units = carrier_cost / per_unit_price
        if debug:
            print(f"      [DEBUG] Calculated units: {carrier_cost} / {per_unit_price} = {calculated_units:.0f}")
    
    if matched_bracket:
        # Clean up bracket name for display (remove "Flat " prefix if present)
        display_bracket = matched_bracket
        if display_bracket.lower().startswith('flat '):
            display_bracket = display_bracket[5:]
        
        return (display_bracket, matched_value, per_unit_price, calculated_units)
    
    return None


def find_cost_price_in_rate_data(df_rate_data, lane_number, cost_type, price_type="flat", debug=False, return_reason=False, charge_weight=None):
    """
    Find the Price value for a given lane and cost type.
    
    Logic:
    1. Find the row where Lane # = lane_number
    2. Find the column named exactly like cost_type (e.g., "Air DGR Fee")
    3. Find the appropriate price column based on price_type:
       - "flat": Look for "Price Flat" (next column after cost)
                 OR weight-tiered columns like "Price Flat <=200", "Price Flat >200 <=500"
       - "min": Look for "Price Flat MIN" column
       - "max": Look for "Price Flat MAX" column
       - "per_unit": Look for "Price per unit" column
                     OR weight-tiered columns like "Price per unit <=200"
    4. If weight-tiered columns exist, use charge_weight to select the correct column
    5. Return the value from that single cell
    
    Args:
        df_rate_data: DataFrame from rate_costs.py (Rate Data sheet)
        lane_number: Lane # to search for
        cost_type: Cost type name (e.g., "Air DGR Fee")
        price_type: "flat", "min", "max", or "per_unit"
        debug: If True, print debug information
        return_reason: If True, returns (price, col_name, reason) instead of (price, col_name)
        charge_weight: Optional weight value for selecting weight-tiered price columns
    
    Returns:
        Tuple: (price_value, column_name) or (None, None) if not found
        If return_reason=True: (price_value, column_name, reason_string)
    """
    if debug:
        print(f"      [DEBUG] Looking for Lane #{lane_number}, Cost: '{cost_type}'")
    
    # Find Lane # column (should be first column)
    lane_col = df_rate_data.columns[0]
    if debug:
        print(f"      [DEBUG] Lane column: '{lane_col}'")
    
    # Find the row index where Lane # matches
    lane_mask = df_rate_data[lane_col].astype(str).str.strip() == str(lane_number).strip()
    matching_rows = df_rate_data[lane_mask]
    
    if matching_rows.empty:
        if debug:
            print(f"      [DEBUG] No row found for Lane #{lane_number}")
        reason = f"Lane #{lane_number} not found in rate data"
        return (None, None, reason) if return_reason else (None, None)
    
    # Get the row index (use first match if multiple)
    row_idx = matching_rows.index[0]
    if debug:
        print(f"      [DEBUG] Found row at index {row_idx} for Lane #{lane_number}")
    
    # Find the column that matches cost_type
    # Try multiple matching strategies:
    # 1. Exact match (case-insensitive)
    # 2. Rate card column starts with cost_type (e.g., "DGR Fee" matches "DGR Fee (Hazardous Surcharge)")
    # 3. Cost type starts with rate card column
    # 4. Rate card column contains cost_type
    cost_col_idx = None
    columns_list = list(df_rate_data.columns)
    cost_type_lower = cost_type.strip().lower()
    
    # Strategy 1: Exact match
    for i, col in enumerate(columns_list):
        if col and str(col).strip().lower() == cost_type_lower:
            cost_col_idx = i
            if debug:
                print(f"      [DEBUG] Found exact match for cost '{cost_type}'")
            break
    
    # Strategy 2: Rate card column starts with cost_type (e.g., "DGR Fee" matches "DGR Fee (Hazardous Surcharge)")
    if cost_col_idx is None:
        for i, col in enumerate(columns_list):
            if col and str(col).strip().lower().startswith(cost_type_lower):
                cost_col_idx = i
                if debug:
                    print(f"      [DEBUG] Found partial match: '{col}' starts with '{cost_type}'")
                break
    
    # Strategy 3: Cost type starts with rate card column (reverse of strategy 2)
    if cost_col_idx is None:
        for i, col in enumerate(columns_list):
            if col and cost_type_lower.startswith(str(col).strip().lower()):
                cost_col_idx = i
                if debug:
                    print(f"      [DEBUG] Found partial match: '{cost_type}' starts with '{col}'")
                break
    
    # Strategy 4: Base names match (strip parentheses from both and compare)
    if cost_col_idx is None:
        base_cost_type = re.sub(r'\s*\([^)]*\)\s*$', '', cost_type_lower).strip()
        for i, col in enumerate(columns_list):
            if col:
                col_lower = str(col).strip().lower()
                base_col = re.sub(r'\s*\([^)]*\)\s*$', '', col_lower).strip()
                if base_col == base_cost_type:
                    cost_col_idx = i
                    if debug:
                        print(f"      [DEBUG] Found match via base name: '{col}' base = '{base_cost_type}'")
                    break
    
    if cost_col_idx is None:
        if debug:
            # Show available columns that might be similar
            similar = [c for c in columns_list if c and cost_type.lower()[:5] in str(c).lower()]
            print(f"      [DEBUG] Cost column '{cost_type}' not found")
            print(f"      [DEBUG] Similar columns: {similar[:5]}")
        reason = f"Cost type '{cost_type}' not found in rate card columns"
        return (None, None, reason) if return_reason else (None, None)
    
    if debug:
        print(f"      [DEBUG] Found cost column '{columns_list[cost_col_idx]}' at index {cost_col_idx}")
    
    # Find the appropriate price column based on price_type
    price_col_idx = None
    price_col_name = None
    weight_tier_info = None
    
    if price_type == "per_unit":
        # First check for weight-tiered "per unit" columns
        tiered_columns = find_weight_tiered_price_columns(columns_list, cost_col_idx, price_type="per_unit")
        
        if tiered_columns and charge_weight is not None:
            if debug:
                print(f"      [DEBUG] Found {len(tiered_columns)} weight-tiered 'per unit' columns")
            selected_col_idx, range_desc = select_price_column_by_weight(tiered_columns, charge_weight, debug=debug)
            
            if selected_col_idx is not None:
                price_col_idx = selected_col_idx
                price_col_name = columns_list[selected_col_idx]
                weight_tier_info = range_desc
                if debug:
                    print(f"      [DEBUG] Selected weight-tiered column: '{price_col_name}' for weight {charge_weight}")
            elif range_desc and 'exceeds' in str(range_desc):
                reason = f"CHARGE_WEIGHT {charge_weight} {range_desc} for cost '{cost_type}'"
                return (None, None, reason) if return_reason else (None, None)
        
        # If no tiered column found/selected, look for regular "Price per unit" column
        if price_col_idx is None:
            for i in range(cost_col_idx + 1, min(cost_col_idx + 5, len(columns_list))):
                col_name = str(columns_list[i]).lower()
                # Skip weight-tiered columns if we're looking for regular one
                if ('per unit' in col_name or 'price per' in col_name) and '<' not in col_name and '>' not in col_name:
                    price_col_idx = i
                    price_col_name = columns_list[i]
                    break
        
        # SPECIAL CASE: If still no per_unit column found, check for weight-tiered FLAT columns
        # This handles cases where Rate By = Weight but only flat tiered prices exist
        if price_col_idx is None and charge_weight is not None:
            tiered_flat_columns = find_weight_tiered_price_columns(columns_list, cost_col_idx, price_type="flat")
            if tiered_flat_columns:
                if debug:
                    print(f"      [DEBUG] No 'per unit' column, but found {len(tiered_flat_columns)} weight-tiered FLAT columns - using fallback")
                selected_col_idx, range_desc = select_price_column_by_weight(tiered_flat_columns, charge_weight, debug=debug)
                
                if selected_col_idx is not None:
                    price_col_idx = selected_col_idx
                    price_col_name = columns_list[selected_col_idx]
                    weight_tier_info = f"FLAT_TIER:{range_desc}"  # Mark as flat tier for caller
                    if debug:
                        print(f"      [DEBUG] Selected weight-tiered FLAT column as fallback: '{price_col_name}' for weight {charge_weight}")
                elif range_desc and 'exceeds' in str(range_desc):
                    reason = f"CHARGE_WEIGHT {charge_weight} {range_desc} for cost '{cost_type}'"
                    return (None, None, reason) if return_reason else (None, None)
        
        if price_col_idx is None:
            if debug:
                print(f"      [DEBUG] 'Price per unit' column not found after cost column")
            reason = f"'Price per unit' column not found for cost '{cost_type}'"
            return (None, None, reason) if return_reason else (None, None)
    
    elif price_type == "min":
        # Look for "Price Flat MIN" or "MIN" column after the cost column
        for i in range(cost_col_idx + 1, min(cost_col_idx + 5, len(columns_list))):
            col_name = str(columns_list[i]).lower()
            if 'min' in col_name or 'flat min' in col_name:
                price_col_idx = i
                price_col_name = columns_list[i]
                break
        
        if price_col_idx is None:
            if debug:
                print(f"      [DEBUG] 'Price Flat MIN' column not found after cost column")
            # MIN column not found is OK - just return None without error reason
            return (None, None, None) if return_reason else (None, None)
    
    elif price_type == "max":
        # Look for "Price Flat MAX" or "MAX" column after the cost column
        for i in range(cost_col_idx + 1, min(cost_col_idx + 6, len(columns_list))):
            col_name = str(columns_list[i]).lower()
            if 'max' in col_name or 'flat max' in col_name:
                price_col_idx = i
                price_col_name = columns_list[i]
                break
        
        if price_col_idx is None:
            if debug:
                print(f"      [DEBUG] 'Price Flat MAX' column not found after cost column")
            # MAX column not found is OK - just return None without error reason
            return (None, None, None) if return_reason else (None, None)
    
    else:
        # Default: "flat" - Look for "Price Flat" column or weight-tiered flat columns
        
        # First check for weight-tiered columns
        tiered_columns = find_weight_tiered_price_columns(columns_list, cost_col_idx, price_type="flat")
        
        if tiered_columns and charge_weight is not None:
            if debug:
                print(f"      [DEBUG] Found {len(tiered_columns)} weight-tiered 'flat' columns")
            selected_col_idx, range_desc = select_price_column_by_weight(tiered_columns, charge_weight, debug=debug)
            
            if selected_col_idx is not None:
                price_col_idx = selected_col_idx
                price_col_name = columns_list[selected_col_idx]
                weight_tier_info = range_desc
                if debug:
                    print(f"      [DEBUG] Selected weight-tiered column: '{price_col_name}' for weight {charge_weight}")
            elif range_desc and 'exceeds' in str(range_desc):
                reason = f"CHARGE_WEIGHT {charge_weight} {range_desc} for cost '{cost_type}'"
                return (None, None, reason) if return_reason else (None, None)
        
        # If no tiered column found/selected, use the regular "Price Flat" column
        if price_col_idx is None:
            price_col_idx = cost_col_idx + 1
            
            if price_col_idx >= len(columns_list):
                if debug:
                    print(f"      [DEBUG] No column after cost column")
                reason = f"No price column found after cost '{cost_type}'"
                return (None, None, reason) if return_reason else (None, None)
    
            price_col_name = columns_list[price_col_idx]
    
    if debug:
        print(f"      [DEBUG] Price column ({price_type}): '{price_col_name}' at index {price_col_idx}")
    
    # Get the single cell value using iloc
    price_value = df_rate_data.iloc[row_idx, price_col_idx]
    
    if debug:
        print(f"      [DEBUG] Price value from cell [{row_idx}, {price_col_idx}]: {price_value}")
    
    # Check if it's empty/null
    if price_value is None:
        if debug:
            print(f"      [DEBUG] Price is None")
        reason = f"Price value is empty for cost '{cost_type}' in lane {lane_number}"
        return (None, price_col_name, reason) if return_reason else (None, price_col_name)
    
    # Handle pandas NA/NaN
    try:
        if pd.isna(price_value):
            if debug:
                print(f"      [DEBUG] Price is NaN")
            reason = f"Price value is empty for cost '{cost_type}' in lane {lane_number}"
            return (None, price_col_name, reason) if return_reason else (None, price_col_name)
    except (ValueError, TypeError):
        pass
    
    # Check if empty string
    if str(price_value).strip() == '':
        if debug:
            print(f"      [DEBUG] Price is empty string")
        reason = f"Price value is empty for cost '{cost_type}' in lane {lane_number}"
        return (None, price_col_name, reason) if return_reason else (None, price_col_name)
    
    if debug:
        print(f"      [DEBUG] Returning price: {price_value}")
    
    return (price_value, price_col_name, None) if return_reason else (price_value, price_col_name)


def check_conditions_and_add_reason(df_mismatch, df_lc_etof_mapping, all_rate_costs, all_accessorial_costs=None, debug=False, debug_first_n=5):
    """
    Check conditions for each mismatch row and add a Reason column.
    
    Looks up Rate By and Applies If from the cost conditions based on cost type.
    If cost is not found in rate_costs, falls back to accessorial costs.
    
    Args:
        df_mismatch: DataFrame from mismacthes_filing.py
        df_lc_etof_mapping: DataFrame from lc_etof_with_comments.xlsx
        all_rate_costs: dict {agreement_number: {'rate_data': DataFrame, 'cost_conditions': DataFrame}}
        all_accessorial_costs: dict {agreement_number: DataFrame} from accessorial costs files (optional fallback)
        debug: If True, print debug information for first N rows
        debug_first_n: Number of rows to debug (default 5)
    
    Returns:
        DataFrame with added "Rate By", "Applies If", and "Reason" columns
    """
    if all_accessorial_costs is None:
        all_accessorial_costs = {}
    df = df_mismatch.copy()
    
    # Find relevant columns
    etof_col_mismatch = None
    for col in df.columns:
        if 'etof' in col.lower() and ('number' in col.lower() or '#' in col.lower()):
            etof_col_mismatch = col
            break
    
    cost_type_col = None
    for col in df.columns:
        if 'cost' in col.lower() and 'type' in col.lower():
            cost_type_col = col
            break
    
    # Find Carrier Agreement # column in mismatch
    agreement_col_mismatch = None
    for col in df.columns:
        if 'carrier' in col.lower() and 'agreement' in col.lower():
            agreement_col_mismatch = col
            break
    
    # Find Comment column in mismatch (if exists, use it as Reason)
    comment_col_mismatch = None
    for col in df.columns:
        if col.lower() == 'comment':
            comment_col_mismatch = col
            break
    
    # Find ETOF # column in lc_etof_mapping
    etof_col_mapping = None
    for col in df_lc_etof_mapping.columns:
        if 'etof' in col.lower() and '#' in col.lower():
            etof_col_mapping = col
            break
    
    # Find comment column in lc_etof_mapping
    comment_col_mapping = None
    for col in df_lc_etof_mapping.columns:
        if 'comment' in col.lower():
            comment_col_mapping = col
            break
    
    print(f"   Mismatch ETOF column: {etof_col_mismatch}")
    print(f"   Cost type column: {cost_type_col}")
    print(f"   Carrier Agreement column: {agreement_col_mismatch}")
    print(f"   Mismatch Comment column: {comment_col_mismatch}")
    print(f"   Mapping ETOF column: {etof_col_mapping}")
    print(f"   Mapping comment column: {comment_col_mapping}")
    print(f"   Rate costs loaded for agreements: {list(all_rate_costs.keys())}")
    print(f"   Accessorial costs loaded for agreements: {list(all_accessorial_costs.keys())}")
    
    # Find CHARGE_WEIGHT column in lc_etof_mapping
    charge_weight_col = None
    for col in df_lc_etof_mapping.columns:
        if 'charge' in col.lower() and 'weight' in col.lower():
            charge_weight_col = col
            break
    
    print(f"   CHARGE_WEIGHT column: {charge_weight_col}")
    
    # Create ETOF -> comment mapping
    etof_to_comment = {}
    if etof_col_mapping and comment_col_mapping:
        for _, row in df_lc_etof_mapping.iterrows():
            etof_num = row.get(etof_col_mapping)
            comment = row.get(comment_col_mapping)
            if pd.notna(etof_num):
                etof_to_comment[str(etof_num).strip()] = comment
    
    print(f"   Created ETOF -> comment mapping: {len(etof_to_comment)} entries")
    
    # Find MEASUREMENT and UNITS_MEASUREMENT columns in lc_etof_mapping
    measurement_col = None
    units_measurement_col = None
    for col in df_lc_etof_mapping.columns:
        col_lower = col.lower()
        if col_lower == 'measurement' or col_lower == 'measurements':
            measurement_col = col
        elif 'units' in col_lower and 'measurement' in col_lower:
            units_measurement_col = col
    
    print(f"   MEASUREMENT column: {measurement_col}")
    print(f"   UNITS_MEASUREMENT column: {units_measurement_col}")
    
    # Create ETOF -> CHARGE_WEIGHT mapping
    etof_to_charge_weight = {}
    if etof_col_mapping and charge_weight_col:
        for _, row in df_lc_etof_mapping.iterrows():
            etof_num = row.get(etof_col_mapping)
            charge_weight = row.get(charge_weight_col)
            if pd.notna(etof_num):
                etof_to_charge_weight[str(etof_num).strip()] = charge_weight
    
    print(f"   Created ETOF -> CHARGE_WEIGHT mapping: {len(etof_to_charge_weight)} entries")
    
    # Create ETOF -> MEASUREMENT and ETOF -> UNITS_MEASUREMENT mappings
    etof_to_measurement = {}
    etof_to_units_measurement = {}
    if etof_col_mapping and measurement_col and units_measurement_col:
        for _, row in df_lc_etof_mapping.iterrows():
            etof_num = row.get(etof_col_mapping)
            measurement = row.get(measurement_col)
            units_measurement = row.get(units_measurement_col)
            if pd.notna(etof_num):
                etof_key = str(etof_num).strip()
                etof_to_measurement[etof_key] = measurement if pd.notna(measurement) else ''
                etof_to_units_measurement[etof_key] = units_measurement if pd.notna(units_measurement) else ''
    
    print(f"   Created ETOF -> MEASUREMENT mapping: {len(etof_to_measurement)} entries")
    print(f"   Created ETOF -> UNITS_MEASUREMENT mapping: {len(etof_to_units_measurement)} entries")
    
    # Create ETOF -> full row data mapping (for Applies If condition checking)
    etof_to_row_data = {}
    if etof_col_mapping:
        for _, row in df_lc_etof_mapping.iterrows():
            etof_num = row.get(etof_col_mapping)
            if pd.notna(etof_num):
                etof_key = str(etof_num).strip()
                etof_to_row_data[etof_key] = row.to_dict()
    
    print(f"   Created ETOF -> row data mapping: {len(etof_to_row_data)} entries")
    
    if debug:
        print(f"\n   [DEBUG] Sample ETOF -> comment entries:")
        for i, (k, v) in enumerate(list(etof_to_comment.items())[:3]):
            print(f"      {k}: {str(v)[:60]}...")
    
    # Process each row - collect Rate By, Applies If, and Reason
    rate_by_values = []
    applies_if_values = []
    reasons = []
    debug_count = 0
    
    for idx, row in df.iterrows():
        cost_type = str(row.get(cost_type_col, '')).strip() if pd.notna(row.get(cost_type_col)) else ''
        etof_number = str(row.get(etof_col_mismatch, '')).strip() if pd.notna(row.get(etof_col_mismatch)) else ''
        agreement = str(row.get(agreement_col_mismatch, '')).strip() if agreement_col_mismatch and pd.notna(row.get(agreement_col_mismatch)) else ''
        
        # Check if there's an existing Comment value - if so, use it as Reason
        existing_comment = ''
        if comment_col_mismatch:
            existing_comment = str(row.get(comment_col_mismatch, '')).strip() if pd.notna(row.get(comment_col_mismatch)) else ''
        
        # Debug first N rows
        row_debug = debug and debug_count < debug_first_n
        
        reason = ''
        rate_by = ''
        applies_if = ''
        
        # If there's an existing comment, use it as reason and skip further analysis
        if existing_comment:
            if row_debug:
                print(f"\n   [DEBUG] === Row {idx} ===")
                print(f"   [DEBUG] Cost type: {cost_type}")
                print(f"   [DEBUG] ETOF_NUMBER: {etof_number}")
                print(f"   [DEBUG] Existing Comment found: {existing_comment[:60]}...")
                debug_count += 1
            
            # Still try to get Rate By and Applies If for display
            agreement_data = all_rate_costs.get(agreement)
            if agreement_data is None and agreement:
                for ag_key in all_rate_costs.keys():
                    if ag_key in agreement or agreement in ag_key:
                        agreement_data = all_rate_costs[ag_key]
                        break
            
            if agreement_data:
                df_cost_conditions = agreement_data.get('cost_conditions')
                rate_by_lookup, applies_if_lookup = get_cost_conditions_for_cost_type(
                    cost_type, df_cost_conditions, debug=False
                )
                rate_by = str(rate_by_lookup).strip() if rate_by_lookup and pd.notna(rate_by_lookup) else ''
                applies_if = str(applies_if_lookup).strip() if applies_if_lookup and pd.notna(applies_if_lookup) else ''
            
            rate_by_values.append(rate_by)
            applies_if_values.append(applies_if)
            reasons.append(existing_comment)
            continue
        
        # Get the rate costs data for this agreement
        agreement_data = all_rate_costs.get(agreement)
        if agreement_data is None and agreement:
            # Try to find a matching agreement (partial match)
            for ag_key in all_rate_costs.keys():
                if ag_key in agreement or agreement in ag_key:
                    agreement_data = all_rate_costs[ag_key]
                    break
        
        if agreement_data is None:
            if row_debug:
                print(f"\n   [DEBUG] === Row {idx} ===")
                print(f"   [DEBUG] Cost type: {cost_type}")
                print(f"   [DEBUG] ETOF_NUMBER: {etof_number}")
                print(f"   [DEBUG] Carrier Agreement: {agreement}")
                print(f"   [DEBUG] No rate data found for agreement: {agreement}")
                debug_count += 1
            reason = f"No rate cost data found for agreement: {agreement}"
            rate_by_values.append('')
            applies_if_values.append('')
            reasons.append(reason)
            continue
        
        df_rate_data = agreement_data.get('rate_data')
        df_cost_conditions = agreement_data.get('cost_conditions')
        
        # Get the row data for this ETOF (needed for smart cost matching)
        etof_row_data = etof_to_row_data.get(etof_number, {})
        
        # Look up Rate By and Applies If from cost conditions based on cost type
        # Use find_best_matching_cost to handle multiple cost variations (e.g., "Delivery Fee (Getafe)" vs "Delivery Fee (Sevilla)")
        matched_cost_name, rate_by_lookup, applies_if_lookup = find_best_matching_cost(
            cost_type, df_cost_conditions, etof_row_data, debug=row_debug
        )
        
        rate_by = str(rate_by_lookup).strip() if rate_by_lookup and pd.notna(rate_by_lookup) else ''
        applies_if = str(applies_if_lookup).strip() if applies_if_lookup and pd.notna(applies_if_lookup) else ''
        
        if row_debug:
            print(f"\n   [DEBUG] === Row {idx} ===")
            print(f"   [DEBUG] Cost type: {cost_type}")
            print(f"   [DEBUG] Matched cost: {matched_cost_name}")
            print(f"   [DEBUG] ETOF_NUMBER: {etof_number}")
            print(f"   [DEBUG] Carrier Agreement: {agreement}")
            print(f"   [DEBUG] Rate By (from cost conditions): {rate_by[:50]}..." if len(rate_by) > 50 else f"   [DEBUG] Rate By (from cost conditions): {rate_by}")
            print(f"   [DEBUG] Applies If (from cost conditions): {applies_if[:50]}..." if len(applies_if) > 50 else f"   [DEBUG] Applies If (from cost conditions): {applies_if}")
        
        # If couldn't find cost conditions in rate_costs, try accessorial costs as fallback
        use_accessorial = False
        accessorial_data = None
        accessorial_price_flat = None
        accessorial_price_per_unit = None
        accessorial_has_min_flat = False
        accessorial_is_percentage = False
        accessorial_percentage_value = None
        accessorial_applied_over = None
        
        if not rate_by and not applies_if:
            if row_debug:
                print(f"   [DEBUG] No cost conditions found in rate_costs for cost type: {cost_type}, trying accessorial costs...")
            
            # Try to find in accessorial costs (lazy load on-demand)
            df_accessorial = get_accessorial_data_for_agreement(agreement, all_accessorial_costs, debug=row_debug)
            
            if df_accessorial is not None:
                
                # Get the lane number from comment
                comment = etof_to_comment.get(etof_number)
                lanes = extract_rate_lane(comment) if comment else []
                lane_number = lanes[0] if len(lanes) == 1 else None
                
                # Get ship date for validity check
                ship_date_val = get_ship_date_from_row_data(etof_row_data, debug=row_debug)
                
                if row_debug:
                    print(f"   [DEBUG] Looking for cost '{cost_type}' in accessorial, lane={lane_number}, ship_date={ship_date_val}")
                
                # Look up in accessorial costs
                (acc_cost_name, acc_rate_by, acc_applies_if, 
                 acc_price_flat, acc_price_per_unit, acc_has_min_flat,
                 acc_is_percentage, acc_percentage_value, acc_applied_over) = find_best_matching_accessorial_cost(
                    cost_type, df_accessorial, lane_number, etof_row_data, debug=row_debug, ship_date=ship_date_val
                )
                
                if acc_rate_by or acc_applies_if or acc_price_flat is not None or acc_price_per_unit is not None or acc_is_percentage:
                    # Found in accessorial costs
                    use_accessorial = True
                    matched_cost_name = acc_cost_name
                    rate_by = acc_rate_by if acc_rate_by else ''
                    applies_if = acc_applies_if if acc_applies_if else ''
                    accessorial_price_flat = acc_price_flat
                    accessorial_price_per_unit = acc_price_per_unit
                    accessorial_has_min_flat = acc_has_min_flat
                    accessorial_is_percentage = acc_is_percentage
                    accessorial_percentage_value = acc_percentage_value
                    accessorial_applied_over = acc_applied_over
                    if row_debug:
                        print(f"   [DEBUG] *** USING ACCESSORIAL COSTS ***")
                        print(f"   [DEBUG] Accessorial cost found: {acc_cost_name}")
                        print(f"   [DEBUG] Accessorial Rate By: {acc_rate_by}")
                        print(f"   [DEBUG] Accessorial Applies If: {str(acc_applies_if)[:50]}..." if acc_applies_if and len(str(acc_applies_if)) > 50 else f"   [DEBUG] Accessorial Applies If: {acc_applies_if}")
                        print(f"   [DEBUG] Accessorial Price Flat: {acc_price_flat}")
                        print(f"   [DEBUG] Accessorial Price per unit: {acc_price_per_unit}")
                        print(f"   [DEBUG] Accessorial Has MIN Flat: {acc_has_min_flat}")
                        print(f"   [DEBUG] Accessorial Is Percentage: {acc_is_percentage}")
                        print(f"   [DEBUG] Accessorial Percentage Value: {acc_percentage_value}%")
                        print(f"   [DEBUG] Accessorial Applied Over: {acc_applied_over}")
                else:
                    if row_debug:
                        print(f"   [DEBUG] Cost '{cost_type}' not found in accessorial costs")
            
            if not use_accessorial:
                if row_debug:
                    print(f"   [DEBUG] No cost conditions found for cost type: {cost_type} (checked both rate_costs and accessorial)")
                reason = f"Cost type '{cost_type}' not found in cost conditions"
                rate_by_values.append('')
                applies_if_values.append('')
                reasons.append(reason)
                if row_debug:
                    debug_count += 1
                continue
        
        # Use matched_cost_name for price lookup (it has the full name with parentheses)
        cost_name_for_lookup = matched_cost_name if matched_cost_name else cost_type
        
        # Check Applies If condition
        # "No condition" or empty means no applies if restriction
        applies_if_lower = applies_if.lower()
        is_no_applies_if_condition = (
            not applies_if or 
            'no condition' in applies_if_lower or 
            applies_if_lower == 'nan'
        )
        
        # Check if it's just "Applies if invoiced by Carrier" with no other conditions
        if not is_no_applies_if_condition and 'applies if invoiced' in applies_if_lower:
            # Check if there are actual conditions (equals, starts with, contains)
            has_real_conditions = any(kw in applies_if_lower for kw in ['equals', 'starts with', 'starts', 'contains', 'does not equal'])
            if not has_real_conditions:
                is_no_applies_if_condition = True
        
        # Parse and check Applies If conditions
        applies_if_met = True
        applies_if_reason = None
        
        if not is_no_applies_if_condition:
            # Parse the conditions
            parsed_conditions = parse_applies_if_condition(applies_if, debug=row_debug)
            
            if parsed_conditions:
                # Get the row data for this ETOF
                etof_row_data = etof_to_row_data.get(etof_number, {})
                
                if not etof_row_data:
                    applies_if_met = False
                    applies_if_reason = f"ETOF {etof_number} not found in lc_etof_with_comments - cannot verify Applies If conditions"
                else:
                    # Check if all conditions are met
                    applies_if_met, applies_if_reason = check_applies_if_condition(
                        parsed_conditions, etof_number, etof_row_data, debug=row_debug
                    )
                
                if row_debug:
                    print(f"   [DEBUG] Applies If conditions: {len(parsed_conditions)} parsed")
                    print(f"   [DEBUG] Applies If met: {applies_if_met}")
                    if applies_if_reason:
                        print(f"   [DEBUG] Applies If reason: {applies_if_reason}")
            else:
                # Couldn't parse conditions, treat as "has conditions but cannot evaluate"
                if row_debug:
                    print(f"   [DEBUG] Could not parse Applies If conditions")
                is_no_applies_if_condition = True  # Proceed but note we couldn't parse
        
        # If Applies If conditions are not met, set reason and continue
        if not applies_if_met:
            reason = applies_if_reason if applies_if_reason else f"Applies If condition not met: {applies_if[:100]}"
            rate_by_values.append(rate_by)
            applies_if_values.append(applies_if)
            reasons.append(reason)
            if row_debug:
                print(f"   [DEBUG] Final reason: {reason[:60]}..." if len(reason) > 60 else f"   [DEBUG] Final reason: {reason}")
                debug_count += 1
            continue
        
        if is_no_applies_if_condition or applies_if_met:
            if row_debug:
                print(f"   [DEBUG] Applies If = {'No condition' if is_no_applies_if_condition else 'Conditions met'} -> checking Rate By...")
            
            # Check Rate By condition
            rate_by_lower = rate_by.lower()
            is_per_shipment = 'per shipment' in rate_by_lower or 'shipment' in rate_by_lower
            
            if is_per_shipment:
                if row_debug:
                    print(f"   [DEBUG] Rate By = PER SHIPMENT -> looking up comment...")
                
                # If using accessorial data, use the pre-fetched prices
                if use_accessorial:
                    if row_debug:
                        print(f"   [DEBUG] Processing PER SHIPMENT with ACCESSORIAL data...")
                        print(f"   [DEBUG] Accessorial Price Flat: {accessorial_price_flat}, Price per unit: {accessorial_price_per_unit}")
                        print(f"   [DEBUG] Accessorial Is Percentage: {accessorial_is_percentage}, Percentage: {accessorial_percentage_value}%, Applied Over: {accessorial_applied_over}")
                    
                    # Check if this is a percentage-based cost
                    if accessorial_is_percentage and accessorial_percentage_value is not None and accessorial_applied_over:
                        if row_debug:
                            print(f"   [DEBUG] Processing PERCENTAGE-BASED cost: {accessorial_percentage_value}% over '{accessorial_applied_over}'")
                        
                        # Parse the applied_over to get the base cost names
                        # Format: "Transport cost (National), Transport cost (International)" or single name
                        base_cost_names = [name.strip() for name in accessorial_applied_over.split(',')]
                        
                        # Find the base cost(s) in the mismatch report for the same ETOF
                        base_cost_found = False
                        total_base_cost = 0.0
                        found_costs_info = []
                        
                        for base_cost_name in base_cost_names:
                            if row_debug:
                                print(f"   [DEBUG] Looking for base cost '{base_cost_name}' in mismatch report for ETOF {etof_number}...")
                            
                            # Search in df_mismatch for rows with same ETOF and cost type matching base_cost_name
                            base_cost_name_lower = base_cost_name.lower().strip()
                            # Handle base name matching (e.g., "Transport cost" matches "Transport cost (National)")
                            base_name_pattern = re.sub(r'\s*\([^)]*\)\s*$', '', base_cost_name_lower).strip()
                            
                            for search_idx, search_row in df_mismatch.iterrows():
                                search_etof = None
                                search_cost_type = None
                                search_precalc_cost = None
                                
                                for col in search_row.index:
                                    col_lower = str(col).lower()
                                    if 'etof' in col_lower:
                                        search_etof = str(search_row[col]).strip() if pd.notna(search_row[col]) else None
                                    elif col_lower == 'cost type' or 'cost type' in col_lower:
                                        search_cost_type = str(search_row[col]).strip() if pd.notna(search_row[col]) else None
                                    elif 'pre' in col_lower and 'calc' in col_lower and 'cost' in col_lower:
                                        try:
                                            val = search_row[col]
                                            if pd.notna(val):
                                                search_precalc_cost = float(val)
                                        except (ValueError, TypeError):
                                            pass
                                
                                if search_etof == etof_number and search_cost_type:
                                    search_cost_type_lower = search_cost_type.lower().strip()
                                    search_base_pattern = re.sub(r'\s*\([^)]*\)\s*$', '', search_cost_type_lower).strip()
                                    
                                    # Check if it matches (exact or base name match)
                                    if (search_cost_type_lower == base_cost_name_lower or 
                                        search_base_pattern == base_name_pattern or
                                        search_cost_type_lower.startswith(base_name_pattern)):
                                        
                                        if search_precalc_cost is not None and search_precalc_cost > 0:
                                            total_base_cost += search_precalc_cost
                                            found_costs_info.append(f"{search_cost_type}: {search_precalc_cost}")
                                            base_cost_found = True
                                            if row_debug:
                                                print(f"   [DEBUG] Found base cost '{search_cost_type}' with Pre-calc. cost: {search_precalc_cost}")
                        
                        if base_cost_found and total_base_cost > 0:
                            # Calculate percentage cost
                            calculated_percentage_cost = total_base_cost * (accessorial_percentage_value / 100.0)
                            if row_debug:
                                print(f"   [DEBUG] Calculated percentage cost: {total_base_cost} * {accessorial_percentage_value}% = {calculated_percentage_cost:.2f}")
                            
                            reason = f"Percentage-based cost: {accessorial_percentage_value}% over [{', '.join(found_costs_info)}] = {calculated_percentage_cost:.2f}"
                        else:
                            if row_debug:
                                print(f"   [DEBUG] Base cost(s) '{accessorial_applied_over}' not found for ETOF {etof_number}")
                            reason = f"Percentage-based cost ({accessorial_percentage_value}% over '{accessorial_applied_over}') - base cost not found for this ETOF"
                    
                    elif accessorial_price_flat is not None:
                        reason = f"The cost is pre-calculated by rate card (accessorial) - {accessorial_price_flat} flat."
                        if row_debug:
                            print(f"   [DEBUG] Using accessorial Price Flat: {accessorial_price_flat}")
                    elif accessorial_price_per_unit is not None:
                        reason = f"Cost per unit (accessorial): {accessorial_price_per_unit}"
                        if row_debug:
                            print(f"   [DEBUG] Using accessorial Price per unit: {accessorial_price_per_unit}")
                    else:
                        reason = "The cost is not covered for the provided shipment details (accessorial - no price found)."
                        if row_debug:
                            print(f"   [DEBUG] No price found in accessorial data")
                else:
                    # Get comment from mapping
                    comment = etof_to_comment.get(etof_number)
                    
                    if row_debug:
                        print(f"   [DEBUG] Comment for ETOF {etof_number}: {str(comment)[:60] if comment else 'NOT FOUND'}...")
                    
                    if comment:
                        # Extract rate lane from comment
                        lanes = extract_rate_lane(comment)
                        
                        if row_debug:
                            print(f"   [DEBUG] Extracted lanes: {lanes}")
                        
                        if lanes:
                            # Skip if multiple lanes - too complex to handle
                            if len(lanes) > 1:
                                if row_debug:
                                    print(f"   [DEBUG] Multiple lanes found ({len(lanes)}): {lanes} - skipping")
                                reason = f"Multiple rate lanes found ({', '.join(lanes)}) - manual check required"
                            else:
                                lane_number = lanes[0]
                                
                                if row_debug:
                                    print(f"   [DEBUG] Using lane: {lane_number}")
                                
                                # Find price in rate data for this agreement
                                # Pass charge_weight for weight-tiered pricing
                                # Use cost_name_for_lookup which has the full name (e.g., "Delivery Fee (Sevilla)")
                                charge_weight_for_lookup = etof_to_charge_weight.get(etof_number)
                                
                                # First try with the matched cost name
                                actual_cost_name_used = cost_name_for_lookup
                                price, price_col, price_reason = find_cost_price_in_rate_data(
                                    df_rate_data, lane_number, cost_name_for_lookup, 
                                    debug=row_debug, return_reason=True,
                                    charge_weight=charge_weight_for_lookup
                                )
                                
                                # If not found, try to find alternative cost with same base name
                                if price is None:
                                    base_cost_name = re.sub(r'\s*\([^)]*\)\s*$', '', cost_name_for_lookup).strip()
                                    if row_debug:
                                        print(f"   [DEBUG] Cost '{cost_name_for_lookup}' not found for lane {lane_number}, looking for alternatives with base '{base_cost_name}'...")
                                    
                                    # Find all columns in rate data that match the base cost name
                                    # AND have non-empty values for this lane
                                    alternative_costs_flat = []  # List of (original_col_name, col_value) tuples
                                    
                                    # First, find the row for this lane
                                    lane_col = df_rate_data.columns[0]
                                    lane_mask = df_rate_data[lane_col].astype(str).str.strip() == str(lane_number).strip()
                                    lane_rows = df_rate_data[lane_mask]
                                    
                                    if not lane_rows.empty:
                                        lane_row = lane_rows.iloc[0]
                                        
                                        for col in df_rate_data.columns:
                                            col_str = str(col).strip()
                                            col_lower = col_str.lower()
                                            col_base = re.sub(r'\s*\([^)]*\)\s*$', '', col_lower).strip()
                                            
                                            if col_base == base_cost_name.lower() and col_str.lower() != cost_name_for_lookup.lower():
                                                # Check if this column has a non-empty value for this lane
                                                col_value = lane_row[col]  # Use original column name
                                                if pd.notna(col_value) and str(col_value).strip() not in ['', 'nan', 'NaN']:
                                                    try:
                                                        alternative_costs_flat.append((col, float(col_value)))
                                                        if row_debug:
                                                            print(f"   [DEBUG] Alternative '{col}' has value: {col_value}")
                                                    except (ValueError, TypeError):
                                                        pass
                                    
                                    if alternative_costs_flat and row_debug:
                                        print(f"   [DEBUG] Found {len(alternative_costs_flat)} alternative costs with values: {[c[0] for c in alternative_costs_flat]}")
                                    elif row_debug:
                                        print(f"   [DEBUG] No alternative costs with values found for base '{base_cost_name}'")
                                    
                                    # Try each alternative - alternative_costs_flat is list of (col_name, direct_value) tuples
                                    for alt_cost, direct_value in alternative_costs_flat:
                                        alt_cost_str = str(alt_cost).strip()
                                        alt_price, alt_col, alt_reason = find_cost_price_in_rate_data(
                                            df_rate_data, lane_number, alt_cost_str,
                                            debug=row_debug, return_reason=True,
                                            charge_weight=charge_weight_for_lookup
                                        )
                                        if alt_price is not None:
                                            price = alt_price
                                            price_col = alt_col
                                            price_reason = alt_reason
                                            actual_cost_name_used = alt_cost_str
                                            if row_debug:
                                                print(f"   [DEBUG] Using alternative cost '{alt_cost_str}' instead of '{cost_name_for_lookup}'")
                                            break
                                        else:
                                            # Fallback: use the direct value from the cost column itself
                                            # This handles rate cards where the price is directly in the cost column
                                            if direct_value > 0:
                                                price = direct_value
                                                price_col = alt_cost_str
                                                actual_cost_name_used = alt_cost_str
                                                if row_debug:
                                                    print(f"   [DEBUG] Using direct value {direct_value} from column '{alt_cost_str}' (no separate Price Flat column)")
                                                break
                                
                                if price is not None:
                                    # Note if an alternative cost was used
                                    alt_note = f" (using '{actual_cost_name_used}')" if actual_cost_name_used != cost_name_for_lookup else ""
                                    # Check if it was a weight-tiered price
                                    if price_col and ('<=' in str(price_col) or '>' in str(price_col)):
                                        reason = f"The cost is pre-calculated by rate card{alt_note} - {price} flat (weight tier: {price_col})."
                                    else:
                                        reason = f"The cost is pre-calculated by rate card{alt_note} - {price} flat."
                                else:
                                    # Price not found in rate_data - try accessorial costs as fallback
                                    if row_debug:
                                        print(f"   [DEBUG] Price not in rate_data, checking accessorial costs...")
                                    
                                    df_accessorial_fallback = get_accessorial_data_for_agreement(agreement, all_accessorial_costs, debug=row_debug)
                                    if df_accessorial_fallback is not None:
                                        # Get ship date for validity check in fallback
                                        ship_date_fallback = get_ship_date_from_row_data(etof_row_data, debug=row_debug)
                                        (_, _, _, acc_price_flat_fb, acc_price_per_unit_fb, _, _, _, _) = find_best_matching_accessorial_cost(
                                            cost_type, df_accessorial_fallback, lane_number, etof_row_data, debug=row_debug, ship_date=ship_date_fallback
                                        )
                                        
                                        if acc_price_flat_fb is not None:
                                            reason = f"The cost is pre-calculated by rate card (accessorial) - {acc_price_flat_fb} flat."
                                            if row_debug:
                                                print(f"   [DEBUG] Found accessorial fallback price: {acc_price_flat_fb}")
                                        elif acc_price_per_unit_fb is not None:
                                            reason = f"Cost per unit (accessorial): {acc_price_per_unit_fb}"
                                            if row_debug:
                                                print(f"   [DEBUG] Found accessorial fallback per-unit price: {acc_price_per_unit_fb}")
                                        else:
                                            # Still not found - use detailed reason if available
                                            reason = price_reason if price_reason else "The cost is not covered for the provided shipment details."
                                    else:
                                        # Use detailed reason if available
                                        reason = price_reason if price_reason else "The cost is not covered for the provided shipment details."
                        else:
                            reason = f"Could not extract rate lane from comment: {comment}"
                    else:
                        reason = f"No comment found for ETOF {etof_number}"
            
            else:
                # All other Rate By cases - use Price per unit
                # Determine what multiplier to use based on Rate By type:
                # 1. Weight-based (contains "weight" or "kg") -> use CHARGE_WEIGHT
                # 2. Measurement-based (Quantity/, Condition/) -> use MEASUREMENT/UNITS_MEASUREMENT
                
                rate_by_lower = rate_by.lower()
                is_weight_based = 'weight' in rate_by_lower or 'kg' in rate_by_lower or 'chargeable' in rate_by_lower
                
                # Get comment from mapping
                comment = etof_to_comment.get(etof_number)
                
                # Determine the multiplier value and its name
                multiplier_value = None
                multiplier_name = None
                multiplier_not_found_reason = None
                
                if is_weight_based:
                    # Use CHARGE_WEIGHT
                    charge_weight = etof_to_charge_weight.get(etof_number)
                    multiplier_value = charge_weight
                    multiplier_name = "CHARGE_WEIGHT"
                    if row_debug:
                        print(f"   [DEBUG] Rate By = '{rate_by}' (weight-based) -> using CHARGE_WEIGHT: {charge_weight}")
                    if charge_weight is None or (isinstance(charge_weight, float) and pd.isna(charge_weight)):
                        multiplier_not_found_reason = f"CHARGE_WEIGHT not found for ETOF {etof_number}"
                else:
                    # Use MEASUREMENT/UNITS_MEASUREMENT
                    measurement_str = etof_to_measurement.get(etof_number, '')
                    units_measurement_str = etof_to_units_measurement.get(etof_number, '')
                    
                    if row_debug:
                        print(f"   [DEBUG] Rate By = '{rate_by}' (measurement-based) -> looking in MEASUREMENT column...")
                    
                    meas_name, meas_value, meas_found = extract_measurement_value(
                        rate_by, measurement_str, units_measurement_str, debug=row_debug
                    )
                    
                    if meas_found:
                        multiplier_value = meas_value
                        multiplier_name = meas_name
                        if row_debug:
                            print(f"   [DEBUG] Found measurement '{meas_name}' = {meas_value}")
                    else:
                        # FALLBACK: Try to find value directly in ETOF columns
                        # This handles cases like Area/ldm -> LDM column, Area/cbm -> CBM column
                        if row_debug:
                            print(f"   [DEBUG] Measurement not found in MEASUREMENT column, trying direct column lookup...")
                        
                        etof_row_data_for_lookup = etof_to_row_data.get(etof_number, {})
                        col_name, col_value, col_found = find_value_in_etof_columns(
                            rate_by, etof_row_data_for_lookup, debug=row_debug
                        )
                        
                        if col_found:
                            multiplier_value = col_value
                            multiplier_name = col_name
                            if row_debug:
                                print(f"   [DEBUG] Found value in column '{col_name}' = {col_value}")
                        else:
                            multiplier_not_found_reason = f"'{meas_name or col_name}' not found in MEASUREMENT column or direct columns for ETOF {etof_number}"
                            if row_debug:
                                print(f"   [DEBUG] Value not found: {multiplier_not_found_reason}")
                
                if row_debug:
                    print(f"   [DEBUG] Comment for ETOF {etof_number}: {str(comment)[:60] if comment else 'NOT FOUND'}...")
                    print(f"   [DEBUG] Multiplier ({multiplier_name}): {multiplier_value}")
                
                # If using accessorial data, use the pre-fetched prices
                if use_accessorial:
                    price_per_unit = accessorial_price_per_unit
                    min_price = accessorial_price_flat if accessorial_has_min_flat else None  # MIN Flat in accessorial
                    max_price = None  # Accessorial doesn't have MAX
                    
                    if row_debug:
                        print(f"   [DEBUG] Processing with ACCESSORIAL data (non-PER SHIPMENT)...")
                        print(f"   [DEBUG] Accessorial Price per unit: {price_per_unit}")
                        print(f"   [DEBUG] Accessorial MIN Flat (if has_min_flat): {min_price}")
                        print(f"   [DEBUG] Multiplier value: {multiplier_value}, Multiplier name: {multiplier_name}")
                    
                    if price_per_unit is not None:
                        try:
                            price_float = float(price_per_unit)
                            
                            if multiplier_value is not None and not (isinstance(multiplier_value, float) and pd.isna(multiplier_value)):
                                try:
                                    multiplier_float = float(multiplier_value)
                                    
                                    # Apply rounding rule if present in rate_by
                                    rounding_dir, rounding_val = parse_rounding_rule(rate_by)
                                    original_multiplier = multiplier_float
                                    if rounding_dir and rounding_val:
                                        multiplier_float = apply_rounding_to_weight(multiplier_float, rounding_dir, rounding_val, debug=row_debug)
                                    
                                    total_cost = price_float * multiplier_float
                                    
                                    # Check MIN price (if accessorial has MIN Flat)
                                    min_applied = False
                                    if min_price is not None:
                                        try:
                                            min_price_float = float(min_price)
                                            if total_cost < min_price_float:
                                                min_applied = True
                                                rounding_info = f" (rounded {original_multiplier} -> {multiplier_float})" if rounding_dir else ""
                                                reason = f"MIN price applied (accessorial) - {min_price} (Calculated: {price_per_unit} * {multiplier_float}{rounding_info} ({multiplier_name}) = {total_cost:.2f}, but MIN is higher)"
                                                if row_debug:
                                                    print(f"   [DEBUG] Accessorial MIN price applied: {min_price} > calculated {total_cost:.2f}")
                                        except (ValueError, TypeError):
                                            pass
                                    
                                    if not min_applied:
                                        rounding_info = f" (rounded from {original_multiplier})" if rounding_dir and original_multiplier != multiplier_float else ""
                                        reason = f"Cost per unit (accessorial): {price_per_unit}, {multiplier_name}: {multiplier_float}{rounding_info}, Total: {price_per_unit} * {multiplier_float} = {total_cost:.2f}"
                                        if row_debug:
                                            print(f"   [DEBUG] Accessorial calculated: {price_per_unit} * {multiplier_float} = {total_cost:.2f}")
                                except (ValueError, TypeError):
                                    reason = f"Cost per unit (accessorial): {price_per_unit}, {multiplier_name}: {multiplier_value} (could not calculate - invalid multiplier value)"
                                    if row_debug:
                                        print(f"   [DEBUG] Accessorial: could not calculate - invalid multiplier value")
                            else:
                                if multiplier_not_found_reason:
                                    reason = f"Cost per unit (accessorial): {price_per_unit}, but {multiplier_not_found_reason}"
                                else:
                                    reason = f"Cost per unit (accessorial): {price_per_unit}, {multiplier_name} not found for ETOF {etof_number}"
                                if row_debug:
                                    print(f"   [DEBUG] Accessorial: multiplier not found")
                        except (ValueError, TypeError):
                            reason = f"Cost per unit (accessorial): {price_per_unit} (could not calculate - invalid price format)"
                            if row_debug:
                                print(f"   [DEBUG] Accessorial: invalid price format")
                    elif accessorial_price_flat is not None:
                        reason = f"The cost is pre-calculated by rate card (accessorial) - {accessorial_price_flat} flat."
                        if row_debug:
                            print(f"   [DEBUG] Accessorial: using flat price {accessorial_price_flat}")
                    else:
                        reason = "The cost is not covered for the provided shipment details (accessorial - no price found)."
                        if row_debug:
                            print(f"   [DEBUG] Accessorial: no price found")
                
                elif comment:
                    # Extract rate lane from comment
                    lanes = extract_rate_lane(comment)
                    
                    if row_debug:
                        print(f"   [DEBUG] Extracted lanes: {lanes}")
                    
                    if lanes:
                        if len(lanes) > 1:
                            if row_debug:
                                print(f"   [DEBUG] Multiple lanes found ({len(lanes)}): {lanes} - skipping")
                            reason = f"Multiple rate lanes found ({', '.join(lanes)}) - manual check required"
                        else:
                            lane_number = lanes[0]
                            
                            if row_debug:
                                print(f"   [DEBUG] Using lane: {lane_number}")
                            
                            # Find price per unit in rate data for this agreement
                            # Pass charge_weight for weight-tiered pricing
                            # Use cost_name_for_lookup which has the full name (e.g., "Delivery Fee (Sevilla)")
                            actual_cost_name_used_perunit = cost_name_for_lookup
                            price_per_unit, price_col, price_reason = find_cost_price_in_rate_data(
                                df_rate_data, lane_number, cost_name_for_lookup, 
                                price_type="per_unit", debug=row_debug, return_reason=True,
                                charge_weight=multiplier_value if is_weight_based else None
                            )
                            
                            # If not found, try to find alternative cost with same base name
                            if price_per_unit is None:
                                base_cost_name = re.sub(r'\s*\([^)]*\)\s*$', '', cost_name_for_lookup).strip()
                                if row_debug:
                                    print(f"   [DEBUG] Cost '{cost_name_for_lookup}' not found for lane {lane_number}, looking for alternatives with base '{base_cost_name}'...")
                                
                                # Find all columns in rate data that match the base cost name
                                # AND have non-empty values for this lane
                                alternative_costs = []  # List of (original_col_name, col_value) tuples
                                
                                # First, find the row for this lane
                                lane_col = df_rate_data.columns[0]
                                lane_mask = df_rate_data[lane_col].astype(str).str.strip() == str(lane_number).strip()
                                lane_rows = df_rate_data[lane_mask]
                                
                                if not lane_rows.empty:
                                    lane_row = lane_rows.iloc[0]
                                    
                                    # Debug: Show all columns matching the base cost name
                                    if row_debug:
                                        print(f"   [DEBUG] Checking all columns with base '{base_cost_name}' for lane {lane_number}:")
                                        for col in df_rate_data.columns:
                                            col_str = str(col).strip()
                                            col_lower = col_str.lower()
                                            col_base = re.sub(r'\s*\([^)]*\)\s*$', '', col_lower).strip()
                                            if col_base == base_cost_name.lower():
                                                col_value = lane_row[col]
                                                print(f"      - Column '{col_str}': value = {col_value} (type: {type(col_value).__name__})")
                                    
                                    for col in df_rate_data.columns:
                                        col_str = str(col).strip()
                                        col_lower = col_str.lower()
                                        col_base = re.sub(r'\s*\([^)]*\)\s*$', '', col_lower).strip()
                                        
                                        if col_base == base_cost_name.lower() and col_str.lower() != cost_name_for_lookup.lower():
                                            # Check if this column has a non-empty value for this lane
                                            col_value = lane_row[col]  # Use original column name
                                            if pd.notna(col_value) and str(col_value).strip() not in ['', 'nan', 'NaN']:
                                                try:
                                                    # Store original col name and value
                                                    alternative_costs.append((col, float(col_value)))
                                                    if row_debug:
                                                        print(f"   [DEBUG] Alternative '{col}' has value: {col_value}")
                                                except (ValueError, TypeError):
                                                    # Non-numeric value (e.g., "EUR") - check for weight-tiered columns
                                                    col_idx = list(df_rate_data.columns).index(col)
                                                    if row_debug:
                                                        print(f"   [DEBUG] Alternative '{col}' has non-numeric value: {col_value}, checking for weight-tiered columns...")
                                                    
                                                    # Look at subsequent columns for weight-tiered prices (e.g., "Flat <=100", "per unit >X")
                                                    for next_col in df_rate_data.columns[col_idx+1:col_idx+50]:
                                                        next_col_str = str(next_col).lower()
                                                        if any(k in next_col_str for k in ['flat', 'per unit', '<=', '>=', '<', '>']):
                                                            next_val = lane_row[next_col]
                                                            if pd.notna(next_val):
                                                                try:
                                                                    next_val_float = float(next_val)
                                                                    if next_val_float > 0:
                                                                        # Found a weight-tiered price column
                                                                        alternative_costs.append((col, next_val_float))
                                                                        if row_debug:
                                                                            print(f"   [DEBUG] Alternative '{col}' has weight-tiered price in '{next_col}': {next_val_float}")
                                                                        break
                                                                except (ValueError, TypeError):
                                                                    pass
                                
                                if alternative_costs and row_debug:
                                    print(f"   [DEBUG] Found {len(alternative_costs)} alternative costs with values: {[c[0] for c in alternative_costs]}")
                                elif row_debug:
                                    print(f"   [DEBUG] No alternative costs with values found for base '{base_cost_name}'")
                                
                                # Try each alternative - alternative_costs is list of (col_name, direct_value) tuples
                                for alt_cost, direct_value in alternative_costs:
                                    alt_cost_str = str(alt_cost).strip()
                                    alt_price, alt_col, alt_reason = find_cost_price_in_rate_data(
                                        df_rate_data, lane_number, alt_cost_str,
                                        price_type="per_unit", debug=row_debug, return_reason=True,
                                        charge_weight=multiplier_value if is_weight_based else None
                                    )
                                    if alt_price is not None:
                                        price_per_unit = alt_price
                                        price_col = alt_col
                                        price_reason = alt_reason
                                        actual_cost_name_used_perunit = alt_cost_str
                                        if row_debug:
                                            print(f"   [DEBUG] Using alternative cost '{alt_cost_str}' instead of '{cost_name_for_lookup}'")
                                        break
                                    else:
                                        # Fallback: weight-tiered FLAT pricing
                                        # The direct_value is from a weight-tiered column
                                        if direct_value > 0:
                                            price_per_unit = direct_value
                                            price_col = alt_cost_str
                                            actual_cost_name_used_perunit = alt_cost_str
                                            if row_debug:
                                                print(f"   [DEBUG] Using weight-tiered value {direct_value} from column '{alt_cost_str}'")
                                            
                                            # For weight-tiered flat pricing, use bracket matching logic
                                            # Get Pre-calc. cost and Carrier's cost from mismatch row
                                            precalc_cost_alt = None
                                            carrier_cost_alt = None
                                            
                                            for col_key in row.index:
                                                col_key_str = str(col_key).lower()
                                                if 'pre' in col_key_str and 'calc' in col_key_str and 'cost' in col_key_str:
                                                    try:
                                                        val = row[col_key]
                                                        if pd.notna(val):
                                                            precalc_cost_alt = float(val)
                                                    except (ValueError, TypeError):
                                                        pass
                                                elif 'carrier' in col_key_str and 'cost' in col_key_str:
                                                    try:
                                                        val = row[col_key]
                                                        if pd.notna(val):
                                                            carrier_cost_alt = float(val)
                                                    except (ValueError, TypeError):
                                                        pass
                                            
                                            if row_debug:
                                                print(f"   [DEBUG] Pre-calc. cost: {precalc_cost_alt}, Carrier's cost: {carrier_cost_alt}")
                                            
                                            # Try to match pre-calc cost to weight brackets
                                            bracket_match_alt = find_weight_bracket_match(
                                                df_rate_data, lane_number, alt_cost_str,
                                                precalc_cost_alt, carrier_cost_alt, debug=row_debug
                                            )
                                            
                                            if bracket_match_alt:
                                                matched_bracket, matched_value, per_unit_price_br, calculated_units = bracket_match_alt
                                                
                                                if per_unit_price_br and calculated_units:
                                                    reason = f"Using '{alt_cost_str}': Pre-calculated for weight bracket {matched_bracket} - {matched_value}, while stated {carrier_cost_alt} as for {calculated_units:.0f} units"
                                                elif matched_bracket and matched_value:
                                                    reason = f"Using '{alt_cost_str}': Pre-calculated for weight bracket {matched_bracket} - {matched_value}"
                                                
                                                # Set price_per_unit to the matched value
                                                price_per_unit = matched_value
                                                price_col = f"BRACKET:{matched_bracket}"  # Mark as bracket match
                                                
                                                if row_debug:
                                                    print(f"   [DEBUG] Bracket match: {matched_bracket} = {matched_value}, reason set")
                                            else:
                                                # No bracket match, report the flat price we found
                                                reason = f"Using '{alt_cost_str}': Weight-tiered flat price {direct_value}"
                                                if row_debug:
                                                    print(f"   [DEBUG] No bracket match, using direct value: {direct_value}")
                                            
                                            break
                            
                            # Also check for MIN and MAX prices (these are optional, don't need reason)
                            # Use the actual cost name that was found
                            min_price, min_price_col = find_cost_price_in_rate_data(df_rate_data, lane_number, actual_cost_name_used_perunit, price_type="min", debug=row_debug)
                            max_price, max_price_col = find_cost_price_in_rate_data(df_rate_data, lane_number, actual_cost_name_used_perunit, price_type="max", debug=row_debug)
                            
                            if row_debug:
                                print(f"   [DEBUG] Price per unit: {price_per_unit}, MIN price: {min_price}, MAX price: {max_price}")
                                if price_col:
                                    print(f"   [DEBUG] Price column: {price_col}")
                            
                            if price_per_unit is not None:
                                # SPECIAL CASE: Check if this is actually a weight-tiered FLAT price
                                # (when per_unit column doesn't exist but weight-tiered flat columns do)
                                # The price_col will contain "FLAT_TIER:" prefix in this case
                                is_flat_tier_fallback = price_col and 'FLAT_TIER:' in str(price_col)
                                
                                if is_flat_tier_fallback:
                                    # Extract the actual tier description
                                    tier_desc = str(price_col).replace('FLAT_TIER:', '')
                                    if row_debug:
                                        print(f"   [DEBUG] Using weight-tiered FLAT price (fallback): {price_per_unit} for tier {tier_desc}")
                                    reason = f"Weight-tiered flat price: {price_per_unit} (tier: {tier_desc}, {multiplier_name}: {multiplier_value})"
                                else:
                                    # Normal per_unit price - multiply by weight/quantity
                                    try:
                                        price_float = float(price_per_unit)
                                        
                                        # Check if multiplier is available
                                        if multiplier_value is not None and not (isinstance(multiplier_value, float) and pd.isna(multiplier_value)):
                                            try:
                                                multiplier_float = float(multiplier_value)
                                                
                                                # Apply rounding rule if present in rate_by
                                                rounding_dir, rounding_val = parse_rounding_rule(rate_by)
                                                original_multiplier = multiplier_float
                                                if rounding_dir and rounding_val:
                                                    multiplier_float = apply_rounding_to_weight(multiplier_float, rounding_dir, rounding_val, debug=row_debug)
                                                
                                                total_cost = price_float * multiplier_float
                                                
                                                # Check if MIN or MAX price applies
                                                min_applied = False
                                                max_applied = False
                                                
                                                # Build rounding info string for reason
                                                rounding_info_str = ""
                                                if rounding_dir and original_multiplier != multiplier_float:
                                                    rounding_info_str = f" (rounded {original_multiplier} -> {multiplier_float})"
                                                
                                                # Check MIN price
                                                if min_price is not None:
                                                    try:
                                                        min_price_float = float(min_price)
                                                        if total_cost < min_price_float:
                                                            min_applied = True
                                                            reason = f"MIN price applied - {min_price} (Calculated: {price_per_unit} * {multiplier_float}{rounding_info_str} ({multiplier_name}) = {total_cost:.2f}, but MIN is higher)"
                                                            if row_debug:
                                                                print(f"   [DEBUG] MIN price applied: {min_price} > calculated {total_cost:.2f}")
                                                    except (ValueError, TypeError):
                                                        pass  # MIN price couldn't be parsed, ignore it
                                                
                                                # Check MAX price (only if MIN was not applied)
                                                if not min_applied and max_price is not None:
                                                    try:
                                                        max_price_float = float(max_price)
                                                        if total_cost > max_price_float:
                                                            max_applied = True
                                                            reason = f"MAX price applied - {max_price} (Calculated: {price_per_unit} * {multiplier_float}{rounding_info_str} ({multiplier_name}) = {total_cost:.2f}, but MAX is lower)"
                                                            if row_debug:
                                                                print(f"   [DEBUG] MAX price applied: {max_price} < calculated {total_cost:.2f}")
                                                    except (ValueError, TypeError):
                                                        pass  # MAX price couldn't be parsed, ignore it
                                                
                                                if not min_applied and not max_applied:
                                                    # Check if it was a weight-tiered price
                                                    tier_info = ""
                                                    if price_col and ('<=' in str(price_col) or '>' in str(price_col)):
                                                        tier_info = f" (weight tier: {price_col})"
                                                    reason = f"Cost per unit: {price_per_unit}{tier_info}, {multiplier_name}: {multiplier_float}{rounding_info_str}, Total: {price_per_unit} * {multiplier_float} = {total_cost:.2f}"
                                                    if row_debug:
                                                        print(f"   [DEBUG] Calculated: {price_per_unit} * {multiplier_float} = {total_cost:.2f}")
                                            except (ValueError, TypeError):
                                                reason = f"Cost per unit: {price_per_unit}, {multiplier_name}: {multiplier_value} (could not calculate - invalid multiplier value)"
                                        else:
                                            # Multiplier not found - use the specific reason
                                            # BUT don't overwrite if we already have a bracket match reason
                                            if price_col and str(price_col).startswith('BRACKET:'):
                                                # Bracket match reason already set, don't overwrite
                                                if row_debug:
                                                    print(f"   [DEBUG] Keeping bracket match reason (not overwriting)")
                                            elif multiplier_not_found_reason:
                                                reason = f"Cost per unit: {price_per_unit}, but {multiplier_not_found_reason}"
                                            else:
                                                reason = f"Cost per unit: {price_per_unit}, {multiplier_name} not found for ETOF {etof_number}"
                                    except (ValueError, TypeError):
                                        reason = f"Cost per unit: {price_per_unit} (could not calculate - invalid price format)"
                            else:
                                # FALLBACK: When no price_per_unit and no CHARGE_WEIGHT
                                # Try to match Pre-calc. cost to weight bracket values
                                if is_weight_based and (multiplier_value is None or (isinstance(multiplier_value, float) and pd.isna(multiplier_value))):
                                    if row_debug:
                                        print(f"   [DEBUG] Fallback: trying to match Pre-calc. cost to weight brackets...")
                                    
                                    # Get Pre-calc. cost and Carrier's cost from MISMATCH row (not ETOF data)
                                    precalc_cost = None
                                    carrier_cost = None
                                    
                                    # Search in mismatch row columns for Pre-calc. cost and Carrier's cost
                                    if row_debug:
                                        print(f"   [DEBUG] Mismatch row columns: {list(row.index)[:15]}")
                                    
                                    for col_key in row.index:
                                        col_key_str = str(col_key)
                                        col_key_lower = col_key_str.lower()
                                        col_val = row.get(col_key)
                                        
                                        # Check for Pre-calc. cost column (various formats)
                                        # Could be: "Pre-calc. cost", "PRE_CALC_COST", "Precalc cost", etc.
                                        is_precalc_col = ('pre-calc' in col_key_lower or 'precalc' in col_key_lower or 
                                                         'pre_calc' in col_key_lower or 'pre calc' in col_key_lower)
                                        
                                        # Check for Carrier's cost column
                                        is_carrier_col = ('carrier' in col_key_lower and 'cost' in col_key_lower)
                                        
                                        if col_val is not None and pd.notna(col_val):
                                            try:
                                                val_float = float(col_val)
                                                
                                                if is_precalc_col and precalc_cost is None:
                                                    precalc_cost = val_float
                                                    if row_debug:
                                                        print(f"   [DEBUG] Found Pre-calc. cost: {precalc_cost} (from mismatch column '{col_key_str}')")
                                                
                                                if is_carrier_col and carrier_cost is None:
                                                    carrier_cost = val_float
                                                    if row_debug:
                                                        print(f"   [DEBUG] Found Carrier's cost: {carrier_cost} (from mismatch column '{col_key_str}')")
                                            except (ValueError, TypeError):
                                                pass
                                    
                                    # Get all weight bracket columns from rate card for this lane and cost
                                    bracket_match = find_weight_bracket_match(
                                        df_rate_data, lane_number, cost_name_for_lookup, 
                                        precalc_cost, carrier_cost, debug=row_debug
                                    )
                                    
                                    if bracket_match:
                                        matched_bracket, matched_value, per_unit_price, calculated_units = bracket_match
                                        
                                        if per_unit_price and calculated_units:
                                            reason = f"Pre-calculated for weight bracket {matched_bracket} - {matched_value}, while stated {carrier_cost} as for {calculated_units:.0f} units"
                                        elif matched_bracket and matched_value:
                                            reason = f"Pre-calculated for weight bracket {matched_bracket} - {matched_value}"
                                        else:
                                            reason = price_reason if price_reason else "The cost is not covered for the provided shipment details."
                                    else:
                                        # Use detailed reason if available
                                        reason = price_reason if price_reason else "The cost is not covered for the provided shipment details."
                                else:
                                    # Use detailed reason if available
                                    reason = price_reason if price_reason else "The cost is not covered for the provided shipment details."
                    else:
                        reason = f"Could not extract rate lane from comment: {comment}"
                else:
                    reason = f"No comment found for ETOF {etof_number}"
        
        if row_debug:
            print(f"   [DEBUG] Final reason: {reason[:60]}..." if len(reason) > 60 else f"   [DEBUG] Final reason: {reason}")
            debug_count += 1
        
        rate_by_values.append(rate_by)
        applies_if_values.append(applies_if)
        reasons.append(reason)
    
    # Add Rate By and Applies If columns to the DataFrame
    df['Rate By'] = rate_by_values
    df['Applies If'] = applies_if_values
    df['Reason'] = reasons
    
    # Print summary
    print(f"\n   Reason summary:")
    reason_counts = df['Reason'].value_counts()
    for reason, count in reason_counts.head(10).items():
        reason_short = reason[:60] + "..." if len(reason) > 60 else reason
        print(f"      {count}: {reason_short}")
    
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


def save_result_with_tabs(df, output_filename="conditions_checked.xlsx"):
    """Save the result DataFrame to Excel with separate tabs per Carrier Agreement #."""
    output_folder = Path(__file__).parent / "partly_df"
    output_folder.mkdir(exist_ok=True)
    
    output_path = output_folder / output_filename
    
    # Find Carrier Agreement # column
    agreement_col = None
    for col in df.columns:
        if 'carrier' in col.lower() and 'agreement' in col.lower():
            agreement_col = col
            break
    
    if agreement_col is None:
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


def main(debug=False, debug_first_n=300):
    """
    Main function to run conditions checking.
    
    Args:
        debug: If True, print debug information for first N rows
        debug_first_n: Number of rows to debug (default 5)
    """
    # Clear caches at start of each run
    clear_accessorial_cache()
    
    print("\n" + "="*80)
    print("CONDITIONS CHECKING")
    print("="*80)
    
    if debug:
        print(f"\n   [DEBUG MODE ENABLED - showing details for first {debug_first_n} rows]")
    
    # Step 1: Load mismatch filing from file (all tabs)
    print("\n1. Loading mismatch filing from file...")
    df_mismatch = load_mismatch_filing()
    print(f"   Columns: {list(df_mismatch.columns)}")
    
    # Step 2: Load LC-ETOF with comments from file
    print("\n2. Loading LC-ETOF with comments from file...")
    df_lc_etof_mapping = load_lc_etof_with_comments()
    print(f"   Columns: {list(df_lc_etof_mapping.columns)}")
    
    # Step 3: Load all rate cost files
    print("\n3. Loading all rate cost files...")
    all_rate_costs = load_all_rate_costs()
    
    if not all_rate_costs:
        print("   [WARNING] No rate cost files found.")
    
    # Step 3b: Load all accessorial cost files
    print("\n3b. Loading all accessorial cost files...")
    all_accessorial_costs = load_all_accessorial_costs()
    
    if not all_rate_costs and not all_accessorial_costs:
        print("   [ERROR] No cost files found (neither rate nor accessorial). Cannot continue.")
        return None
    
    # Step 4: Check conditions and add Reason
    print("\n4. Checking conditions and adding Reason column...")
    df_result = check_conditions_and_add_reason(
        df_mismatch, df_lc_etof_mapping, all_rate_costs, all_accessorial_costs,
        debug=debug, debug_first_n=debug_first_n
    )
    
    # Step 5: Save result (with tabs per Carrier Agreement #)
    print("\n5. Saving result (with tabs per Carrier Agreement #)...")
    output_path = save_result_with_tabs(df_result)
    
    print("\n" + "="*80)
    print(f"DONE! Output saved to: {output_path}")
    print("="*80)
    
    return df_result


if __name__ == "__main__":
    # Set DEBUG = True to see detailed processing for first N rows
    DEBUG = True
    DEBUG_FIRST_N = 1000  # Process all rows with debug
    
    # Setup logging to file
    log_folder = get_partly_df_folder()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_folder / f"conditions_debug_{timestamp}.txt"
    
    # Redirect stdout to both console and file
    logger = Logger(str(log_file))
    sys.stdout = logger
    
    print(f"Logging debug output to: {log_file}")
    print("="*80)
    
    try:
        df_result = main(debug=DEBUG, debug_first_n=DEBUG_FIRST_N)
    finally:
        # Restore stdout and close log file
        sys.stdout = logger.terminal
        logger.close()
        print(f"\nDebug log saved to: {log_file}")
