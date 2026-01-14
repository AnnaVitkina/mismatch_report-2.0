"""
Vocabulary Mapping Script

This script:
1. Reads LC/ETOF mapping file from partly_df folder (created by part7_optional_order_lc_etof_mapping.py)
2. For each Carrier agreement # tab, finds the corresponding rate card file in partly_df folder
3. Creates vocabulary mappings for each rate card separately
4. Saves separate output files for each rate card
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
from pathlib import Path

# Import processing functions (only for fallback, prefer reading from files)
from part4_rate_card_processing import (
    process_rate_card,
    process_business_rules,
    transform_business_rules_to_conditions,
    find_business_rule_columns,
    get_business_rules_lookup,
    get_required_geo_columns
)
from part1_etof_file_processing import process_etof_file
# from part3_origin_file_processing import process_origin_file  # COMMENTED OUT - origin file processing disabled
from part7_optional_order_lc_etof_mapping import process_lc_etof_mapping

# Try to import lightweight ML libraries for semantic similarity
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    SEMANTIC_AVAILABLE = True
except ImportError:
    SEMANTIC_AVAILABLE = False
    print("Note: sentence-transformers not available. Install with: pip install sentence-transformers scikit-learn")
    print("      Will use fuzzy string matching instead.")


def get_partly_df_folder():
    """Get the path to the partly_df folder."""
    return Path(__file__).parent / "partly_df"


def get_lc_etof_mapping_file(filename="lc_etof_mapping.xlsx"):
    """
    Get the path to the LC/ETOF mapping file created by part7_optional_order_lc_etof_mapping.py
    
    Args:
        filename: Name of the mapping file (default: "lc_etof_mapping.xlsx")
    
    Returns:
        Path to the file if it exists, else None
    """
    partly_df_folder = get_partly_df_folder()
    file_path = partly_df_folder / filename
    
    if file_path.exists():
        return file_path
    
    # Also check for order_lc_etof_mapping.xlsx (alternative name)
    alt_path = partly_df_folder / "order_lc_etof_mapping.xlsx"
    if alt_path.exists():
        return alt_path
    
    return None


def get_agreement_tabs_from_mapping_file(mapping_file_path):
    """
    Get all Carrier agreement # tabs from the mapping file.
    Excludes 'All Data' and 'No Agreement' tabs.
    
    Args:
        mapping_file_path: Path to the LC/ETOF mapping Excel file
    
    Returns:
        list: List of agreement tab names (e.g., ['RA20220420022', 'RA20241129009'])
    """
    if mapping_file_path is None or not Path(mapping_file_path).exists():
        print(f"   [ERROR] Mapping file not found: {mapping_file_path}")
        return []
    
    try:
        xl = pd.ExcelFile(mapping_file_path)
        sheet_names = xl.sheet_names
        
        # Filter out 'All Data' and 'No Agreement' tabs
        excluded_tabs = ['All Data', 'all data', 'No Agreement', 'no agreement']
        agreement_tabs = [name for name in sheet_names if name not in excluded_tabs]
        
        print(f"   Found {len(agreement_tabs)} agreement tabs: {agreement_tabs}")
        return agreement_tabs
    except Exception as e:
        print(f"   [ERROR] Could not read mapping file: {e}")
        return []


def read_lc_data_from_tab(mapping_file_path, tab_name):
    """
    Read LC data from a specific tab in the mapping file.
    
    Args:
        mapping_file_path: Path to the LC/ETOF mapping Excel file
        tab_name: Name of the tab to read
    
    Returns:
        tuple: (dataframe, list of column names) or (None, []) if error
    """
    if mapping_file_path is None or not Path(mapping_file_path).exists():
        print(f"   [ERROR] Mapping file not found: {mapping_file_path}")
        return None, []
    
    try:
        df = pd.read_excel(mapping_file_path, sheet_name=tab_name)
        columns = df.columns.tolist()
        print(f"   Read {len(df)} rows, {len(columns)} columns from tab '{tab_name}'")
        return df, columns
    except Exception as e:
        print(f"   [ERROR] Could not read tab '{tab_name}': {e}")
        return None, []


def find_rate_card_file(agreement_number):
    """
    Find the rate card file for a given agreement number in the partly_df folder.
    
    Args:
        agreement_number: The agreement number (e.g., 'RA20220420022')
    
    Returns:
        Path to the rate card file if found, else None
    """
    partly_df_folder = get_partly_df_folder()
    
    # Try exact match first
    exact_path = partly_df_folder / f"{agreement_number}.xlsx"
    if exact_path.exists():
        print(f"   Found rate card file: {exact_path}")
        return exact_path
    
    # Try with variations (case-insensitive)
    for file in partly_df_folder.glob("*.xlsx"):
        if file.stem.lower() == agreement_number.lower():
            print(f"   Found rate card file: {file}")
            return file
    
    # Try partial match
    for file in partly_df_folder.glob("*.xlsx"):
        if agreement_number.lower() in file.stem.lower():
            print(f"   Found rate card file (partial match): {file}")
            return file
    
    print(f"   [WARNING] Rate card file not found for agreement: {agreement_number}")
    return None


def read_rate_card_from_file(rate_card_file_path):
    """
    Read rate card data and conditions from a processed rate card file.
    
    Args:
        rate_card_file_path: Path to the rate card Excel file (in partly_df folder)
    
    Returns:
        tuple: (dataframe, list of column names, conditions dict, agreement number) or (None, [], {}, None) if error
    """
    if rate_card_file_path is None or not Path(rate_card_file_path).exists():
        print(f"   [ERROR] Rate card file not found: {rate_card_file_path}")
        return None, [], {}, None
    
    try:
        # Read the Rate Card Data sheet
        df = pd.read_excel(rate_card_file_path, sheet_name='Rate Card Data')
        columns = df.columns.tolist()
        
        # Try to read conditions from the Conditions sheet
        conditions = {}
        try:
            df_conditions = pd.read_excel(rate_card_file_path, sheet_name='Conditions')
            for _, row in df_conditions.iterrows():
                col_name = row.get('Column')
                condition = row.get('Condition Rule', '')
                if col_name and condition and str(condition).strip():
                    conditions[col_name] = str(condition).strip()
        except Exception:
            pass  # Conditions sheet might not exist
        
        # Get agreement number from filename
        agreement_number = Path(rate_card_file_path).stem
        
        print(f"   Read rate card: {len(df)} rows, {len(columns)} columns, {len(conditions)} conditions")
        return df, columns, conditions, agreement_number
    except Exception as e:
        print(f"   [ERROR] Could not read rate card file: {e}")
        return None, [], {}, None


def read_business_rules_from_file(rate_card_file_path):
    """
    Read business rules from a processed rate card file.
    
    Args:
        rate_card_file_path: Path to the rate card Excel file (in partly_df folder)
    
    Returns:
        dict: Business rules information with:
            - 'rules': list of rule dicts with Rule Name, Section, Country, Postal Codes, Exclude, Rate Card Columns
            - 'zone_columns': dict mapping rate card columns to their underlying mappings
              e.g., {'Origin Postal Code Zone': {'country_target': 'SHIP_COUNTRY', 'postal_target': 'SHIP_POST', 'rules': [...]}}
            - 'skip_columns': set of columns that should NOT be directly mapped (they're zone columns)
    """
    result = {
        'rules': [],
        'zone_columns': {},
        'skip_columns': set()
    }
    
    if rate_card_file_path is None or not Path(rate_card_file_path).exists():
        return result
    
    try:
        # Read the Business Rules sheet
        df_rules = pd.read_excel(rate_card_file_path, sheet_name='Business Rules')
        
        for _, row in df_rules.iterrows():
            rule_data = {
                'rule_name': str(row.get('Rule Name', '')).strip(),
                'section': str(row.get('Section', '')).strip(),
                'country': str(row.get('Country', '')).strip(),
                'postal_codes': str(row.get('Postal Codes', '')).strip(),
                'exclude': str(row.get('Exclude', '')).strip().lower() in ['yes', 'true', '1'],
                'rate_card_columns': str(row.get('Rate Card Columns', '')).strip()
            }
            result['rules'].append(rule_data)
            
            # Parse the Rate Card Columns to identify zone columns
            rate_card_cols = rule_data['rate_card_columns']
            if rate_card_cols and rate_card_cols != '(not found in data)':
                for col in rate_card_cols.split(','):
                    col = col.strip()
                    if col:
                        # Determine if this is an Origin or Destination column
                        col_lower = col.lower()
                        if 'origin' in col_lower or 'ship' in col_lower or 'from' in col_lower:
                            country_target = 'SHIP_COUNTRY'
                            postal_target = 'SHIP_POST'
                        elif 'destination' in col_lower or 'cust' in col_lower or 'to' in col_lower:
                            country_target = 'CUST_COUNTRY'
                            postal_target = 'CUST_POST'
                        else:
                            # Default to origin if unclear
                            country_target = 'SHIP_COUNTRY'
                            postal_target = 'SHIP_POST'
                        
                        if col not in result['zone_columns']:
                            result['zone_columns'][col] = {
                                'country_target': country_target,
                                'postal_target': postal_target,
                                'countries': set(),
                                'postal_codes': set(),
                                'rules': []
                            }
                        
                        # Add this rule's data
                        result['zone_columns'][col]['rules'].append(rule_data)
                        if rule_data['country']:
                            result['zone_columns'][col]['countries'].add(rule_data['country'])
                        if rule_data['postal_codes']:
                            # Parse postal codes (comma-separated)
                            for pc in rule_data['postal_codes'].split(','):
                                pc = pc.strip()
                                if pc:
                                    result['zone_columns'][col]['postal_codes'].add(pc)
                        
                        # Mark this column as one to skip from direct mapping
                        result['skip_columns'].add(col)
        
        print(f"   Read business rules: {len(result['rules'])} rules, {len(result['zone_columns'])} zone columns to skip")
        if result['skip_columns']:
            print(f"   Zone columns (skip direct mapping): {sorted(result['skip_columns'])}")
        
    except Exception as e:
        print(f"   [INFO] No Business Rules sheet found or error reading: {e}")
    
    return result


def get_business_rule_mappings(business_rules_info):
    """
    Get the required column mappings based on business rules.
    
    When a zone column like "Origin Postal Code Zone" is found, this returns
    the mappings that should be applied instead:
    - Country from business rules → SHIP_COUNTRY or CUST_COUNTRY
    - Postal codes from business rules → SHIP_POST or CUST_POST
    
    Args:
        business_rules_info: Output from read_business_rules_from_file()
    
    Returns:
        dict: Required mappings with:
            - 'required_columns': dict of {standard_name: {'source': 'business_rule', 'values': set}}
              e.g., {'SHIP_COUNTRY': {'source': 'business_rule', 'values': {'ES'}}}
            - 'zone_column_to_standard': dict mapping zone columns to their standard column mappings
    """
    result = {
        'required_columns': {},
        'zone_column_to_standard': {}
    }
    
    for zone_col, zone_info in business_rules_info.get('zone_columns', {}).items():
        country_target = zone_info['country_target']
        postal_target = zone_info['postal_target']
        
        # Record required mappings
        if zone_info['countries']:
            if country_target not in result['required_columns']:
                result['required_columns'][country_target] = {'source': 'business_rule', 'values': set()}
            result['required_columns'][country_target]['values'].update(zone_info['countries'])
        
        if zone_info['postal_codes']:
            if postal_target not in result['required_columns']:
                result['required_columns'][postal_target] = {'source': 'business_rule', 'values': set()}
            result['required_columns'][postal_target]['values'].update(zone_info['postal_codes'])
        
        # Map zone column to standard columns
        result['zone_column_to_standard'][zone_col] = {
            'country': country_target,
            'postal': postal_target
        }
    
    if result['required_columns']:
        print(f"   Business rule required mappings:")
        for col, info in result['required_columns'].items():
            print(f"      - {col}: {len(info['values'])} unique values from business rules")
    
    return result


# Direct mappings from Rate Card columns to LC columns
# These are explicit, fixed mappings that should always be used
RATE_CARD_TO_LC_DIRECT_MAPPINGS = {
    # Origin/Ship columns
    'Origin Country': 'SHIP_COUNTRY',
    'Origin Postal Code': 'SHIP_POST',
    'Origin City': 'SHIP_CITY',
    'Origin State': 'SHIP_STATE',
    'Origin Airport': 'SHIP_AIRPORT',
    # Destination/Customer columns
    'Destination Country': 'CUST_COUNTRY',
    'Destination Postal Code': 'CUST_POST',
    'Destination City': 'CUST_CITY',
    'Destination state': 'CUST_STATE',
    'Destination State': 'CUST_STATE',
    'Destination Airport': 'CUST_AIRPORT',
}

# Columns that should be SKIPPED from mapping (zone columns)
# These contain zone values like "HUB MADRID", "Zone A" etc.
ZONE_COLUMN_PATTERNS = [
    'postal code zone',
    'postalcodezone',
    'postal_code_zone',
]


def is_zone_column(column_name):
    """Check if a column is a zone column that should be skipped from direct mapping."""
    col_lower = column_name.lower().replace(' ', '').replace('_', '')
    for pattern in ZONE_COLUMN_PATTERNS:
        pattern_normalized = pattern.lower().replace(' ', '').replace('_', '')
        if pattern_normalized in col_lower:
            return True
    return False


def get_direct_lc_mapping(rate_card_column):
    """
    Get the direct LC column mapping for a rate card column.
    
    Args:
        rate_card_column: Rate card column name
    
    Returns:
        str or None: LC column name if direct mapping exists, else None
    """
    # Check exact match first
    if rate_card_column in RATE_CARD_TO_LC_DIRECT_MAPPINGS:
        return RATE_CARD_TO_LC_DIRECT_MAPPINGS[rate_card_column]
    
    # Check case-insensitive match
    col_lower = rate_card_column.lower()
    for rc_col, lc_col in RATE_CARD_TO_LC_DIRECT_MAPPINGS.items():
        if rc_col.lower() == col_lower:
            return lc_col
    
    # Check for partial matches with normalization
    col_normalized = col_lower.replace(' ', '').replace('_', '')
    
    # Origin mappings
    if 'origin' in col_normalized or 'ship' in col_normalized or 'from' in col_normalized:
        if 'country' in col_normalized:
            return 'SHIP_COUNTRY'
        if 'postal' in col_normalized or 'zip' in col_normalized or 'post' in col_normalized:
            if 'zone' not in col_normalized:  # Skip zone columns
                return 'SHIP_POST'
        if 'city' in col_normalized:
            return 'SHIP_CITY'
        if 'state' in col_normalized:
            return 'SHIP_STATE'
        if 'airport' in col_normalized:
            return 'SHIP_AIRPORT'
    
    # Destination mappings
    if 'destination' in col_normalized or 'cust' in col_normalized or 'to' in col_normalized:
        if 'country' in col_normalized:
            return 'CUST_COUNTRY'
        if 'postal' in col_normalized or 'zip' in col_normalized or 'post' in col_normalized:
            if 'zone' not in col_normalized:  # Skip zone columns
                return 'CUST_POST'
        if 'city' in col_normalized:
            return 'CUST_CITY'
        if 'state' in col_normalized:
            return 'CUST_STATE'
        if 'airport' in col_normalized:
            return 'CUST_AIRPORT'
    
    return None

# Initialize lightweight model for semantic similarity (if available)
_semantic_model = None
def get_semantic_model():
    """Get or initialize the semantic similarity model."""
    global _semantic_model
    if _semantic_model is None and SEMANTIC_AVAILABLE:
        try:
            _semantic_model = SentenceTransformer('all-MiniLM-L6-v2')  # ~80MB, fast
            print("   Loaded semantic similarity model for column mapping")
        except Exception as e:
            print(f"   Warning: Could not load semantic model: {e}")
            return None
    return _semantic_model


def calculate_string_similarity(str1, str2):
    """Calculate similarity between two strings (0-1)."""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


def normalize_for_semantics(text):
    """Normalize text by replacing semantic equivalents."""
    text = text.lower()
    text = text.replace('ship', 'origin')
    text = text.replace('cust', 'destination')
    text = text.replace('equipment type', 'cont_load')
    text = text.replace('equipmenttype', 'cont_load')
    text = text.replace('equipment', 'cont_load')
    # Postal code mappings
    text = text.replace('origin postal code', 'ship_post')
    text = text.replace('origin postal', 'ship_post')
    text = text.replace('destination postal code', 'cust_post')
    text = text.replace('destination postal', 'cust_post')
    text = text.replace('postal code', 'post')
    text = text.replace('zip code', 'post')
    text = text.replace('zip', 'post')
    # Country mappings
    text = text.replace('origin country', 'ship_country')
    text = text.replace('ship country', 'ship_country')
    text = text.replace('from country', 'ship_country')
    text = text.replace('destination country', 'cust_country')
    text = text.replace('cust country', 'cust_country')
    text = text.replace('to country', 'cust_country')
    # Flow Type / Category mappings
    text = text.replace('flow type', 'category')
    text = text.replace('flowtype', 'category')
    text = text.replace('flow_type', 'category')
    # Port / Seaport mappings
    text = text.replace('port of loading', 'origin airport')
    text = text.replace('port of entry', 'destination airport')
    return text


def find_semantic_match_llm(target_col, candidate_cols, threshold=0.3):
    """Find the best semantic match for a column name using LLM."""
    if not candidate_cols:
        return None, 0.0
    
    target_lower = target_col.lower().strip()
    target_normalized = normalize_for_semantics(target_col)
    
    # Direct postal code mappings
    postal_mappings = {
        'origin postal code': 'ship_post',
        'origin postal': 'ship_post',
        'originpostalcode': 'ship_post',
        'origin_postal_code': 'ship_post',
        'ship postal code': 'ship_post',
        'ship postal': 'ship_post',
        'shippostal': 'ship_post',
        'ship_postal': 'ship_post',
        'from postal code': 'ship_post',
        'from postal': 'ship_post',
        'origin zip': 'ship_post',
        'origin zip code': 'ship_post',
        'destination postal code': 'cust_post',
        'destination postal': 'cust_post',
        'destinationpostalcode': 'cust_post',
        'destination_postal_code': 'cust_post',
        'cust postal code': 'cust_post',
        'cust postal': 'cust_post',
        'custpostal': 'cust_post',
        'cust_postal': 'cust_post',
        'to postal code': 'cust_post',
        'to postal': 'cust_post',
        'destination zip': 'cust_post',
        'destination zip code': 'cust_post',
        'ship_post': 'ship_post',
        'cust_post': 'cust_post'
    }
    
    # Direct Flow Type / Category mappings
    flow_type_mappings = {
        'flow type': 'category',
        'flowtype': 'category',
        'flow_type': 'category'
    }
    
    # Direct Port / Seaport mappings
    port_mappings = {
        'port of loading': 'origin airport',
        'portofloading': 'origin airport',
        'port_of_loading': 'origin airport',
        'pol': 'origin airport',
        'port of entry': 'destination airport',
        'portofentry': 'destination airport',
        'port_of_entry': 'destination airport',
        'poe': 'destination airport'
    }
    
    # Direct Country mappings
    country_mappings = {
        'origin country': 'ship_country',
        'origincountry': 'ship_country',
        'origin_country': 'ship_country',
        'ship country': 'ship_country',
        'shipcountry': 'ship_country',
        'ship_country': 'ship_country',
        'from country': 'ship_country',
        'fromcountry': 'ship_country',
        'from_country': 'ship_country',
        'destination country': 'cust_country',
        'destinationcountry': 'cust_country',
        'destination_country': 'cust_country',
        'cust country': 'cust_country',
        'custcountry': 'cust_country',
        'cust_country': 'cust_country',
        'to country': 'cust_country',
        'tocountry': 'cust_country',
        'to_country': 'cust_country'
    }
    
    # Direct City mappings
    city_mappings = {
        'Origin City': 'ship_city',
        'origin city': 'ship_city',
        'origincity': 'ship_city',
        'origin_city': 'ship_city',
        'ship city': 'ship_city',
        'shipcity': 'ship_city',
        'ship_city': 'ship_city',
        'from city': 'ship_city',
        'fromcity': 'ship_city',
        'from_city': 'ship_city',
        'destination city': 'cust_city',
        'destinationcity': 'cust_city',
        'destination_city': 'cust_city',
        'cust city': 'cust_city',
        'custcity': 'cust_city',
        'cust_city': 'cust_city',
        'to city': 'cust_city',
        'tocity': 'cust_city',
        'to_city': 'cust_city'
    }
    
    # Direct State mappings
    state_mappings = {
        'Origin State': 'ship_state',
        'origin state': 'ship_state',
        'originstate': 'ship_state',
        'origin_state': 'ship_state',
        'ship state': 'ship_state',
        'shipstate': 'ship_state',
        'ship_state': 'ship_state',
        'from state': 'ship_state',
        'fromstate': 'ship_state',
        'from_state': 'ship_state',
        'Destination State': 'cust_state',
        'Destination state': 'cust_state',
        'destination state': 'cust_state',
        'destinationstate': 'cust_state',
        'destination_state': 'cust_state',
        'cust state': 'cust_state',
        'custstate': 'cust_state',
        'cust_state': 'cust_state',
        'to state': 'cust_state',
        'tostate': 'cust_state',
        'to_state': 'cust_state'
    }
    
    # Check direct postal code mappings first
    target_for_postal = target_lower.replace(' ', '')
    for postal_key, postal_value in postal_mappings.items():
        if postal_key in target_lower or postal_key.replace(' ', '') in target_for_postal:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if postal_value in cand_lower or cand_lower in postal_value:
                    return cand, 0.95
    
    # Check direct Flow Type / Category mappings
    target_for_flow = target_lower.replace(' ', '').replace('_', '')
    for flow_key, flow_value in flow_type_mappings.items():
        if flow_key in target_lower or flow_key.replace(' ', '').replace('_', '') in target_for_flow:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if flow_value in cand_lower or cand_lower == flow_value:
                    return cand, 0.95
    
    # Check direct Port / Seaport mappings
    target_for_port = target_lower.replace(' ', '').replace('_', '')
    for port_key, port_value in port_mappings.items():
        if port_key in target_lower or port_key.replace(' ', '').replace('_', '') in target_for_port:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if port_value in cand_lower or port_value.replace(' ', '') in cand_lower.replace(' ', ''):
                    return cand, 0.95
    
    # Check direct City mappings FIRST (before Country - "origin city" should not match "origin country")
    target_for_city = target_lower.replace(' ', '').replace('_', '')
    for city_key, city_value in city_mappings.items():
        city_key_lower = city_key.lower()
        city_key_normalized = city_key_lower.replace(' ', '').replace('_', '')
        if city_key_lower == target_lower or city_key_normalized == target_for_city:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if city_value in cand_lower or city_value.replace('_', '') in cand_lower.replace('_', ''):
                    return cand, 0.95
    
    # Check direct State mappings (Destination state -> CUST_STATE, Origin state -> SHIP_STATE)
    target_for_state = target_lower.replace(' ', '').replace('_', '')
    for state_key, state_value in state_mappings.items():
        state_key_lower = state_key.lower()
        state_key_normalized = state_key_lower.replace(' ', '').replace('_', '')
        if state_key_lower == target_lower or state_key_normalized == target_for_state:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if state_value in cand_lower or state_value.replace('_', '') in cand_lower.replace('_', ''):
                    return cand, 0.95
    
    # Check direct Country mappings
    target_for_country = target_lower.replace(' ', '').replace('_', '')
    for country_key, country_value in country_mappings.items():
        country_key_lower = country_key.lower()
        country_key_normalized = country_key_lower.replace(' ', '').replace('_', '')
        if country_key_lower == target_lower or country_key_normalized == target_for_country:
            for cand in candidate_cols:
                cand_lower = cand.lower().strip()
                if country_value in cand_lower or country_value.replace('_', '') in cand_lower.replace('_', ''):
                    return cand, 0.95
    
    # First try exact or very close matches
    for cand in candidate_cols:
        cand_lower = cand.lower().strip()
        cand_normalized = normalize_for_semantics(cand)
        
        if target_lower == cand_lower:
            return cand, 1.0
        
        if target_normalized == cand_normalized:
            return cand, 0.95
        
        if target_normalized in cand_normalized or cand_normalized in target_normalized:
            similarity = calculate_string_similarity(target_col, cand)
            if similarity > 0.7:
                return cand, similarity
    
    # Try semantic similarity if model is available
    model = get_semantic_model()
    if model is not None:
        try:
            enhanced_target = target_col.replace('SHIP', 'origin').replace('CUST', 'destination').replace('ship', 'origin').replace('cust', 'destination')
            enhanced_target = enhanced_target.replace('equipment type', 'cont_load').replace('equipmenttype', 'cont_load').replace('equipment', 'cont_load')
            enhanced_target = enhanced_target.replace('Origin postal code', 'SHIP_POST').replace('origin postal code', 'ship_post')
            enhanced_target = enhanced_target.replace('Destination postal code', 'CUST_POST').replace('destination postal code', 'cust_post')
            enhanced_target = enhanced_target.replace('SHIP_POST', 'ship_post').replace('CUST_POST', 'cust_post')
            # Flow Type / Category mapping
            enhanced_target = enhanced_target.replace('Flow Type', 'category').replace('flow type', 'category').replace('flow_type', 'category')
            
            enhanced_candidates = [c.replace('SHIP', 'origin').replace('CUST', 'destination').replace('ship', 'origin').replace('cust', 'destination')
                                 for c in candidate_cols]
            enhanced_candidates = [c.replace('equipment type', 'cont_load').replace('equipmenttype', 'cont_load').replace('equipment', 'cont_load')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('Origin postal code', 'SHIP_POST').replace('origin postal code', 'ship_post')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('Destination postal code', 'CUST_POST').replace('destination postal code', 'cust_post')
                                 for c in enhanced_candidates]
            enhanced_candidates = [c.replace('SHIP_POST', 'ship_post').replace('CUST_POST', 'cust_post')
                                 for c in enhanced_candidates]
            # Flow Type / Category mapping for candidates
            enhanced_candidates = [c.replace('CATEGORY', 'category').replace('Category', 'category')
                                 for c in enhanced_candidates]
            
            target_embedding = model.encode([enhanced_target])
            candidate_embeddings = model.encode(enhanced_candidates)
            similarities = cosine_similarity(target_embedding, candidate_embeddings)[0]
            
            best_idx = np.argmax(similarities)
            best_similarity = float(similarities[best_idx])
            
            if best_similarity >= threshold:
                return candidate_cols[best_idx], best_similarity
        except Exception as e:
            print(f"   Warning: Semantic matching failed: {e}, using fuzzy matching")
    
    # Fallback to fuzzy string matching
    best_match = None
    best_score = 0.0
    
    for cand in candidate_cols:
        similarity = calculate_string_similarity(target_col, cand)
        if similarity > best_score:
            best_score = similarity
            best_match = cand
    
    if best_score >= threshold:
        return best_match, best_score
    
    return None, best_score


def find_carrier_id_column(column_list):
    """Find the column that represents CARRIER ID."""
    carrier_keywords = ['carrier', 'carrier_id', 'carrier id']
    for col in column_list:
        col_lower = col.lower()
        for keyword in carrier_keywords:
            if keyword in col_lower:
                return col
    return None


def find_transport_mode_column(column_list):
    """Find the column that represents TRANSPORT MODE."""
    transport_keywords = ['transport', 'transport_mode', 'transport mode', 'mode']
    for col in column_list:
        col_lower = col.lower()
        for keyword in transport_keywords:
            if keyword in col_lower:
                return col
    return None



def check_custom_logic(carrier_id, shipper_id, transport_mode, custom_logic_dict):
    """
    Check if custom logic exists for the combination of carrier_id, shipper_id, transport_mode, and ship_port.
    
    Args:
        carrier_id: Carrier ID value
        shipper_id: Shipper ID value
        transport_mode: Transport mode value
        custom_logic_dict: Dictionary with keys as tuples (carrier_id, shipper_id, transport_mode, ship_port)
    
    Returns:
        Custom mapping if found, else None
    """
    if custom_logic_dict is None:
        return None
    
    # Try exact match
    key = (str(carrier_id), str(shipper_id), str(transport_mode))
    if key in custom_logic_dict:
        return custom_logic_dict[key]
    
    # Try partial matches (if some values are None/empty)
    for logic_key, logic_value in custom_logic_dict.items():
        match = True
        for i, val in enumerate(logic_key):
            if val and val != 'None' and val != '':
                if i == 0 and str(carrier_id) != val:
                    match = False
                    break
                elif i == 1 and str(shipper_id) != val:
                    match = False
                    break
                elif i == 2 and str(transport_mode) != val:
                    match = False
                    break
        if match:
            return logic_value
    
        return None


def is_date_column(column_name):
    """Check if column is related to SHIP_DATE."""
    date_keywords = ['date', 'ship_date', 'ship date', 'delivery_date', 'delivery date', 
                     'arrival_date', 'arrival date', 'invoice_date', 'invoice date']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in date_keywords)


def is_shipment_id_column(column_name):
    """Check if column is related to SHIPMENT_ID/delivery number/etof #/lc#."""
    shipment_keywords = ['shipment', 'shipment_id', 'shipment id', 'delivery', 'delivery number', 
                         'delivery_number', 'etof', 'etof #', 'etof#', 'lc', 'lc #', 'lc#', 
                         'order file', 'order_file', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)', 'delivery number(s)']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in shipment_keywords)


# CUSTOM LOGIC MAPPINGS
# Format: {(carrier_id, shipper_id, transport_mode, ship_port): {source_col: standard_col}}
# ship_port is the Origin Airport code (SHIP_PORT)
CUSTOM_LOGIC_MAPPINGS = {
    # Custom mapping for dairb: map LC column "SERVICE" to rate card column "Service"
    (None, 'dairb', None): {
        'SERVICE': 'Service'
    },
    # Example custom mappings - add your specific mappings here
    # ('CARRIER1', 'SHIPPER1', 'AIR', 'JFK'): {
    #     'Origin airport': 'Origin Airport Code',
    #     'Destination airport': 'Destination Airport Code'
    # },
    # Add more custom mappings as needed
}

# Columns to exclude from mapping
EXCLUDED_COLUMNS = [
    'ETOF #',
    'ETOF#',
    'LC #',
    'LC#',
    'Carrier',
    'Delivery Number',
    'DeliveryNumber',
    'Lane #',
    'DELIVERY_NUMBER',
    'DELIVERY NUMBER(s)',
    'SHIPMENT_ID',
    'Shipment ID',
    'ShipmentID',
    'shipment id',
    'shipmentid'
]

# Rate card columns that should not be mapped (kept as-is)
RATE_CARD_EXCLUDED_COLUMNS = [
    'Valid to',
    'Valid from',
    'Valid To',
    'Valid From'
]


def is_excluded_column(column_name):
    """Check if a column name should be excluded (case-insensitive, handles variations)."""
    if not column_name:
        return False
    
    col_lower = str(column_name).lower().strip()
    
    # Check against excluded columns (case-insensitive)
    for excluded in EXCLUDED_COLUMNS:
        excluded_lower = str(excluded).lower().strip()
        # Exact match
        if col_lower == excluded_lower:
            return True
        # Check if column contains excluded keyword (for variations like "ETOF #" vs "ETOF#")
        if excluded_lower.replace(' ', '') in col_lower.replace(' ', '') or col_lower.replace(' ', '') in excluded_lower.replace(' ', ''):
            # Additional check: make sure it's not just a partial match
            if 'etof' in excluded_lower and 'etof' in col_lower:
                return True
            if 'lc' in excluded_lower and 'lc' in col_lower and '#' in col_lower:
                return True
            if excluded_lower == 'carrier' and col_lower == 'carrier':
                return True
            if 'delivery' in excluded_lower and 'delivery' in col_lower and 'number' in col_lower:
                return True
    
    return False


def create_vocabulary_dataframe(
    rate_card_file_path: str,
    etof_file_path: Optional[str] = None,
    # origin_file_path: Optional[str] = None,  # COMMENTED OUT - origin file processing disabled
    lc_input_path: Optional[str] = None,
    shipper_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Create a vocabulary DataFrame mapping columns from all sources to standard names.
    
    Args:
        rate_card_file_path: Path to rate card file
        etof_file_path: Optional path to ETOF file
        # origin_file_path: Optional path to origin file  # COMMENTED OUT - origin file processing disabled
        lc_input_path: Optional path to LC input (file/folder/list)
        shipper_id: Optional shipper ID constant (used for custom logic matching)
    
    Returns:
        DataFrame with vocabulary mappings
        Columns: 
            - 'Source': Source of the column (ETOF, LC)
            - 'Source_Column': Original column name from source
            - 'Standard_Name': Standard column name (from rate card)
            - 'Mapping': Shows "Original_Column → Standard_Name" mapping
            - 'Mapping_Method': How it was mapped ('custom', 'LLM', 'fuzzy', 'keyword_match')
            - 'Confidence': Confidence score (0-1)
        Mapping_Method values: 'custom', 'LLM', 'fuzzy', 'keyword_match'
    """
    print("\n" + "="*80)
    print("CREATING VOCABULARY DATAFRAME")
    print("="*80)
    
    # Step 1: Get rate card columns (these are the standard names)
    print("\n1. Processing Rate Card...")
    try:
        rate_card_df, rate_card_columns, rate_card_conditions, _ = process_rate_card(rate_card_file_path)
        print(f"   Found {len(rate_card_columns)} rate card columns")
        
        # Filter out excluded columns from rate card (case-insensitive)
        excluded_found = [col for col in rate_card_columns if is_excluded_column(col)]
        rate_card_columns = [col for col in rate_card_columns if not is_excluded_column(col)]
        
        if excluded_found:
            print(f"   Excluded {len(excluded_found)} columns from mapping: {excluded_found}")
            print(f"   Remaining rate card columns for mapping: {len(rate_card_columns)}")
    except Exception as e:
        print(f"   Error processing rate card: {e}")
        return pd.DataFrame()
    
    # Step 2: Collect columns from all sources (excluding specified columns)
    all_source_columns = {}
    
    if etof_file_path:
        print("\n2. Processing ETOF file...")
        try:
            etof_df, etof_columns = process_etof_file(etof_file_path)
            # Filter out excluded columns (case-insensitive)
            excluded_etof = [col for col in etof_columns if is_excluded_column(col)]
            etof_columns = [col for col in etof_columns if not is_excluded_column(col)]
            all_source_columns['ETOF'] = etof_columns
            print(f"   Found {len(etof_columns)} ETOF columns (excluded {len(excluded_etof)}: {excluded_etof})")
        except Exception as e:
            print(f"   Error processing ETOF: {e}")
    
    # COMMENTED OUT - origin file processing disabled
    # if origin_file_path:
    #     print("\n3. Processing Origin file...")
    #     try:
    #         # Try to detect if it's an EDI file (doesn't need header_row)
    #         file_ext = os.path.splitext(origin_file_path)[1].lower()
    #         if file_ext == '.edi':
    #             origin_df, origin_columns = process_origin_file(origin_file_path, header_row=None, end_column=None)
    #         else:
    #             # For CSV/Excel, try with header_row=1 as default
    #             origin_df, origin_columns = process_origin_file(origin_file_path, header_row=1, end_column=None)
    #         
    #         # Custom logic for shipper "dairb": rename "SHAI Reference" to "SHIPMENT_ID"
    #         if shipper_id and shipper_id.lower() == 'dairb' and origin_df is not None and not origin_df.empty:
    #             if 'SHAI Reference' in origin_df.columns:
    #                 origin_df = origin_df.rename(columns={'SHAI Reference': 'SHIPMENT_ID'})
    #                 origin_columns = origin_df.columns.tolist()
    #         
    #         # Filter out excluded columns (case-insensitive)
    #         excluded_origin = [col for col in origin_columns if is_excluded_column(col)]
    #         origin_columns = [col for col in origin_columns if not is_excluded_column(col)]
    #         all_source_columns['Origin'] = origin_columns
    #         print(f"   Found {len(origin_columns)} origin columns (excluded {len(excluded_origin)}: {excluded_origin})")
    #     except Exception as e:
    #         print(f"   Error processing origin file: {e}")
    
    if lc_input_path and etof_file_path:
        print("\n4. Processing LC/ETOF files...")
        try:
            # process_lc_etof_mapping uses SHIPMENT_ID or DELIVERY_NUMBER mapping
            lc_df, lc_columns = process_lc_etof_mapping(lc_input_path, etof_file_path)
            # Filter out excluded columns (case-insensitive)
            excluded_lc = [col for col in lc_columns if is_excluded_column(col)]
            lc_columns = [col for col in lc_columns if not is_excluded_column(col)]
            all_source_columns['LC'] = lc_columns
            print(f"   Found {len(lc_columns)} LC columns (excluded {len(excluded_lc)}: {excluded_lc})")
        except Exception as e:
            print(f"   Error processing LC files: {e}")
    
    # Step 3: Print all columns explored from each source
    print("\n" + "="*80)
    print("COLUMNS EXPLORED FROM EACH SOURCE")
    print("="*80)
    print(f"\nRate Card ({len(rate_card_columns)} columns):")
    for i, col in enumerate(rate_card_columns, 1):
        print(f"  {i}. {col}")
    
    for source_name, source_columns in all_source_columns.items():
        print(f"\n{source_name} ({len(source_columns)} columns):")
        for i, col in enumerate(source_columns, 1):
            print(f"  {i}. {col}")
    
    # Step 4: Find CARRIER_ID and TRANSPORT_MODE columns for custom logic
    carrier_id_col = None
    transport_mode_col = None
    
    # Try to find these columns in rate card first
    carrier_id_col = find_carrier_id_column(rate_card_columns)
    transport_mode_col = find_transport_mode_column(rate_card_columns)
    
    # Step 5: Create vocabulary mappings (one-to-one mapping)
    print("\n" + "="*80)
    print("CREATING VOCABULARY MAPPINGS (ONE-TO-ONE)")
    print("="*80)
    vocabulary_data = []
    
    # Track which source columns have been used (for one-to-one mapping)
    # Format: {source_name: set of used source columns}
    used_source_columns = {source_name: set() for source_name in all_source_columns.keys()}
    
    # Check if we have custom logic mappings
    has_custom_logic = len(CUSTOM_LOGIC_MAPPINGS) > 0
    if has_custom_logic:
        print(f"   Found {len(CUSTOM_LOGIC_MAPPINGS)} custom logic mapping(s)")
    
    # For each rate card column (standard name), find ONE match per source
    for standard_col in rate_card_columns:
        # Map to each source (one-to-one: one rate card column -> one source column per source)
        for source_name, source_columns in all_source_columns.items():
            # Skip if this rate card column already has a mapping for this source
            existing_mapping = [item for item in vocabulary_data 
                               if item['Standard_Name'] == standard_col and item['Source'] == source_name]
            if existing_mapping:
                continue  # Already mapped for this source
            
            # Get available source columns (not yet used)
            available_columns = [col for col in source_columns if col not in used_source_columns[source_name]]
            
            if not available_columns:
                continue  # No available columns in this source
            
            # Check custom logic first if available
            custom_mapping_found = False
            if has_custom_logic and shipper_id:
                # Check all custom logic entries for this standard column
                for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                    # Check if shipper_id matches (if specified in custom logic)
                    if shipper_id_key and shipper_id_key != shipper_id:
                        continue
                    
                    # Check if this standard column has a custom mapping
                    if standard_col in mapping_dict.values():
                        # Find the source column that maps to this standard column
                        for source_col, mapped_standard in mapping_dict.items():
                            if mapped_standard == standard_col and source_col in available_columns:
                                # Double-check: skip if either column is excluded
                                if is_excluded_column(standard_col) or is_excluded_column(source_col):
                                    continue
                                vocabulary_data.append({
                                    'Standard_Name': standard_col,
                                    'Source': source_name,
                                    'Source_Column': source_col,
                                    'Mapping_Method': 'custom',
                                    'Confidence': 1.0
                                })
                                used_source_columns[source_name].add(source_col)
                                custom_mapping_found = True
                                break
                    
                    if custom_mapping_found:
                        break
                
                if custom_mapping_found:
                    continue
            
            # Use LLM/semantic matching if no custom mapping found
            match, confidence = find_semantic_match_llm(standard_col, available_columns, threshold=0.3)
            if match:
                # Double-check: skip if either column is excluded
                if is_excluded_column(standard_col) or is_excluded_column(match):
                    continue
                method = 'LLM' if SEMANTIC_AVAILABLE else 'fuzzy'
                vocabulary_data.append({
                    'Standard_Name': standard_col,
                    'Source': source_name,
                    'Source_Column': match,
                    'Mapping_Method': method,
                    'Confidence': confidence
                })
                used_source_columns[source_name].add(match)
    
    # Step 6: Create DataFrame and identify unmapped columns
    print("\nCreating vocabulary DataFrame...")
    
    # Create DataFrame
    df_vocabulary = pd.DataFrame(vocabulary_data)
    
    if not df_vocabulary.empty:
        # Add a mapping column that shows Original → Standard clearly
        df_vocabulary['Mapping'] = df_vocabulary['Source_Column'] + ' → ' + df_vocabulary['Standard_Name']
        
        # Reorder columns to make it clearer: show original name, then what it maps to
        column_order = ['Source', 'Source_Column', 'Standard_Name', 'Mapping', 'Mapping_Method', 'Confidence']
        df_vocabulary = df_vocabulary[column_order]
        
        # Sort by Source, then Standard_Name
        df_vocabulary = df_vocabulary.sort_values(['Source', 'Standard_Name'])
    
    # Step 7: Identify and print unmapped columns
    print("\n" + "="*80)
    print("UNMAPPED COLUMNS ANALYSIS")
    print("="*80)
    
    # Find unmapped rate card columns
    if not df_vocabulary.empty:
        mapped_rate_cols = set(df_vocabulary['Standard_Name'].unique())
    else:
        mapped_rate_cols = set()
    
    unmapped_rate_cols = set(rate_card_columns) - mapped_rate_cols
    
    print(f"\nRate Card Columns:")
    print(f"  Total: {len(rate_card_columns)}")
    print(f"  Mapped: {len(mapped_rate_cols)}")
    print(f"  Unmapped: {len(unmapped_rate_cols)}")
    if unmapped_rate_cols:
        print(f"\n  Unmapped Rate Card Columns:")
        for col in sorted(unmapped_rate_cols):
            print(f"    - {col}")
    
    # Find unmapped source columns (columns that could have matched but didn't due to one-to-one constraint)
    print(f"\nSource Files Columns:")
    for source_name, source_columns in all_source_columns.items():
        used_cols = used_source_columns.get(source_name, set())
        unmapped_source_cols = set(source_columns) - used_cols
        print(f"\n  {source_name}:")
        print(f"    Total: {len(source_columns)}")
        print(f"    Mapped: {len(used_cols)}")
        print(f"    Unmapped: {len(unmapped_source_cols)}")
        if unmapped_source_cols:
            print(f"    Unmapped {source_name} Columns:")
            for col in sorted(unmapped_source_cols):
                print(f"      - {col}")
    
    print(f"\n   Created vocabulary with {len(df_vocabulary)} mappings")
    print(f"   Rate card columns mapped: {len(mapped_rate_cols)} out of {len(rate_card_columns)}")
    if not df_vocabulary.empty:
        print(f"   Sources: {df_vocabulary['Source'].unique().tolist()}")
    
    # Show mapping method breakdown
    if not df_vocabulary.empty:
        method_counts = df_vocabulary['Mapping_Method'].value_counts()
        print(f"\n   Mapping methods:")
        for method, count in method_counts.items():
            print(f"     {method}: {count}")
    
    return df_vocabulary


def map_and_rename_columns(
    rate_card_file_path: str,
    etof_file_path: Optional[str] = None,
    # origin_file_path: Optional[str] = None,  # COMMENTED OUT - origin file processing disabled
    # origin_header_row: Optional[int] = None,  # COMMENTED OUT - origin file processing disabled
    # origin_end_column: Optional[int] = None,  # COMMENTED OUT - origin file processing disabled
    lc_input_path: Optional[str] = None,
    output_txt_path: str = "column_mapping_results.txt",
    ignore_rate_card_columns: Optional[List[str]] = None,
    shipper_id: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Map rate card columns to ETOF and LC files, rename columns, and save results.
    
    Args:
        rate_card_file_path: Path to rate card file
        etof_file_path: Optional path to ETOF file
        # origin_file_path: Optional path to origin file  # COMMENTED OUT - origin file processing disabled
        # origin_header_row: Optional header row for origin file (required for CSV/Excel)  # COMMENTED OUT
        # origin_end_column: Optional end column for origin file  # COMMENTED OUT
        lc_input_path: Optional path to LC input (file/folder/list)
        output_txt_path: Path to save the mapping results text file
        ignore_rate_card_columns: Optional list of rate card column names to ignore/delete from processing
        shipper_id: Optional shipper ID for custom logic (e.g., "dairb")
    
    Returns:
        Tuple: (etof_dataframe_renamed, lc_dataframe_renamed, None)
    """
    # Step 1: Get rate card columns
    try:
        print(f"\nStep 1: Processing rate card file: {rate_card_file_path}")
        
        # Check if file exists in input folder (process_rate_card expects files in "input" folder)
        import os
        input_folder = "input"
        expected_path = os.path.join(input_folder, rate_card_file_path)
        
        # Check if input folder exists
        if not os.path.exists(input_folder):
            print(f"   WARNING: '{input_folder}' folder does not exist. Creating it...")
            os.makedirs(input_folder, exist_ok=True)
        
        # Check if file exists in input folder
        if not os.path.exists(expected_path):
            # Try with just the filename
            filename = os.path.basename(rate_card_file_path)
            alt_path = os.path.join(input_folder, filename)
            if os.path.exists(alt_path):
                rate_card_file_path = filename
                print(f"   Using file: {alt_path}")
            else:
                error_msg = f"Rate card file not found at: {expected_path}"
                if os.path.exists(rate_card_file_path):
                    error_msg += f"\n   Found file at current location: {rate_card_file_path}"
                    error_msg += f"\n   Please move it to: {expected_path}"
                else:
                    error_msg += f"\n   Please ensure the file exists in the '{input_folder}' folder."
                raise FileNotFoundError(error_msg)
        else:
            print(f"   Found rate card at: {expected_path}")
        
        rate_card_df, rate_card_columns_all, rate_card_conditions, _ = process_rate_card(rate_card_file_path)
        print(f"   Successfully loaded rate card: {len(rate_card_columns_all)} columns")
        
        # Filter out ignored columns
        if ignore_rate_card_columns is None:
            ignore_rate_card_columns = []
        
        # Remove ignored columns from rate card dataframe
        if ignore_rate_card_columns:
            columns_to_drop = [col for col in ignore_rate_card_columns if col in rate_card_df.columns]
            if columns_to_drop:
                rate_card_df = rate_card_df.drop(columns=columns_to_drop)
        
        # Update rate_card_columns_all to exclude ignored columns
        rate_card_columns_all = [col for col in rate_card_columns_all if col not in ignore_rate_card_columns]
        
        rate_card_columns_to_map = [
            col for col in rate_card_columns_all 
            if not is_excluded_column(col) and col not in RATE_CARD_EXCLUDED_COLUMNS
        ]
        rate_card_columns = rate_card_columns_to_map
        print(f"   Rate card columns to map: {len(rate_card_columns)}")
    except Exception as e:
        print(f"   ERROR processing rate card: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Get ETOF and LC dataframes
    etof_df = None
    lc_df = None
    # origin_df = None  # COMMENTED OUT - origin file processing disabled
    
    if etof_file_path:
        try:
            print(f"\nStep 2a: Processing ETOF file: {etof_file_path}")
            etof_df, etof_columns = process_etof_file(etof_file_path)
            print(f"   Successfully loaded ETOF: {len(etof_columns)} columns, {len(etof_df)} rows")
        except Exception as e:
            print(f"   ERROR processing ETOF: {e}")
            import traceback
            traceback.print_exc()
            etof_df = None
    
    # COMMENTED OUT - origin file processing disabled
    # if origin_file_path:
    #     try:
    #         file_ext = os.path.splitext(origin_file_path)[1].lower()
    #         if file_ext == '.edi':
    #             origin_df, origin_columns = process_origin_file(origin_file_path, header_row=None, end_column=origin_end_column)
    #         else:
    #             if origin_header_row is None:
    #                 origin_header_row = 1
    #             origin_df, origin_columns = process_origin_file(origin_file_path, header_row=origin_header_row, end_column=origin_end_column)
    #         
    #         # Custom logic for shipper "dairb": rename "SHAI Reference" to "SHIPMENT_ID"
    #         if shipper_id and shipper_id.lower() == 'dairb' and origin_df is not None and not origin_df.empty:
    #             if 'SHAI Reference' in origin_df.columns:
    #                 origin_df = origin_df.rename(columns={'SHAI Reference': 'SHIPMENT_ID'})
    #                 origin_columns = origin_df.columns.tolist()
    #     except Exception:
    #         pass
    
    if lc_input_path and etof_file_path:
        try:
            print(f"\nStep 2b: Processing LC file: {lc_input_path}")
            # process_lc_etof_mapping uses SHIPMENT_ID or DELIVERY_NUMBER mapping
            lc_df, lc_columns = process_lc_etof_mapping(lc_input_path, etof_file_path)
            print(f"   Successfully loaded LC: {len(lc_columns)} columns, {len(lc_df)} rows")
        except Exception as e:
            print(f"   ERROR processing LC: {e}")
            import traceback
            traceback.print_exc()
            lc_df = None
    
    # Step 3: Find mappings for each rate card column
    print(f"\nStep 3: Checking dataframes...")
    print(f"   ETOF df: {'Exists' if etof_df is not None and not etof_df.empty else 'None/Empty'}")
    print(f"   LC df: {'Exists' if lc_df is not None and not lc_df.empty else 'None/Empty'}")
    # print(f"   Origin df: {'Exists' if origin_df is not None and not origin_df.empty else 'None/Empty'}")  # COMMENTED OUT
    
    if etof_df is None and lc_df is None:
        print("   ERROR: All dataframes are None/Empty. Returning empty dataframes.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Columns to always keep (even if not in rate card)
    keep_columns = ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Delivery Number', 'DeliveryNumber', 
                   'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 'SHIPMENT_ID', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)']
    
    # Mapping results
    mapping_results = []
    etof_mappings = {}  # {rate_card_col: etof_col}
    lc_mappings = {}    # {rate_card_col: lc_col}
    # origin_mappings = {}  # {rate_card_col: origin_col}  # COMMENTED OUT - origin file processing disabled
    
    # Track which source columns have been used (cannot be reused)
    used_etof_columns = set()
    used_lc_columns = set()
    # used_origin_columns = set()  # COMMENTED OUT - origin file processing disabled
    
    for rate_card_col in rate_card_columns:
        etof_match = None
        lc_match = None
        origin_match = None
        
        # Find match in ETOF (only if column hasn't been used yet)
        if etof_df is not None and not etof_df.empty:
            etof_columns = [col for col in etof_df.columns 
                          if not is_excluded_column(col) and col not in used_etof_columns]
            if etof_columns:
                match, confidence = find_semantic_match_llm(rate_card_col, etof_columns, threshold=0.3)
                if match and not is_excluded_column(match) and match not in used_etof_columns:
                    etof_match = match
                    etof_mappings[rate_card_col] = match
                    used_etof_columns.add(match)
        
        # Find match in LC (only if column hasn't been used yet)
        if lc_df is not None and not lc_df.empty:
            lc_columns = [col for col in lc_df.columns 
                        if not is_excluded_column(col) and col not in used_lc_columns]
            if lc_columns:
                # Check custom logic first if available
                custom_match_found = False
                if shipper_id and len(CUSTOM_LOGIC_MAPPINGS) > 0:
                    for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                        # Check if shipper_id matches
                        if shipper_id_key and shipper_id_key == shipper_id:
                            # Check if this rate card column has a custom mapping
                            if rate_card_col in mapping_dict.values():
                                # Find the source column that maps to this rate card column
                                for source_col, mapped_standard in mapping_dict.items():
                                    if mapped_standard == rate_card_col and source_col in lc_columns:
                                        if not is_excluded_column(source_col) and source_col not in used_lc_columns:
                                            lc_match = source_col
                                            lc_mappings[rate_card_col] = source_col
                                            used_lc_columns.add(source_col)
                                            custom_match_found = True
                                            break
                        if custom_match_found:
                            break
                
                # Use semantic matching if no custom mapping found
                if not custom_match_found:
                    match, confidence = find_semantic_match_llm(rate_card_col, lc_columns, threshold=0.3)
                    if match and not is_excluded_column(match) and match not in used_lc_columns:
                        lc_match = match
                        lc_mappings[rate_card_col] = match
                        used_lc_columns.add(match)
        
        # COMMENTED OUT - origin file processing disabled
        # # Find match in Origin (only if column hasn't been used yet)
        # if origin_df is not None and not origin_df.empty:
        #     origin_columns = [col for col in origin_df.columns 
        #                     if not is_excluded_column(col) and col not in used_origin_columns]
        #     if origin_columns:
        #         match, confidence = find_semantic_match_llm(rate_card_col, origin_columns, threshold=0.3)
        #         if match and not is_excluded_column(match) and match not in used_origin_columns:
        #             origin_match = match
        #             origin_mappings[rate_card_col] = match
        #             used_origin_columns.add(match)
        
        mapping_results.append({
            'Rate_Card_Column': rate_card_col,
            'ETOF_Column': etof_match if etof_match else 'NONE',
            'LC_Column': lc_match if lc_match else 'NONE',
            # 'Origin_Column': origin_match if origin_match else 'NONE'  # COMMENTED OUT
        })
    
    # Step 4: Rename columns and include ALL rate card columns
    all_rate_card_cols_for_output = rate_card_columns_all.copy()
    
    etof_df_renamed = None
    lc_df_renamed = None
    # origin_df_renamed = None  # COMMENTED OUT - origin file processing disabled
    
    def create_output_dataframe(source_df, source_mappings, source_name, keep_cols_list, specific_keep_list, all_rate_card_cols):
        """Helper function to create output dataframe with rate card columns and key columns only."""
        if source_df is None or source_df.empty:
            return None

        output_df = source_df.copy()
        rename_dict = {}
        columns_to_keep = []
        
        # Step 1: Add rate card mapped columns (will be renamed to "RateCardColumn (OriginalColumn)")
        print(f"\n  [{source_name}] Step 1: Adding rate card mapped columns...")
        for rate_card_col, source_col in source_mappings.items():
            if source_col in output_df.columns:
                rename_dict[source_col] = f"{rate_card_col} ({source_col})"
                columns_to_keep.append(source_col)
        print(f"    After Step 1: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 2: Add columns to always keep (ETOF #, LC #, Carrier, Delivery Number)
        print(f"\n  [{source_name}] Step 2: Adding columns to always keep...")
        for keep_col in keep_cols_list:
            # Try to find the column (case-insensitive and handle variations)
            found = False
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                keep_normalized = keep_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == keep_normalized:
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    found = True
                    break
            if not found:
                # Also check if the column name itself matches (exact match)
                if keep_col in output_df.columns and keep_col not in columns_to_keep:
                    columns_to_keep.append(keep_col)
        print(f"    After Step 2: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns to keep so far: {columns_to_keep}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 3: Add source-specific columns to keep (Loading date for ETOF, SHIP_DATE for LC)
        print(f"\n  [{source_name}] Step 3: Adding source-specific columns to keep...")
        for keep_col in specific_keep_list:
            # Try to find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == keep_col.lower():
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    break
        print(f"    After Step 3: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns to keep so far: {columns_to_keep}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 4: Rename columns first (before filtering)
        print(f"\n  [{source_name}] Step 4: Renaming columns...")
        output_df.rename(columns=rename_dict, inplace=True)
        print(f"    After Step 4: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 5: Now rename "RateCardColumn (OriginalColumn)" to just "RateCardColumn"
        print(f"\n  [{source_name}] Step 5: Renaming to standard column names...")
        rename_to_standard = {}
        for col in output_df.columns:
            if ' (' in col and col.endswith(')'):
                standard_name = col.split(' (')[0]
                # Only rename if it's a rate card column
                if standard_name in all_rate_card_cols:
                    rename_to_standard[col] = standard_name
        
        if rename_to_standard:
            output_df.rename(columns=rename_to_standard, inplace=True)
            # Update columns_to_keep list with renamed columns
            updated_columns_to_keep = []
            for col in columns_to_keep:
                if col in rename_to_standard:
                    updated_columns_to_keep.append(rename_to_standard[col])
                elif col in output_df.columns:
                    updated_columns_to_keep.append(col)
            columns_to_keep = updated_columns_to_keep
        print(f"    After Step 5: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 6: Add ALL rate card columns that are not yet in the dataframe (as empty columns)
        # Only add columns that don't have a mapping (were not mapped from this source)
        print(f"\n  [{source_name}] Step 6: Adding missing rate card columns as empty...")
        for rate_card_col in all_rate_card_cols:
            # Skip if this column was excluded from mapping
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Check if this column is already in the dataframe (was mapped)
            if rate_card_col not in output_df.columns:
                # Check if this rate card column has a mapping from this source
                # If it does, we should have already added it, so skip
                # If it doesn't, add it as empty
                if rate_card_col not in source_mappings:
                    # No mapping found - add as empty column
                    output_df[rate_card_col] = None
                    if rate_card_col not in columns_to_keep:
                        columns_to_keep.append(rate_card_col)
        print(f"    After Step 6: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 7: Build final column list - ONLY rate card columns + key columns (LC #, ETOF #, Carrier, Loading date/SHIP_DATE)
        print(f"\n  [{source_name}] Step 7: Building final column list...")
        final_columns = []
        
        # Add all rate card columns first (mapped or unmapped)
        for rate_card_col in all_rate_card_cols:
            # Skip excluded columns
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Add rate card column if it exists
            if rate_card_col in output_df.columns:
                final_columns.append(rate_card_col)
        
        # Add key columns: LC #, ETOF #, Carrier, Delivery Number, Shipment ID
        key_columns_to_find = ['ETOF #','LC #', 'carrier', 'carrier_name', 'shipment_id',
                              'delivery_number', 'deliverynumber(s)',]
                              #'SHIPMENT_ID', 'DELIVERY_NUMBER','DELIVERY NUMBER(s)', 'delivery_number', 'delivery number(s)', 'deliverynumber', 'deliverynumber(s)']
        for key_col in key_columns_to_find:
            # Find the column (case-insensitive, handle variations)
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                key_normalized = key_col.lower().replace(' ', '').replace('#', '#')
                result = (col_normalized == key_normalized)
                print(f"Comparing '{col}' to '{key_col}': {col_normalized} == {key_normalized} -> {result}")
                if result:
                    if col not in final_columns:
                        final_columns.append(col)
                        print(f"Added '{col}' to final_columns")
                    break
        
        # Add source-specific columns: Loading date (ETOF) or SHIP_DATE (LC)
        for specific_col in specific_keep_list:
            # Find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == specific_col.lower():
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        print(f"    After Step 7: Final columns list: {final_columns}")
        
        # Step 8: Filter to keep ONLY the final columns
        print(f"\n  [{source_name}] Step 8: Filtering to final columns...")
        output_df = output_df[final_columns]
        print(f"    After Step 8: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 9: Ensure Carrier column exists (add if not present)
        print(f"\n  [{source_name}] Step 9: Ensuring required columns exist (Carrier, Delivery Number, Shipment ID)...")
        carrier_col_found = False
        carrier_variations = ['Carrier', 'carrier_name', 'CARRIER']
        
        for col in output_df.columns:
            if str(col).strip() in carrier_variations:
                carrier_col_found = True
                break
        
        if not carrier_col_found:
            output_df['Carrier'] = None
            final_columns.append('Carrier')
        
        delivery_col_found = False
        delivery_variations = ['Delivery Number', 'DeliveryNumber', 'delivery number', 'deliverynumber', 
                              'Delivery', 'delivery', 'DELIVERY', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)','delivery_number', 'delivery_number', 'delivery number(s)', 'deliverynumber(s)']
        
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            for variation in delivery_variations:
                var_lower = variation.lower().replace(' ', '').replace('_', '')
                if col_lower == var_lower or ('delivery' in col_lower and 'number' in col_lower):
                    delivery_col_found = True
                    break
            if delivery_col_found:
                break
        
        if not delivery_col_found:
            output_df['Delivery Number'] = None
            final_columns.append('Delivery Number')
        
        shipment_id_col_found = False
        shipment_id_variations = ['Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 
                                 'SHIPMENT_ID', 'SHIPMENT ID', 'Shipment', 'shipment', 'SHIPMENT']
        
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            for variation in shipment_id_variations:
                var_lower = variation.lower().replace(' ', '').replace('_', '')
                if col_lower == var_lower or ('shipment' in col_lower and 'id' in col_lower):
                    shipment_id_col_found = True
                    break
            if shipment_id_col_found:
                break
        
        if not shipment_id_col_found:
            output_df['Shipment ID'] = None
            final_columns.append('Shipment ID')
        
        print(f"    After Step 9: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Final columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 10: Ensure geographic columns exist (fallback - rename variations or add empty if not mapped)
        # These should already be mapped via semantic matching, but this ensures standard naming
        print(f"\n  [{source_name}] Step 10: Standardizing geographic columns (Country, Postal Code)...")
        
        geo_columns_mapping = {
            'Origin Country': ['Origin Country', 'origin country', 'OriginCountry', 'ORIGIN_COUNTRY', 
                              'Ship Country', 'ship country', 'ShipCountry', 'SHIP_COUNTRY',
                              'From Country', 'from country', 'FromCountry', 'FROM_COUNTRY'],
            'Origin Postal Code': ['Origin Postal Code', 'origin postal code', 'OriginPostalCode', 'ORIGIN_POSTAL_CODE',
                                   'Ship Postal', 'ship postal', 'ShipPostal', 'SHIP_POSTAL', 'SHIP_POST',
                                   'From Postal', 'from postal', 'FromPostal', 'FROM_POSTAL',
                                   'Origin Zip', 'origin zip', 'OriginZip', 'ORIGIN_ZIP'],
            'Destination Country': ['Destination Country', 'destination country', 'DestinationCountry', 'DESTINATION_COUNTRY',
                                   'Cust Country', 'cust country', 'CustCountry', 'CUST_COUNTRY',
                                   'To Country', 'to country', 'ToCountry', 'TO_COUNTRY'],
            'Destination Postal Code': ['Destination Postal Code', 'destination postal code', 'DestinationPostalCode', 'DESTINATION_POSTAL_CODE',
                                        'Cust Postal', 'cust postal', 'CustPostal', 'CUST_POSTAL', 'CUST_POST',
                                        'To Postal', 'to postal', 'ToPostal', 'TO_POSTAL',
                                        'Destination Zip', 'destination zip', 'DestinationZip', 'DESTINATION_ZIP']
        }
        
        for standard_geo_col, variations in geo_columns_mapping.items():
            geo_col_found = False
            found_col_name = None
            
            for col in output_df.columns:
                col_str = str(col).strip()
                col_lower = col_str.lower().replace(' ', '').replace('_', '')
                
                for variation in variations:
                    var_lower = variation.lower().replace(' ', '').replace('_', '')
                    if col_lower == var_lower:
                        geo_col_found = True
                        found_col_name = col
                        break
                if geo_col_found:
                    break
            
            if geo_col_found and found_col_name != standard_geo_col:
                # Rename to standard name
                output_df = output_df.rename(columns={found_col_name: standard_geo_col})
                print(f"    Renamed '{found_col_name}' -> '{standard_geo_col}'")
            elif not geo_col_found:
                # Add empty column
                output_df[standard_geo_col] = None
                final_columns.append(standard_geo_col)
                print(f"    Added empty column: '{standard_geo_col}'")
        
        print(f"    After Step 10: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Final columns: {list(output_df.columns)}")
        
        return output_df
    
    # Process ETOF
    if etof_df is not None:
        etof_specific_keep = ['Loading date', 'Loading Date', 'loading date', 'LOADING DATE', 
                              'Carrier agreement #', 'Carrier Agreement #', 'carrier agreement #']
        etof_df_renamed = create_output_dataframe(
            etof_df, etof_mappings, 'ETOF', keep_columns, etof_specific_keep, all_rate_card_cols_for_output
        )
        print(f"\nStep 4a: After creating ETOF output dataframe")
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
            print(f"   Columns: {list(etof_df_renamed.columns)}")
            print(f"   First few rows:\n{etof_df_renamed.head(3).to_string()}")
        else:
            print(f"   ETOF DataFrame: Empty or None")
    
    # Process LC
    if lc_df is not None:
        lc_specific_keep = ['SHIP_DATE', 'ship_date', 'Ship Date', 'ship date', 'SHIP DATE',
                            'Carrier agreement #', 'Carrier Agreement #', 'carrier agreement #']
        lc_df_renamed = create_output_dataframe(
            lc_df, lc_mappings, 'LC', keep_columns, lc_specific_keep, all_rate_card_cols_for_output
        )
        print(f"\nStep 4b: After creating LC output dataframe")
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            print(f"   LC DataFrame: {len(lc_df_renamed)} rows, {len(lc_df_renamed.columns)} columns")
            print(f"   Columns: {list(lc_df_renamed.columns)}")
            print(f"   First few rows:\n{lc_df_renamed.head(3).to_string()}")
        else:
            print(f"   LC DataFrame: Empty or None")
    
    # COMMENTED OUT - origin file processing disabled
    # # Process Origin
    # if origin_df is not None:
    #     origin_specific_keep = []  # No specific columns for origin
    #     origin_df_renamed = create_output_dataframe(
    #         origin_df, origin_mappings, 'Origin', keep_columns, origin_specific_keep, all_rate_card_cols_for_output
    #     )
    #     print(f"\nStep 4c: After creating Origin output dataframe")
    #     if origin_df_renamed is not None and not origin_df_renamed.empty:
    #         print(f"   Origin DataFrame: {len(origin_df_renamed)} rows, {len(origin_df_renamed.columns)} columns")
    #         print(f"   Columns: {list(origin_df_renamed.columns)}")
    #         print(f"   First few rows:\n{origin_df_renamed.head(3).to_string()}")
    #     else:
    #         print(f"   Origin DataFrame: Empty or None")
    
    # Step 4.5: Fill LC Carrier column from ETOF Carrier ID
    if lc_df_renamed is not None and etof_df_renamed is not None:
        print("\n4.5. Filling LC Carrier column from ETOF Carrier ID...")
        
        # Find ETOF # column in both dataframes
        lc_etof_col = None
        etof_etof_col = None
        
        # Find ETOF # in LC dataframe
        etof_patterns = ['ETOF #', 'ETOF#', 'etof #', 'etof#']
        for col in lc_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in etof_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    lc_etof_col = col
                    break
            if lc_etof_col:
                break
        
        # Find ETOF # in ETOF dataframe
        for col in etof_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in etof_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    etof_etof_col = col
                    break
            if etof_etof_col:
                break
        
        # Find Carrier ID in ETOF dataframe
        etof_carrier_col = None
        carrier_patterns = ['Carrier', 'carrier', 'CARRIER', 'Carrier ID', 'CarrierID', 'carrier id', 'carrierid']
        for col in etof_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in carrier_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    etof_carrier_col = col
                    break
            if etof_carrier_col:
                break
        
        # Find Carrier column in LC dataframe
        lc_carrier_col = None
        for col in lc_df_renamed.columns:
            col_str = str(col).strip()
            if col_str.lower() == 'carrier':
                lc_carrier_col = col
                break
        
        if lc_etof_col and etof_etof_col and etof_carrier_col and lc_carrier_col:
            etof_mapping = {}
            for idx, row in etof_df_renamed.iterrows():
                etof_num = row.get(etof_etof_col)
                carrier_id = row.get(etof_carrier_col)
                
                if pd.notna(etof_num) and pd.notna(carrier_id):
                    etof_num_str = str(etof_num).strip()
                    carrier_id_str = str(carrier_id).strip()
                    if etof_num_str and carrier_id_str:
                        etof_mapping[etof_num_str] = carrier_id_str
            
            for idx, row in lc_df_renamed.iterrows():
                lc_etof_num = row.get(lc_etof_col)
                
                if pd.notna(lc_etof_num):
                    lc_etof_num_str = str(lc_etof_num).strip()
                    if lc_etof_num_str in etof_mapping:
                        lc_df_renamed.at[idx, lc_carrier_col] = etof_mapping[lc_etof_num_str]
            
            # Show statistics
            total_lc_rows = len(lc_df_renamed)
            lc_rows_with_etof = len(lc_df_renamed[lc_df_renamed[lc_etof_col].notna()])
            lc_rows_with_carrier = len(lc_df_renamed[lc_df_renamed[lc_carrier_col].notna()])
            
            print(f"   LC statistics:")
            print(f"     Total rows: {total_lc_rows}")
            print(f"     Rows with ETOF #: {lc_rows_with_etof}")
            print(f"     Rows with Carrier (after fill): {lc_rows_with_carrier}")
        else:
            missing_cols = []
            if not lc_etof_col:
                missing_cols.append("LC ETOF #")
            if not etof_etof_col:
                missing_cols.append("ETOF ETOF #")
            if not etof_carrier_col:
                missing_cols.append("ETOF Carrier ID")
            if not lc_carrier_col:
                missing_cols.append("LC Carrier")
        
        # Step 4.6: Fill LC 'Carrier agreement #' from ETOF
        print("\n4.6. Filling LC 'Carrier agreement #' from ETOF...")
        
        # Find 'Carrier agreement #' in ETOF dataframe
        etof_agreement_col = None
        agreement_patterns = ['Carrier agreement #', 'Carrier Agreement #', 'carrier agreement #', 'CARRIER AGREEMENT #']
        for col in etof_df_renamed.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '')
            for pattern in agreement_patterns:
                if col_lower == pattern.lower().replace(' ', '') or col_str == pattern:
                    etof_agreement_col = col
                    break
            if etof_agreement_col:
                break
        
        if lc_etof_col and etof_etof_col and etof_agreement_col:
            # Add 'Carrier agreement #' column to LC if not exists
            if 'Carrier agreement #' not in lc_df_renamed.columns:
                lc_df_renamed['Carrier agreement #'] = None
            
            # Create mapping from ETOF # to Carrier agreement #
            etof_agreement_mapping = {}
            for idx, row in etof_df_renamed.iterrows():
                etof_num = row.get(etof_etof_col)
                agreement = row.get(etof_agreement_col)
                
                if pd.notna(etof_num) and pd.notna(agreement):
                    etof_num_str = str(etof_num).strip()
                    agreement_str = str(agreement).strip()
                    if etof_num_str and agreement_str:
                        etof_agreement_mapping[etof_num_str] = agreement_str
            
            # Fill LC 'Carrier agreement #' based on ETOF #
            for idx, row in lc_df_renamed.iterrows():
                lc_etof_num = row.get(lc_etof_col)
                
                if pd.notna(lc_etof_num):
                    lc_etof_num_str = str(lc_etof_num).strip()
                    if lc_etof_num_str in etof_agreement_mapping:
                        lc_df_renamed.at[idx, 'Carrier agreement #'] = etof_agreement_mapping[lc_etof_num_str]
            
            # Show statistics
            lc_rows_with_agreement = len(lc_df_renamed[lc_df_renamed['Carrier agreement #'].notna()])
            print(f"   Rows with Carrier agreement # (after fill): {lc_rows_with_agreement}")
        else:
            if not etof_agreement_col:
                print(f"   [WARNING] 'Carrier agreement #' column not found in ETOF dataframe")
        
        # Print dataframe headers after Step 4.5/4.6
        print(f"\nStep 4.6: After filling LC Carrier and Carrier agreement # from ETOF")
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
            print(f"   Columns: {list(etof_df_renamed.columns)}")
            print(f"   First few rows:\n{etof_df_renamed.head(3).to_string()}")
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            print(f"   LC DataFrame: {len(lc_df_renamed)} rows, {len(lc_df_renamed.columns)} columns")
            print(f"   Columns: {list(lc_df_renamed.columns)}")
            print(f"   First few rows:\n{lc_df_renamed.head(3).to_string()}")
        # COMMENTED OUT - origin file processing disabled
        # if origin_df_renamed is not None and not origin_df_renamed.empty:
        #     print(f"   Origin DataFrame: {len(origin_df_renamed)} rows, {len(origin_df_renamed.columns)} columns")
        #     print(f"   Columns: {list(origin_df_renamed.columns)}")
        #     print(f"   First few rows:\n{origin_df_renamed.head(3).to_string()}")
            
    
    # COMMENTED OUT - origin file processing disabled
    # # Step 6: Update ETOF dataframe with values from Origin dataframe
    # # Skip if LC file was provided, only update if ETOF and Origin files were provided
    # if lc_df_renamed is None and etof_df_renamed is not None and origin_df_renamed is not None:
    if False:  # Disabled - origin file processing commented out
        try:
            # Find matching columns
            shipment_id_col_etof = None
            shipment_id_col_origin = None
            delivery_col_etof = None
            delivery_col_origin = None
            
            # Find SHIPMENT_ID columns
            shipment_variations = ['SHIPMENT_ID', 'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid']
            for col in etof_df_renamed.columns:
                if str(col).strip() in shipment_variations or str(col).strip().upper() == 'SHIPMENT_ID':
                    shipment_id_col_etof = col
                    break
            for col in origin_df_renamed.columns:
                if str(col).strip() in shipment_variations or str(col).strip().upper() == 'SHIPMENT_ID':
                    shipment_id_col_origin = col
                    break
            
            # Find Delivery Number columns
            delivery_variations = ['Delivery Number', 'DeliveryNumber', 'delivery number', 'deliverynumber', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)','delivery_number','delivery number(s)','delivery number(s)']
            for col in etof_df_renamed.columns:
                col_str = str(col).strip()
                if col_str in delivery_variations or 'delivery' in col_str.lower() and 'number' in col_str.lower():
                    delivery_col_etof = col
                    break
            for col in origin_df_renamed.columns:
                col_str = str(col).strip()
                if col_str in delivery_variations or 'delivery' in col_str.lower() and 'number' in col_str.lower():
                    delivery_col_origin = col
                    break
            
            # Create mapping from Origin dataframe: (shipment_id, delivery_num) -> row data
            origin_dict_by_shipment = {}
            origin_dict_by_delivery = {}
            
            for idx, row in origin_df_renamed.iterrows():
                shipment_id = str(row.get(shipment_id_col_origin, '')).strip() if shipment_id_col_origin and pd.notna(row.get(shipment_id_col_origin)) else None
                delivery_num = str(row.get(delivery_col_origin, '')).strip() if delivery_col_origin and pd.notna(row.get(delivery_col_origin)) else None
                
                if shipment_id and shipment_id.lower() != 'nan':
                    origin_dict_by_shipment[shipment_id] = {
                        'delivery': delivery_num if delivery_num and delivery_num.lower() != 'nan' else None,
                        'row': row.to_dict()
                    }
                
                if delivery_num and delivery_num.lower() != 'nan':
                    origin_dict_by_delivery[delivery_num] = row.to_dict()
            
            # Update ETOF dataframe
            common_cols = [col for col in etof_df_renamed.columns if col in origin_df_renamed.columns 
                          and col not in ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Loading date', 'Loading Date']]
            
            updated_count = 0
            for idx, row in etof_df_renamed.iterrows():
                shipment_id = str(row.get(shipment_id_col_etof, '')).strip() if shipment_id_col_etof and pd.notna(row.get(shipment_id_col_etof)) else None
                delivery_num = str(row.get(delivery_col_etof, '')).strip() if delivery_col_etof and pd.notna(row.get(delivery_col_etof)) else None
                
                origin_row = None
                
                # First try: SHIPMENT_ID matching
                # If SHIPMENT_ID matches, also verify DELIVERY_NUMBER matches (if both have it)
                if shipment_id and shipment_id.lower() != 'nan' and shipment_id in origin_dict_by_shipment:
                    origin_data = origin_dict_by_shipment[shipment_id]
                    # If both ETOF and Origin have delivery number, they must match
                    if delivery_num and delivery_num.lower() != 'nan' and origin_data['delivery']:
                        if origin_data['delivery'] == delivery_num:
                            origin_row = origin_data['row']
                    else:
                        # SHIPMENT_ID matches, and either no delivery number in ETOF or no delivery number in Origin - use it
                        origin_row = origin_data['row']
                
                # Fallback: If no SHIPMENT_ID in ETOF or no match, use DELIVERY_NUMBER
                if origin_row is None:
                    if not shipment_id or shipment_id.lower() == 'nan':
                        # No SHIPMENT_ID in ETOF - use DELIVERY_NUMBER
                        if delivery_num and delivery_num.lower() != 'nan' and delivery_num in origin_dict_by_delivery:
                            origin_row = origin_dict_by_delivery[delivery_num]
                    # If SHIPMENT_ID didn't match, also try DELIVERY_NUMBER as fallback
                    elif delivery_num and delivery_num.lower() != 'nan' and delivery_num in origin_dict_by_delivery:
                        origin_row = origin_dict_by_delivery[delivery_num]
                
                # Update NaN columns with values from Origin
                if origin_row:
                    for col in common_cols:
                        if pd.isna(etof_df_renamed.at[idx, col]) or etof_df_renamed.at[idx, col] is None:
                            origin_value = origin_row.get(col)
                            if pd.notna(origin_value) and origin_value is not None:
                                etof_df_renamed.at[idx, col] = origin_value
                                updated_count += 1
        except Exception:
            pass
        
        # Print dataframe headers after Step 6
        print(f"\nStep 6: After updating ETOF from Origin")
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
            print(f"   Columns: {list(etof_df_renamed.columns)}")
            print(f"   First few rows:\n{etof_df_renamed.head(3).to_string()}")
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            print(f"   LC DataFrame: {len(lc_df_renamed)} rows, {len(lc_df_renamed.columns)} columns")
            print(f"   Columns: {list(lc_df_renamed.columns)}")
            print(f"   First few rows:\n{lc_df_renamed.head(3).to_string()}")
        # COMMENTED OUT - origin file processing disabled
        # if origin_df_renamed is not None and not origin_df_renamed.empty:
        #     print(f"   Origin DataFrame: {len(origin_df_renamed)} rows, {len(origin_df_renamed.columns)} columns")
        #     print(f"   Columns: {list(origin_df_renamed.columns)}")
        #     print(f"   First few rows:\n{origin_df_renamed.head(3).to_string()}")
    
    # Step 7: Save mapping to txt file
    from pathlib import Path
    output_folder = Path(__file__).parent / "partly_df"
    output_folder.mkdir(exist_ok=True)
    txt_output_path = output_folder / output_txt_path
    
    with open(txt_output_path, 'w', encoding='utf-8') as f:
        f.write("COLUMN MAPPING RESULTS\n")
        f.write("="*80 + "\n\n")
        f.write("MAPPINGS: Rate Card Column -> ETOF Column / LC Column\n")
        f.write("="*80 + "\n\n")
        for result in mapping_results:
            f.write(f"{result['Rate_Card_Column']} -> ETOF: {result['ETOF_Column']}, LC: {result['LC_Column']}\n")
        f.write("\n" + "="*80 + "\n")
        f.write("DETAILED MAPPINGS\n")
        f.write("="*80 + "\n\n")
        f.write("ETOF Mappings:\n")
        for rate_card_col, etof_col in etof_mappings.items():
            f.write(f"  {rate_card_col} <- {etof_col}\n")
        f.write("\nLC Mappings:\n")
        for rate_card_col, lc_col in lc_mappings.items():
            f.write(f"  {rate_card_col} <- {lc_col}\n")
        # COMMENTED OUT - origin file processing disabled
        # f.write("\nOrigin Mappings:\n")
        # for rate_card_col, origin_col in origin_mappings.items():
        #     f.write(f"  {rate_card_col} <- {origin_col}\n")
    
    # Print final dataframe headers before returning
    print(f"\nStep 7: Final dataframes before return")
    if etof_df_renamed is not None and not etof_df_renamed.empty:
        print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
        print(f"   Columns: {list(etof_df_renamed.columns)}")
        print(f"   First few rows:\n{etof_df_renamed.head(3).to_string()}")
    else:
        print(f"   ETOF DataFrame: Empty or None")
    if lc_df_renamed is not None and not lc_df_renamed.empty:
        print(f"   LC DataFrame: {len(lc_df_renamed)} rows, {len(lc_df_renamed.columns)} columns")
        print(f"   Columns: {list(lc_df_renamed.columns)}")
        print(f"   First few rows:\n{lc_df_renamed.head(3).to_string()}")
    else:
        print(f"   LC DataFrame: Empty or None")
    # COMMENTED OUT - origin file processing disabled
    # if origin_df_renamed is not None and not origin_df_renamed.empty:
    #     print(f"   Origin DataFrame: {len(origin_df_renamed)} rows, {len(origin_df_renamed.columns)} columns")
    #     print(f"   Columns: {list(origin_df_renamed.columns)}")
    #     print(f"   First few rows:\n{origin_df_renamed.head(3).to_string()}")
    # else:
    #     print(f"   Origin DataFrame: Empty or None")


     # Step 8: Save dataframes to Excel file
    excel_output_path = output_folder / "vocabulary_mapping.xlsx"
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            etof_df_renamed.to_excel(writer, sheet_name='ETOF', index=False)
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            lc_df_renamed.to_excel(writer, sheet_name='LC', index=False)
        # COMMENTED OUT - origin file processing disabled
        # if origin_df_renamed is not None and not origin_df_renamed.empty:
        #     origin_df_renamed.to_excel(writer, sheet_name='Origin', index=False)
        
        # Save mapping DataFrame
        mapping_df = pd.DataFrame(mapping_results)
        if not mapping_df.empty:
            mapping_df.to_excel(writer, sheet_name='Mapping', index=False)
    
    return etof_df_renamed, lc_df_renamed, None  # Return None instead of origin_df_renamed


def map_and_rename_columns_from_files(
    rate_card_file_path: str,
    lc_df: pd.DataFrame,
    agreement_number: str,
    etof_file_path: Optional[str] = None,
    output_txt_path: str = "column_mapping_results.txt",
    ignore_rate_card_columns: Optional[List[str]] = None,
    shipper_id: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Map rate card columns to pre-loaded LC dataframe, rename columns, and save results.
    This version reads from pre-processed files instead of calling processing functions.
    
    Args:
        rate_card_file_path: Path to rate card file (in partly_df folder)
        lc_df: Pre-loaded LC dataframe (from a specific agreement tab)
        agreement_number: Agreement number for this rate card
        etof_file_path: Optional path to ETOF file (for additional data)
        output_txt_path: Path to save the mapping results text file
        ignore_rate_card_columns: Optional list of rate card column names to ignore/delete from processing
        shipper_id: Optional shipper ID for custom logic (e.g., "dairb")
    
    Returns:
        Tuple: (etof_dataframe_renamed, lc_dataframe_renamed, None)
    """
    print(f"\n{'='*80}")
    print(f"PROCESSING AGREEMENT: {agreement_number}")
    print(f"{'='*80}")
    
    # Step 1: Read rate card from file
    try:
        print(f"\nStep 1: Reading rate card file: {rate_card_file_path}")
        rate_card_df, rate_card_columns_all, rate_card_conditions, _ = read_rate_card_from_file(rate_card_file_path)
        
        if rate_card_df is None or rate_card_df.empty:
            print(f"   ERROR: Rate card is empty or could not be read")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        print(f"   Successfully loaded rate card: {len(rate_card_columns_all)} columns")
        
        # Filter out ignored columns
        if ignore_rate_card_columns is None:
            ignore_rate_card_columns = []
        
        # Remove ignored columns from rate card dataframe
        if ignore_rate_card_columns:
            columns_to_drop = [col for col in ignore_rate_card_columns if col in rate_card_df.columns]
            if columns_to_drop:
                rate_card_df = rate_card_df.drop(columns=columns_to_drop)
        
        # Update rate_card_columns_all to exclude ignored columns
        rate_card_columns_all = [col for col in rate_card_columns_all if col not in ignore_rate_card_columns]
        
        rate_card_columns_to_map = [
            col for col in rate_card_columns_all 
            if not is_excluded_column(col) and col not in RATE_CARD_EXCLUDED_COLUMNS
        ]
        rate_card_columns = rate_card_columns_to_map
        print(f"   Rate card columns to map: {len(rate_card_columns)}")
    except Exception as e:
        print(f"   ERROR reading rate card: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Use the pre-loaded LC dataframe
    etof_df = None
    
    if etof_file_path:
        try:
            print(f"\nStep 2a: Processing ETOF file: {etof_file_path}")
            etof_df, etof_columns = process_etof_file(etof_file_path)
            print(f"   Successfully loaded ETOF: {len(etof_columns)} columns, {len(etof_df)} rows")
        except Exception as e:
            print(f"   ERROR processing ETOF: {e}")
            etof_df = None
    
    print(f"\nStep 2b: Using pre-loaded LC data for agreement {agreement_number}")
    print(f"   LC DataFrame: {len(lc_df)} rows, {len(lc_df.columns)} columns")
    
    # Step 3: Find mappings for each rate card column
    print(f"\nStep 3: Checking dataframes...")
    print(f"   ETOF df: {'Exists' if etof_df is not None and not etof_df.empty else 'None/Empty'}")
    print(f"   LC df: {'Exists' if lc_df is not None and not lc_df.empty else 'None/Empty'}")
    
    if etof_df is None and (lc_df is None or lc_df.empty):
        print("   ERROR: All dataframes are None/Empty. Returning empty dataframes.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Columns to always keep (even if not in rate card)
    keep_columns = ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Delivery Number', 'DeliveryNumber', 
                   'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid', 'SHIPMENT_ID', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)']
    
    # Mapping results
    mapping_results = []
    etof_mappings = {}
    lc_mappings = {}
    
    # Track which source columns have been used
    used_etof_columns = set()
    used_lc_columns = set()
    
    for rate_card_col in rate_card_columns:
        etof_match = None
        lc_match = None
        
        # Find match in ETOF
        if etof_df is not None and not etof_df.empty:
            etof_columns = [col for col in etof_df.columns 
                          if not is_excluded_column(col) and col not in used_etof_columns]
            if etof_columns:
                match, confidence = find_semantic_match_llm(rate_card_col, etof_columns, threshold=0.3)
                if match and not is_excluded_column(match) and match not in used_etof_columns:
                    etof_match = match
                    etof_mappings[rate_card_col] = match
                    used_etof_columns.add(match)
        
        # Find match in LC
        if lc_df is not None and not lc_df.empty:
            lc_columns = [col for col in lc_df.columns 
                        if not is_excluded_column(col) and col not in used_lc_columns]
            if lc_columns:
                # Check custom logic first
                custom_match_found = False
                if shipper_id and len(CUSTOM_LOGIC_MAPPINGS) > 0:
                    for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                        if shipper_id_key and shipper_id_key == shipper_id:
                            if rate_card_col in mapping_dict.values():
                                for source_col, mapped_standard in mapping_dict.items():
                                    if mapped_standard == rate_card_col and source_col in lc_columns:
                                        if not is_excluded_column(source_col) and source_col not in used_lc_columns:
                                            lc_match = source_col
                                            lc_mappings[rate_card_col] = source_col
                                            used_lc_columns.add(source_col)
                                            custom_match_found = True
                                            break
                        if custom_match_found:
                            break
                
                if not custom_match_found:
                    match, confidence = find_semantic_match_llm(rate_card_col, lc_columns, threshold=0.3)
                    if match and not is_excluded_column(match) and match not in used_lc_columns:
                        lc_match = match
                        lc_mappings[rate_card_col] = match
                        used_lc_columns.add(match)
        
        mapping_results.append({
            'Rate_Card_Column': rate_card_col,
            'ETOF_Column': etof_match if etof_match else 'NONE',
            'LC_Column': lc_match if lc_match else 'NONE',
        })
    
    # Step 4: Rename columns and create output dataframes
    all_rate_card_cols_for_output = rate_card_columns_all.copy()
    
    etof_df_renamed = None
    lc_df_renamed = None
    
    def create_output_dataframe_simple(source_df, source_mappings, source_name, keep_cols_list, specific_keep_list, all_rate_card_cols):
        """Helper function to create output dataframe."""
        if source_df is None or source_df.empty:
            return None

        output_df = source_df.copy()
        rename_dict = {}
        columns_to_keep = []
        
        # Add rate card mapped columns
        for rate_card_col, source_col in source_mappings.items():
            if source_col in output_df.columns:
                rename_dict[source_col] = f"{rate_card_col} ({source_col})"
                columns_to_keep.append(source_col)
        
        # Add columns to always keep
        for keep_col in keep_cols_list:
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                keep_normalized = keep_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == keep_normalized:
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    break
        
        # Add source-specific columns
        for keep_col in specific_keep_list:
            for col in output_df.columns:
                if col.lower() == keep_col.lower():
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    break
        
        # Rename columns
        output_df.rename(columns=rename_dict, inplace=True)
        
        # Rename to standard column names
        rename_to_standard = {}
        for col in output_df.columns:
            if ' (' in col and col.endswith(')'):
                standard_name = col.split(' (')[0]
                if standard_name in all_rate_card_cols:
                    rename_to_standard[col] = standard_name
        
        if rename_to_standard:
            output_df.rename(columns=rename_to_standard, inplace=True)
        
        # Add missing rate card columns as empty
        for rate_card_col in all_rate_card_cols:
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            if rate_card_col not in output_df.columns:
                if rate_card_col not in source_mappings:
                    output_df[rate_card_col] = None
        
        # Build final column list
        final_columns = []
        
        for rate_card_col in all_rate_card_cols:
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            if rate_card_col in output_df.columns:
                final_columns.append(rate_card_col)
        
        # Add key columns
        key_columns_to_find = ['ETOF #', 'LC #', 'carrier', 'carrier_name', 'shipment_id',
                              'delivery_number', 'deliverynumber(s)', 'Carrier agreement #']
        for key_col in key_columns_to_find:
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                key_normalized = key_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == key_normalized:
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        
        # Add source-specific columns
        for specific_col in specific_keep_list:
            for col in output_df.columns:
                if col.lower() == specific_col.lower():
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        
        # Filter to final columns
        final_columns = [col for col in final_columns if col in output_df.columns]
        output_df = output_df[final_columns]
        
        return output_df
    
    # Process ETOF
    if etof_df is not None:
        etof_specific_keep = ['Loading date', 'Loading Date', 'Carrier agreement #']
        etof_df_renamed = create_output_dataframe_simple(
            etof_df, etof_mappings, 'ETOF', keep_columns, etof_specific_keep, all_rate_card_cols_for_output
        )
    
    # Process LC
    if lc_df is not None and not lc_df.empty:
        lc_specific_keep = ['SHIP_DATE', 'ship_date', 'Carrier agreement #']
        lc_df_renamed = create_output_dataframe_simple(
            lc_df, lc_mappings, 'LC', keep_columns, lc_specific_keep, all_rate_card_cols_for_output
        )
    
    # Step 5: Save results
    output_folder = get_partly_df_folder()
    output_folder.mkdir(exist_ok=True)
    
    # Save txt mapping file
    txt_output_path = output_folder / f"{agreement_number}_{output_txt_path}"
    with open(txt_output_path, 'w', encoding='utf-8') as f:
        f.write(f"COLUMN MAPPING RESULTS - {agreement_number}\n")
        f.write("="*80 + "\n\n")
        f.write("MAPPINGS: Rate Card Column -> ETOF Column / LC Column\n")
        f.write("="*80 + "\n\n")
        for result in mapping_results:
            f.write(f"{result['Rate_Card_Column']} -> ETOF: {result['ETOF_Column']}, LC: {result['LC_Column']}\n")
    
    # Save Excel file
    excel_output_path = output_folder / f"{agreement_number}_vocabulary_mapping.xlsx"
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            etof_df_renamed.to_excel(writer, sheet_name='ETOF', index=False)
        if lc_df_renamed is not None and not lc_df_renamed.empty:
            lc_df_renamed.to_excel(writer, sheet_name='LC', index=False)
        mapping_df = pd.DataFrame(mapping_results)
        if not mapping_df.empty:
            mapping_df.to_excel(writer, sheet_name='Mapping', index=False)
    
    print(f"\n   Saved results to: {excel_output_path}")
    
    return etof_df_renamed, lc_df_renamed, None


def process_all_rate_cards_from_mapping_file(
    mapping_filename: str = "lc_etof_mapping.xlsx",
    ignore_rate_card_columns: Optional[List[str]] = None,
    shipper_id: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Process all rate cards from the LC/ETOF mapping file.
    
    Workflow:
    1. Read the LC/ETOF mapping file from partly_df (created by part7_optional_order_lc_etof_mapping.py)
    2. Get all Carrier agreement # tabs (ignore "All Data" and "No Agreement")
    3. For each agreement tab:
       - Read the LC/ETOF data from that tab (already filtered by Carrier agreement #)
       - Find the corresponding rate card file in partly_df (e.g., RA20220420022.xlsx)
       - Run vocabulary mapping
       - Save output to: <agreement>_vocabulary_mapping.xlsx
    
    Args:
        mapping_filename: Name of the LC/ETOF mapping file in partly_df folder
        ignore_rate_card_columns: Optional list of rate card column names to ignore
        shipper_id: Optional shipper ID for custom logic
    
    Returns:
        dict: {agreement_number: lc_df_renamed, ...}
    """
    print("\n" + "="*80)
    print("PROCESSING ALL RATE CARDS FROM LC/ETOF MAPPING FILE")
    print("="*80)
    
    # Step 1: Find the mapping file in partly_df
    mapping_file_path = get_lc_etof_mapping_file(mapping_filename)
    if mapping_file_path is None:
        print(f"\n   [ERROR] Mapping file not found: {mapping_filename}")
        print(f"   Please ensure the file exists in: {get_partly_df_folder()}")
        print(f"\n   Expected workflow:")
        print(f"   1. Run part4_rate_card_processing.py to create rate card files in partly_df/")
        print(f"   2. Run part7_optional_order_lc_etof_mapping.py to create {mapping_filename} in partly_df/")
        print(f"   3. Run this script (vocabular.py)")
        return {}
    
    print(f"\n1. Found LC/ETOF mapping file: {mapping_file_path}")
    
    # Step 2: Get all agreement tabs (excluding "All Data" and "No Agreement")
    agreement_tabs = get_agreement_tabs_from_mapping_file(mapping_file_path)
    if not agreement_tabs:
        print("\n   [ERROR] No agreement tabs found in mapping file")
        print("   The mapping file should have tabs named after Carrier agreement numbers")
        print("   (e.g., 'RA20220420022', 'RA20241129009')")
        return {}
    
    print(f"\n2. Found {len(agreement_tabs)} agreement tab(s) to process:")
    for tab in agreement_tabs:
        print(f"   - {tab}")
    
    # Step 3: Check which rate card files exist
    print(f"\n3. Checking for corresponding rate card files in partly_df/:")
    rate_card_files = {}
    for agreement in agreement_tabs:
        rate_card_file = find_rate_card_file(agreement)
        if rate_card_file:
            rate_card_files[agreement] = rate_card_file
            print(f"   ✓ {agreement}: {rate_card_file.name}")
        else:
            print(f"   ✗ {agreement}: NOT FOUND")
    
    if not rate_card_files:
        print("\n   [ERROR] No matching rate card files found")
        print("   Please ensure rate card files are named after agreement numbers")
        print("   (e.g., 'RA20220420022.xlsx' for agreement 'RA20220420022')")
        return {}
    
    # Step 4: Process each agreement
    print(f"\n4. Processing {len(rate_card_files)} agreement(s)...")
    results = {}
    
    for i, (agreement_number, rate_card_file) in enumerate(rate_card_files.items(), 1):
        print(f"\n{'='*80}")
        print(f"[{i}/{len(rate_card_files)}] PROCESSING: {agreement_number}")
        print(f"{'='*80}")
        
        # Read LC/ETOF data from this agreement's tab
        print(f"\n   a) Reading LC/ETOF data from tab '{agreement_number}'...")
        lc_df, lc_columns = read_lc_data_from_tab(mapping_file_path, agreement_number)
        if lc_df is None or lc_df.empty:
            print(f"      [SKIP] No data in tab '{agreement_number}'")
            continue
        print(f"      Found {len(lc_df)} rows, {len(lc_columns)} columns")
        
        # Read rate card
        print(f"\n   b) Reading rate card from '{rate_card_file.name}'...")
        rate_card_df, rate_card_columns, rate_card_conditions, _ = read_rate_card_from_file(rate_card_file)
        if rate_card_df is None or rate_card_df.empty:
            print(f"      [SKIP] Could not read rate card for '{agreement_number}'")
            continue
        print(f"      Found {len(rate_card_df)} rows, {len(rate_card_columns)} columns")
        
        # Read business rules to identify columns to skip (same as canf_vocabular.py)
        print(f"\n   c) Reading business rules...")
        business_rules_info = read_business_rules_from_file(rate_card_file)
        business_rule_columns = business_rules_info.get('skip_columns', set())
        if business_rule_columns:
            print(f"      Found {len(business_rule_columns)} columns containing business rules (will skip semantic matching):")
            for col in sorted(business_rule_columns):
                print(f"         - {col}")
        
        # Add required geographic columns to the rate card columns (same as canf_vocabular.py)
        geo_columns = get_required_geo_columns()
        for geo_col in geo_columns:
            if geo_col not in rate_card_columns:
                rate_card_columns.append(geo_col)
        print(f"      Added geographic columns for mapping: {geo_columns}")
        
        # Process vocabulary mapping (same logic as canf_vocabular.py)
        print(f"\n   d) Creating vocabulary mapping...")
        try:
            lc_renamed = process_single_agreement(
                lc_df=lc_df,
                rate_card_columns=rate_card_columns,
                agreement_number=agreement_number,
                ignore_rate_card_columns=ignore_rate_card_columns,
                shipper_id=shipper_id,
                business_rule_columns=business_rule_columns
            )
            
            if lc_renamed is not None and not lc_renamed.empty:
                results[agreement_number] = lc_renamed
                print(f"\n   [SUCCESS] Processed {agreement_number}: {len(lc_renamed)} rows, {len(lc_renamed.columns)} columns")
            else:
                print(f"\n   [WARNING] No output data for {agreement_number}")
                
        except Exception as e:
            print(f"\n   [ERROR] Failed to process {agreement_number}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Summary
    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"\n   Agreements found in mapping file: {len(agreement_tabs)}")
    print(f"   Rate card files found: {len(rate_card_files)}")
    print(f"   Successfully processed: {len(results)}")
    
    if results:
        print(f"\n   Output files created in: {get_partly_df_folder()}")
        for agreement in results.keys():
            print(f"     - {agreement}_vocabulary_mapping.xlsx")
    
    return results


def process_single_agreement(
    lc_df: pd.DataFrame,
    rate_card_columns: List[str],
    agreement_number: str,
    ignore_rate_card_columns: Optional[List[str]] = None,
    shipper_id: Optional[str] = None,
    business_rule_columns: Optional[set] = None
) -> pd.DataFrame:
    """
    Process a single agreement: map LC columns to rate card columns and save result.
    Same mapping logic as canf_vocabular.py's map_and_rename_columns.
    
    Args:
        lc_df: LC/ETOF dataframe for this agreement (from mapping file tab)
        rate_card_columns: List of column names from the rate card
        agreement_number: Agreement number
        ignore_rate_card_columns: Optional list of rate card columns to ignore
        shipper_id: Optional shipper ID for custom logic
        business_rule_columns: Set of columns containing business rules (skip from mapping)
    
    Returns:
        pd.DataFrame: Processed LC dataframe with renamed columns
    """
    # Filter out ignored columns
    if ignore_rate_card_columns is None:
        ignore_rate_card_columns = []
    
    if business_rule_columns is None:
        business_rule_columns = set()
    
    # Filter rate card columns (same logic as canf_vocabular.py)
    # Skip columns that contain business rules (e.g., "Origin Postal Code Zone")
    rate_card_columns_filtered = [
        col for col in rate_card_columns 
        if col not in ignore_rate_card_columns 
        and not is_excluded_column(col) 
        and col not in RATE_CARD_EXCLUDED_COLUMNS
        and col not in business_rule_columns  # Skip business rule columns
    ]
    
    print(f"      Rate card columns to map: {len(rate_card_columns_filtered)}")
    if business_rule_columns:
        print(f"      Skipping {len(business_rule_columns)} business rule columns")
    
    # Columns to always keep
    keep_columns = ['ETOF #', 'ETOF#', 'LC #', 'LC#', 'Carrier', 'Carrier agreement #',
                   'Delivery Number', 'DeliveryNumber', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)',
                   'Shipment ID', 'ShipmentID', 'SHIPMENT_ID', 'SHIP_DATE', 'ship_date']
    
    # Find mappings for each rate card column
    lc_mappings = {}
    used_lc_columns = set()
    mapping_results = []
    
    lc_columns = lc_df.columns.tolist()
    
    for rate_card_col in rate_card_columns_filtered:
        # Get available columns (not yet used)
        available_columns = [col for col in lc_columns 
                           if not is_excluded_column(col) and col not in used_lc_columns]
        
        if not available_columns:
            mapping_results.append({
                'Rate_Card_Column': rate_card_col,
                'LC_Column': 'NONE',
                'Method': 'no_available_columns'
            })
            continue
        
        # Step 1: Check custom logic first if available
        custom_match_found = False
        if shipper_id and len(CUSTOM_LOGIC_MAPPINGS) > 0:
            for (carrier_id_key, shipper_id_key, transport_mode_key), mapping_dict in CUSTOM_LOGIC_MAPPINGS.items():
                # Check if shipper_id matches
                if shipper_id_key and shipper_id_key == shipper_id:
                    # Check if this rate card column has a custom mapping
                    if rate_card_col in mapping_dict.values():
                        # Find the source column that maps to this rate card column
                        for source_col, mapped_standard in mapping_dict.items():
                            if mapped_standard == rate_card_col and source_col in available_columns:
                                if not is_excluded_column(source_col) and source_col not in used_lc_columns:
                                    lc_mappings[rate_card_col] = source_col
                                    used_lc_columns.add(source_col)
                                    custom_match_found = True
                                    mapping_results.append({
                                        'Rate_Card_Column': rate_card_col,
                                        'LC_Column': source_col,
                                        'Method': 'custom'
                                    })
                                    break
                if custom_match_found:
                    break
        
        if custom_match_found:
            continue
        
        # Step 2: Use semantic matching if no custom mapping found
        match, confidence = find_semantic_match_llm(rate_card_col, available_columns, threshold=0.3)
        if match and not is_excluded_column(match) and match not in used_lc_columns:
            lc_mappings[rate_card_col] = match
            used_lc_columns.add(match)
            mapping_results.append({
                'Rate_Card_Column': rate_card_col,
                'LC_Column': match,
                'Method': 'semantic',
                'Confidence': confidence
            })
        else:
            mapping_results.append({
                'Rate_Card_Column': rate_card_col,
                'LC_Column': 'NONE',
                'Method': 'none'
            })
    
    print(f"      Mapped {len(lc_mappings)} columns out of {len(rate_card_columns_filtered)} rate card columns")
    
    # Create output dataframe
    output_df = lc_df.copy()
    
    # Rename mapped columns to rate card names
    rename_dict = {v: k for k, v in lc_mappings.items()}
    output_df = output_df.rename(columns=rename_dict)
    
    # Build final column list
    final_columns = []
    
    # Add rate card columns (mapped ones first)
    for rate_card_col in rate_card_columns_filtered:
        if rate_card_col in output_df.columns:
            final_columns.append(rate_card_col)
    
    # Add unmapped rate card columns as empty
    for rate_card_col in rate_card_columns_filtered:
        if rate_card_col not in output_df.columns:
            output_df[rate_card_col] = None
            final_columns.append(rate_card_col)
    
    # Add key columns that should always be kept
    for keep_col in keep_columns:
        for col in output_df.columns:
            if col.lower().replace(' ', '').replace('#', '') == keep_col.lower().replace(' ', '').replace('#', ''):
                if col not in final_columns:
                    final_columns.append(col)
                break
    
    # Filter to final columns
    final_columns = [col for col in final_columns if col in output_df.columns]
    output_df = output_df[final_columns]
    
    # Step: Standardize geographic columns (same as canf_vocabular.py Step 10)
    # Ensure Origin Country, Origin Postal Code, etc. exist and have standard names
    geo_columns_mapping = {
        'Origin Country': ['Origin Country', 'origin country', 'OriginCountry', 'ORIGIN_COUNTRY', 
                          'Ship Country', 'ship country', 'ShipCountry', 'SHIP_COUNTRY',
                          'From Country', 'from country', 'FromCountry', 'FROM_COUNTRY'],
        'Origin Postal Code': ['Origin Postal Code', 'origin postal code', 'OriginPostalCode', 'ORIGIN_POSTAL_CODE',
                               'Ship Postal', 'ship postal', 'ShipPostal', 'SHIP_POSTAL', 'SHIP_POST',
                               'From Postal', 'from postal', 'FromPostal', 'FROM_POSTAL',
                               'Origin Zip', 'origin zip', 'OriginZip', 'ORIGIN_ZIP'],
        'Destination Country': ['Destination Country', 'destination country', 'DestinationCountry', 'DESTINATION_COUNTRY',
                               'Cust Country', 'cust country', 'CustCountry', 'CUST_COUNTRY',
                               'To Country', 'to country', 'ToCountry', 'TO_COUNTRY'],
        'Destination Postal Code': ['Destination Postal Code', 'destination postal code', 'DestinationPostalCode', 'DESTINATION_POSTAL_CODE',
                                    'Cust Postal', 'cust postal', 'CustPostal', 'CUST_POSTAL', 'CUST_POST',
                                    'To Postal', 'to postal', 'ToPostal', 'TO_POSTAL',
                                    'Destination Zip', 'destination zip', 'DestinationZip', 'DESTINATION_ZIP']
    }
    
    for standard_geo_col, variations in geo_columns_mapping.items():
        geo_col_found = False
        found_col_name = None
        
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            
            for variation in variations:
                var_lower = variation.lower().replace(' ', '').replace('_', '')
                if col_lower == var_lower:
                    geo_col_found = True
                    found_col_name = col
                    break
            if geo_col_found:
                break
        
        if geo_col_found and found_col_name != standard_geo_col:
            # Rename to standard name
            output_df = output_df.rename(columns={found_col_name: standard_geo_col})
            print(f"      Renamed '{found_col_name}' -> '{standard_geo_col}'")
        elif not geo_col_found:
            # Add empty column
            output_df[standard_geo_col] = None
            print(f"      Added empty column: '{standard_geo_col}'")
    
    # Save output
    output_folder = get_partly_df_folder()
    output_folder.mkdir(exist_ok=True)
    
    # Save Excel file
    excel_output_path = output_folder / f"{agreement_number}_vocabulary_mapping.xlsx"
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        output_df.to_excel(writer, sheet_name='Mapped Data', index=False)
        
        # Save mapping info
        mapping_df = pd.DataFrame(mapping_results)
        if not mapping_df.empty:
            mapping_df.to_excel(writer, sheet_name='Column Mapping', index=False)
    
    print(f"      Saved to: {excel_output_path}")
    
    return output_df


# Example usage
if __name__ == "__main__":
    try:
        print("\n" + "="*80)
        print("VOCABULARY MAPPING - MULTI RATE CARD PROCESSING")
        print("="*80)
        print("\nThis script processes LC/ETOF data for each Carrier agreement separately.")
        print("\nExpected input files in 'partly_df/' folder:")
        print("  1. lc_etof_mapping.xlsx - created by part7_optional_order_lc_etof_mapping.py")
        print("     (has tabs: 'All Data', 'RA20220420022', 'RA20241129009', etc.)")
        print("  2. <agreement>.xlsx - rate cards created by part4_rate_card_processing.py")
        print("     (e.g., 'RA20220420022.xlsx', 'RA20241129009.xlsx')")
        print("\nOutput files (one per agreement):")
        print("  - <agreement>_vocabulary_mapping.xlsx")
        
        # Process all rate cards from the mapping file
        results = process_all_rate_cards_from_mapping_file(
            mapping_filename="lc_etof_mapping.xlsx",  # or "order_lc_etof_mapping.xlsx"
            #ignore_rate_card_columns=["Business Unit Name", "Remark"],
            shipper_id="densir"
        )
        
        # Print summary of results
        if results:
            print("\n" + "="*80)
            print("RESULTS SUMMARY")
            print("="*80)
            for agreement, df in results.items():
                print(f"\n{agreement}:")
                if df is not None and not df.empty:
                    print(f"   Rows: {len(df)}")
                    print(f"   Columns: {len(df.columns)}")
                    print(f"   Column names: {list(df.columns)[:10]}..." if len(df.columns) > 10 else f"   Column names: {list(df.columns)}")
        else:
            print("\n   No results produced. Please check the error messages above.")
            
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()  

 



