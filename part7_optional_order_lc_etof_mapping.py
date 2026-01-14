import pandas as pd
import os
from pathlib import Path
from part2_lc_processing import process_lc_input
from part1_etof_file_processing import process_etof_file


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def save_dataframe_by_carrier_agreement(df, output_filename, folder_name="partly_df"):
    """
    Save DataFrame to Excel with separate tabs for each Carrier agreement #.
    Also includes an "All Data" tab with all rows.
    
    Args:
        df: DataFrame with "Carrier agreement #" column
        output_filename: Name of the output Excel file
        folder_name: Output folder name (default: "partly_df")
    
    Returns:
        str: Path to the saved file
    """
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    output_path = output_folder / output_filename
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # First tab: All Data
        df.to_excel(writer, sheet_name='All Data', index=False)
        
        # Check if Carrier agreement # column exists
        if 'Carrier agreement #' in df.columns:
            # Get unique carrier agreements (excluding NaN/None/empty)
            raw_values = df['Carrier agreement #'].unique()
            print(f"   DEBUG: 'Carrier agreement #' column found with {len(raw_values)} unique raw values: {raw_values[:10]}{'...' if len(raw_values) > 10 else ''}")
            carrier_agreements = df['Carrier agreement #'].dropna().unique()
            print(f"   DEBUG: After dropna: {len(carrier_agreements)} unique values: {carrier_agreements[:10]}{'...' if len(carrier_agreements) > 10 else ''}")
            carrier_agreements = [ca for ca in carrier_agreements if str(ca).strip() and str(ca).lower() != 'nan']
            print(f"   DEBUG: After filtering empty/nan strings: {len(carrier_agreements)} values: {carrier_agreements[:10]}{'...' if len(carrier_agreements) > 10 else ''}")
            
            # Create a tab for each carrier agreement
            for carrier_agreement in sorted(carrier_agreements, key=str):
                # Filter rows for this carrier agreement
                df_filtered = df[df['Carrier agreement #'] == carrier_agreement]
                
                # Create safe sheet name (Excel limits to 31 chars, no special chars)
                sheet_name = str(carrier_agreement).strip()
                # Remove invalid characters for Excel sheet names
                invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
                for char in invalid_chars:
                    sheet_name = sheet_name.replace(char, '_')
                # Truncate to 31 characters (Excel limit)
                sheet_name = sheet_name[:31]
                
                if df_filtered.empty:
                    continue
                    
                df_filtered.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Also add a tab for rows without carrier agreement (if any)
            df_no_agreement = df[df['Carrier agreement #'].isna() | (df['Carrier agreement #'].astype(str).str.strip() == '') | (df['Carrier agreement #'].astype(str).str.lower() == 'nan')]
            if not df_no_agreement.empty:
                df_no_agreement.to_excel(writer, sheet_name='No Agreement', index=False)
    
    print(f"   Saved to: {output_path}")
    if 'Carrier agreement #' in df.columns:
        print(f"   Tabs created: All Data + {len(carrier_agreements)} carrier agreement tabs")
    else:
        print(f"   DEBUG: 'Carrier agreement #' column NOT found in dataframe. Available columns: {df.columns.tolist()}")
    
    return str(output_path)


def map_etof_to_lc(etof_dataframe, lc_dataframe_updated):
    """
    Map "ETOF #" and "Carrier agreement #" from etof_dataframe to lc_dataframe_updated.
    If SHIPMENT_ID is present in both dataframes, uses SHIPMENT_ID for mapping.
    Otherwise, uses DELIVERY_NUMBER for mapping.
    
    Args:
        etof_dataframe: DataFrame with "ETOF #" column and optionally "LC #", "SHIPMENT_ID" (or "SHIPMENT ID(s)"), and "Carrier agreement #" columns
        lc_dataframe_updated: DataFrame with "SHIPMENT_ID" or "DELIVERY_NUMBER" column
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: lc_dataframe_updated with added "ETOF #", "Carrier agreement #", "LC #" columns
            - list: List of column names in the processed dataframe
    """
    # Create a copy to avoid modifying the original
    lc_dataframe_final = lc_dataframe_updated.copy()
    
    # Check required columns exist
    if 'ETOF #' not in etof_dataframe.columns:
        raise ValueError("etof_dataframe must have 'ETOF #' column")
    
    # Check if Carrier agreement # column exists in ETOF
    has_carrier_agreement = 'Carrier agreement #' in etof_dataframe.columns
    print(f"   DEBUG ETOF columns: {etof_dataframe.columns.tolist()}")
    print(f"   DEBUG LC columns: {lc_dataframe_final.columns.tolist()}")
    print(f"   DEBUG has_carrier_agreement in ETOF: {has_carrier_agreement}")
    
    # Check if SHIPMENT_ID is present in both dataframes
    has_shipment_id_etof = 'SHIPMENT_ID' in etof_dataframe.columns or 'SHIPMENT ID(s)' in etof_dataframe.columns
    has_shipment_id_lc = 'SHIPMENT_ID' in lc_dataframe_final.columns
    use_shipment_id = has_shipment_id_etof and has_shipment_id_lc
    print(f"   DEBUG has_shipment_id_etof: {has_shipment_id_etof}, has_shipment_id_lc: {has_shipment_id_lc}, use_shipment_id: {use_shipment_id}")
    
    # Check if DELIVERY NUMBER is present in both dataframes (fallback option)
    has_delivery_number_etof = 'DELIVERY NUMBER(s)' in etof_dataframe.columns or 'DELIVERY_NUMBER' in etof_dataframe.columns
    has_delivery_number_lc = 'DELIVERY_NUMBER' in lc_dataframe_final.columns
    use_delivery_number = has_delivery_number_etof and has_delivery_number_lc and not use_shipment_id
    print(f"   DEBUG has_delivery_number_etof: {has_delivery_number_etof}, has_delivery_number_lc: {has_delivery_number_lc}, use_delivery_number: {use_delivery_number}")
    
    if use_shipment_id:
        # Determine which SHIPMENT_ID column name exists in ETOF
        etof_shipment_col = 'SHIPMENT_ID' if 'SHIPMENT_ID' in etof_dataframe.columns else 'SHIPMENT ID(s)'
        print(f"   DEBUG using ETOF shipment column: '{etof_shipment_col}'")
        
        # Use SHIPMENT_ID for mapping
        # Create mapping dictionaries: SHIPMENT_ID (from ETOF) -> ETOF #, LC #, and Carrier agreement #
        shipment_to_etof = {}
        shipment_to_lc = {}
        shipment_to_carrier_agreement = {}
        
        for _, row in etof_dataframe.iterrows():
            shipment_id = str(row.get(etof_shipment_col, '')).strip()
            etof_value = str(row.get('ETOF #', '')).strip()
            lc_value = str(row.get('LC #', '')).strip() if 'LC #' in etof_dataframe.columns else None
            carrier_agreement_value = str(row.get('Carrier agreement #', '')).strip() if has_carrier_agreement else None
            
            if pd.notna(row.get(etof_shipment_col)) and shipment_id and shipment_id.lower() != 'nan':
                if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                    # Map SHIPMENT_ID (key) to ETOF # (value)
                    shipment_to_etof[shipment_id] = etof_value
                
                if lc_value and pd.notna(row.get('LC #')) and lc_value.lower() != 'nan':
                    # Map SHIPMENT_ID (key) to LC # (value)
                    shipment_to_lc[shipment_id] = lc_value
                
                if carrier_agreement_value and pd.notna(row.get('Carrier agreement #')) and carrier_agreement_value.lower() != 'nan':
                    # Map SHIPMENT_ID (key) to Carrier agreement # (value)
                    shipment_to_carrier_agreement[shipment_id] = carrier_agreement_value
        
        print(f"   DEBUG shipment_to_etof mappings created: {len(shipment_to_etof)}")
        print(f"   DEBUG shipment_to_carrier_agreement mappings: {len(shipment_to_carrier_agreement)}")
        if shipment_to_etof:
            sample_keys = list(shipment_to_etof.keys())[:3]
            print(f"   DEBUG sample ETOF SHIPMENT_IDs (keys): {sample_keys}")
        
        # Show sample LC SHIPMENT_IDs
        lc_shipment_ids = lc_dataframe_final['SHIPMENT_ID'].dropna().unique()[:5].tolist()
        print(f"   DEBUG sample LC SHIPMENT_IDs: {lc_shipment_ids}")
        
        # Map ETOF # values by matching SHIPMENT_ID
        def find_etof_number_by_shipment(row):
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            if pd.isna(row.get('SHIPMENT_ID')) or shipment_id == '' or shipment_id.lower() == 'nan':
                return None
            return shipment_to_etof.get(shipment_id)
        
        # Map LC # values by matching SHIPMENT_ID
        def find_lc_number_by_shipment(row):
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            if pd.isna(row.get('SHIPMENT_ID')) or shipment_id == '' or shipment_id.lower() == 'nan':
                return None
            return shipment_to_lc.get(shipment_id)
        
        # Map Carrier agreement # values by matching SHIPMENT_ID
        def find_carrier_agreement_by_shipment(row):
            shipment_id = str(row.get('SHIPMENT_ID', '')).strip()
            if pd.isna(row.get('SHIPMENT_ID')) or shipment_id == '' or shipment_id.lower() == 'nan':
                return None
            return shipment_to_carrier_agreement.get(shipment_id)
        
        # Apply mappings
        lc_dataframe_final['ETOF #'] = lc_dataframe_final.apply(find_etof_number_by_shipment, axis=1)
        matched_count = lc_dataframe_final['ETOF #'].notna().sum()
        print(f"   DEBUG rows with ETOF # after mapping: {matched_count} / {len(lc_dataframe_final)}")
        
        # Map Carrier agreement # from ETOF if available
        if has_carrier_agreement:
            lc_dataframe_final['Carrier agreement #'] = lc_dataframe_final.apply(find_carrier_agreement_by_shipment, axis=1)
        
        # Map LC # from ETOF if available, otherwise create empty
        if shipment_to_lc:
            lc_dataframe_final['LC #'] = lc_dataframe_final.apply(find_lc_number_by_shipment, axis=1)
        else:
            lc_dataframe_final['LC #'] = None
    
    elif use_delivery_number:
        # Use DELIVERY NUMBER for mapping (second fallback)
        # Determine which DELIVERY NUMBER column name exists in ETOF
        etof_delivery_col = 'DELIVERY NUMBER(s)' if 'DELIVERY NUMBER(s)' in etof_dataframe.columns else 'DELIVERY_NUMBER'
        print(f"   DEBUG using ETOF delivery column: '{etof_delivery_col}'")
        
        # Create mapping dictionaries: DELIVERY_NUMBER (from ETOF) -> ETOF #, LC #, and Carrier agreement #
        delivery_to_etof = {}
        delivery_to_lc = {}
        delivery_to_carrier_agreement = {}
        
        for _, row in etof_dataframe.iterrows():
            delivery_number = str(row.get(etof_delivery_col, '')).strip()
            etof_value = str(row.get('ETOF #', '')).strip()
            lc_value = str(row.get('LC #', '')).strip() if 'LC #' in etof_dataframe.columns else None
            carrier_agreement_value = str(row.get('Carrier agreement #', '')).strip() if has_carrier_agreement else None
            
            if pd.notna(row.get(etof_delivery_col)) and delivery_number and delivery_number.lower() != 'nan':
                if pd.notna(row.get('ETOF #')) and etof_value and etof_value.lower() != 'nan':
                    # Map DELIVERY_NUMBER (key) to ETOF # (value)
                    delivery_to_etof[delivery_number] = etof_value
                
                if lc_value and pd.notna(row.get('LC #')) and lc_value.lower() != 'nan':
                    # Map DELIVERY_NUMBER (key) to LC # (value)
                    delivery_to_lc[delivery_number] = lc_value
                
                if carrier_agreement_value and pd.notna(row.get('Carrier agreement #')) and carrier_agreement_value.lower() != 'nan':
                    # Map DELIVERY_NUMBER (key) to Carrier agreement # (value)
                    delivery_to_carrier_agreement[delivery_number] = carrier_agreement_value
        
        print(f"   DEBUG delivery_to_etof mappings created: {len(delivery_to_etof)}")
        print(f"   DEBUG delivery_to_carrier_agreement mappings: {len(delivery_to_carrier_agreement)}")
        if delivery_to_etof:
            sample_keys = list(delivery_to_etof.keys())[:3]
            print(f"   DEBUG sample ETOF DELIVERY_NUMBERs (keys): {sample_keys}")
        
        # Show sample LC DELIVERY_NUMBERs
        lc_delivery_numbers = lc_dataframe_final['DELIVERY_NUMBER'].dropna().unique()[:5].tolist()
        print(f"   DEBUG sample LC DELIVERY_NUMBERs: {lc_delivery_numbers}")
        
        # Map ETOF # values by matching DELIVERY_NUMBER
        def find_etof_number_by_delivery(row):
            delivery_number = str(row.get('DELIVERY_NUMBER', '')).strip()
            if pd.isna(row.get('DELIVERY_NUMBER')) or delivery_number == '' or delivery_number.lower() == 'nan':
                return None
            return delivery_to_etof.get(delivery_number)
        
        # Map LC # values by matching DELIVERY_NUMBER
        def find_lc_number_by_delivery(row):
            delivery_number = str(row.get('DELIVERY_NUMBER', '')).strip()
            if pd.isna(row.get('DELIVERY_NUMBER')) or delivery_number == '' or delivery_number.lower() == 'nan':
                return None
            return delivery_to_lc.get(delivery_number)
        
        # Map Carrier agreement # values by matching DELIVERY_NUMBER
        def find_carrier_agreement_by_delivery(row):
            delivery_number = str(row.get('DELIVERY_NUMBER', '')).strip()
            if pd.isna(row.get('DELIVERY_NUMBER')) or delivery_number == '' or delivery_number.lower() == 'nan':
                return None
            return delivery_to_carrier_agreement.get(delivery_number)
        
        # Apply mappings
        lc_dataframe_final['ETOF #'] = lc_dataframe_final.apply(find_etof_number_by_delivery, axis=1)
        matched_count = lc_dataframe_final['ETOF #'].notna().sum()
        print(f"   DEBUG rows with ETOF # after DELIVERY_NUMBER mapping: {matched_count} / {len(lc_dataframe_final)}")
        
        # Map Carrier agreement # from ETOF if available
        if has_carrier_agreement:
            lc_dataframe_final['Carrier agreement #'] = lc_dataframe_final.apply(find_carrier_agreement_by_delivery, axis=1)
        
        # Map LC # from ETOF if available, otherwise create empty
        if delivery_to_lc:
            lc_dataframe_final['LC #'] = lc_dataframe_final.apply(find_lc_number_by_delivery, axis=1)
        else:
            lc_dataframe_final['LC #'] = None
    
    else:
        # No matching strategy available
        raise ValueError("Cannot map ETOF to LC: Neither SHIPMENT_ID nor DELIVERY_NUMBER available in both files")
    
    # Remove rows with empty ETOF # column
    rows_before = len(lc_dataframe_final)
    lc_dataframe_final = lc_dataframe_final[
        lc_dataframe_final['ETOF #'].notna() & 
        (lc_dataframe_final['ETOF #'].astype(str).str.strip() != '') &
        (lc_dataframe_final['ETOF #'].astype(str).str.lower() != 'nan')
    ]
    rows_removed = rows_before - len(lc_dataframe_final)
    if rows_removed > 0:
        print(f"   Removed {rows_removed} rows with empty ETOF # (kept {len(lc_dataframe_final)} rows)")
    
    # Get list of column names
    column_names = lc_dataframe_final.columns.tolist()
    
    return lc_dataframe_final, column_names


def process_lc_etof_mapping(lc_input_path, etof_path, lc_recursive=False):
    """
    Complete workflow: Process LC files and ETOF file.
    
    Maps ETOF # to LC dataframe using SHIPMENT_ID or DELIVERY_NUMBER.
    
    Args:
        lc_input_path (str or list): Path(s) to LC file(s) or folder(s) relative to "input/" folder
        etof_path (str): Path to ETOF file relative to "input/" folder
        lc_recursive (bool): Whether to search recursively in LC folders (default: False)
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: LC dataframe with "LC #" and "ETOF #" columns
            - list: List of column names in the processed dataframe
    """
    # Step 1: Process LC files
    lc_dataframe, lc_column_names = process_lc_input(lc_input_path, recursive=lc_recursive)
    
    # Step 2: Process ETOF file
    etof_dataframe, etof_column_names = process_etof_file(etof_path)
    
    # Step 3: Map ETOF # to LC dataframe (also removes rows with empty ETOF #)
    lc_dataframe_final, lc_column_names = map_etof_to_lc(etof_dataframe, lc_dataframe)
    
    # Save with separate tabs per Carrier agreement #
    save_dataframe_by_carrier_agreement(lc_dataframe_final, "lc_etof_mapping.xlsx")
    
    return lc_dataframe_final, lc_column_names


if __name__ == "__main__":
    lc_input_path = "lc_rhenus.xml"
    etof_path = "etofs_rhenus.xlsx"
    
    df_lc_updated, lc_column_names = process_lc_etof_mapping(
        lc_input_path, 
        etof_path
    )

