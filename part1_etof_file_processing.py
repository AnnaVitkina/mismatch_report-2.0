import pandas as pd
import os
from pathlib import Path


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def process_etof_file(file_path):
    """
    Process an ETOF Excel file from the input folder.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder (e.g., "etof_file.xlsx")
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: Processed pandas DataFrame with specified columns removed
            - list: List of column names in the processed dataframe
    """
    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Read Excel file (skip first row)
    df_etofs = pd.read_excel(full_path, skiprows=1)
    
    # Rename duplicate columns
    new_column_names = {
        'Country code': 'Origin Country',
        'Postal code': 'Origin postal code',
        'Airport': 'Origin airport',
        'City': 'Origin city',
        'Country code.1': 'Destination Country',
        'Postal code.1': 'Destination postal code',
        'Airport.1': 'Destination airport',
        'City.1': 'Destination city',
    }
    df_etofs = df_etofs.rename(columns=new_column_names, inplace=False)
    

    columns_to_remove = ['Match', 'Approve', 'Calculation', 'State', 'Issue',
                         'Currency', 'Value', 'Currency.1', 'Value.1', 'Currency.2', 'Value.2']
    # Remove specified columns
    # Only remove columns that actually exist in the dataframe
    columns_to_drop = [col for col in columns_to_remove if col in df_etofs.columns]
    if columns_to_drop:
        df_etofs = df_etofs.drop(columns=columns_to_drop)
    
    # Get list of column names
    column_names = df_etofs.columns.tolist()

    def extract_country_code(country_string):
        """Extract the two-letter country code from a country string."""
        if isinstance(country_string, str) and ' - ' in country_string:
            return country_string.split(' - ')[0]
        return country_string

    df_etofs['Origin Country'] = df_etofs['Origin Country'].apply(extract_country_code)
    df_etofs['Destination Country'] = df_etofs['Destination Country'].apply(extract_country_code)

    def extract_carrier_agreement(agreement_string):
        """Extract the carrier agreement number (e.g., 'RA20220420022') from the full string.
        Input: 'RA20220420022 (v.12) - Active'
        Output: 'RA20220420022'
        """
        if isinstance(agreement_string, str):
            # Split by space and take the first part (the RA number)
            return agreement_string.split(' ')[0]
        return agreement_string

    if 'Carrier agreement #' in df_etofs.columns:
        df_etofs['Carrier agreement #'] = df_etofs['Carrier agreement #'].apply(extract_carrier_agreement)

    # Check if SHIPMENT_ID column is missing or has no values
    shipment_id_missing = (
        'SHIPMENT_ID' not in df_etofs.columns or 
        df_etofs['SHIPMENT_ID'].isna().all() or 
        (df_etofs['SHIPMENT_ID'].astype(str).str.strip() == '').all()
    )
    
    if shipment_id_missing:
        print(f"   SHIPMENT_ID is missing in ETOF, looking for mismatch file...")
        
        # Find mismatch file in input folder (any file containing 'mismatch' in the name)
        try:
            if os.path.exists(input_folder):
                all_files = os.listdir(input_folder)
                mismatch_files = [f for f in all_files 
                                 if 'mismatch_rhenus' in f.lower() and (f.endswith('.xlsx') or f.endswith('.xls'))]
                print(f"   Input folder has {len(all_files)} files, found {len(mismatch_files)} mismatch file(s)")
            else:
                print(f"   WARNING: Input folder '{input_folder}' does not exist!")
                mismatch_files = []
        except Exception as e:
            print(f"   ERROR listing input folder: {e}")
            mismatch_files = []
        
        if mismatch_files:
            # Use the first matching file found
            mismatch_path = os.path.join(input_folder, mismatch_files[0])
            print(f"   Found mismatch file: {mismatch_files[0]}")
            
            try:
                df_mismatch = pd.read_excel(mismatch_path)
                print(f"   Mismatch file has {len(df_mismatch)} rows, columns: {df_mismatch.columns.tolist()}")
                
                # Create mapping from ETOF_NUMBER to SHIPMENT_ID
                if 'ETOF_NUMBER' in df_mismatch.columns and 'SHIPMENT_ID' in df_mismatch.columns:
                    # Drop duplicates to avoid issues with set_index
                    df_mismatch_unique = df_mismatch[['ETOF_NUMBER', 'SHIPMENT_ID']].drop_duplicates(subset='ETOF_NUMBER')
                    
                    # Convert ETOF_NUMBER to string for consistent matching
                    df_mismatch_unique['ETOF_NUMBER'] = df_mismatch_unique['ETOF_NUMBER'].astype(str)
                    etof_to_shipment = df_mismatch_unique.set_index('ETOF_NUMBER')['SHIPMENT_ID'].to_dict()
                    print(f"   Created mapping with {len(etof_to_shipment)} unique ETOF->SHIPMENT_ID entries")
                    
                    # Show sample mapping keys for debugging
                    sample_keys = list(etof_to_shipment.keys())[:3]
                    print(f"   Sample mismatch ETOF_NUMBER values: {sample_keys}")
                    
                    # Map SHIPMENT_ID using ETOF # column from etof file
                    if 'ETOF #' in df_etofs.columns:
                        # Show sample ETOF # values for comparison
                        sample_etof = df_etofs['ETOF #'].head(3).tolist()
                        print(f"   Sample ETOF 'ETOF #' values: {sample_etof}")
                        
                        # Convert to string for consistent matching
                        df_etofs['SHIPMENT_ID'] = df_etofs['ETOF #'].astype(str).map(etof_to_shipment)
                        mapped_count = df_etofs['SHIPMENT_ID'].notna().sum()
                        print(f"   Mapped SHIPMENT_ID for {mapped_count}/{len(df_etofs)} rows")
                    else:
                        print(f"   WARNING: 'ETOF #' column not found in ETOF dataframe")
                else:
                    print(f"   WARNING: Mismatch file missing required columns (ETOF_NUMBER and/or SHIPMENT_ID)")
            except Exception as e:
                print(f"   ERROR reading mismatch file: {e}")
        else:
            print(f"   No mismatch file found in input folder")
    
    # Debug: show final columns
    print(f"   Final ETOF columns ({len(df_etofs.columns)}): {df_etofs.columns.tolist()}")

    return df_etofs, column_names

if __name__ == "__main__":
    etof_dataframe, etof_column_names = process_etof_file('etofs_rhenus.xlsx')
    save_dataframe_to_excel(etof_dataframe, "etof_rhenus.xlsx")
    print(etof_dataframe.head())
