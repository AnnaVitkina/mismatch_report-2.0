from pathlib import Path
from typing import List, Union
import xml.etree.ElementTree as ET
import pandas as pd
import os


def find_lc_xml_files(folder_path: str, recursive: bool = False) -> List[str]:
    """
    Find all XML files that start with 'LC' letters in a folder.
    
    Args:
        folder_path: Path to the folder containing XML files
        recursive: If True, search subdirectories as well (default: False)
    
    Returns:
        List of full file paths that are XML files and start with 'LC'
    
    Example:
        # Find LC XML files in a folder
        result = find_lc_xml_files('C:/path/to/folder')
        print(f"Found {len(result)} LC XML files")
        
        # Search recursively in subdirectories
        result = find_lc_xml_files('C:/path/to/folder', recursive=True)
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        raise ValueError(f"Folder does not exist: {folder_path}")
    
    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")
    
    lc_xml_files = []
    
    # Search pattern: '**/*.xml' for recursive, '*.xml' for current directory only
    pattern = '**/*.xml' if recursive else '*.xml'
    
    # Find all XML files in the folder
    for xml_file in folder.glob(pattern):
        filename = xml_file.name
        
        # Check if filename starts with 'LC' (case-insensitive)
        if filename.upper().startswith('LC'):
            lc_xml_files.append(str(xml_file.resolve()))
    
    return sorted(lc_xml_files)


def create_dataframe_from_xml_files(file_paths: List[str]) -> pd.DataFrame:
    """
    Create a DataFrame from XML files where each row represents one ORDER element.
    If a file contains multiple ORDER elements, each will be a separate row.
    
    Args:
        file_paths: List of XML file paths to process
    
    Returns:
        DataFrame with rows = order items (one per ORDER element), columns = XML tags
    
    Example:
        files = find_lc_xml_files('20251007')
        df = create_dataframe_from_xml_files(files)
        print(df.head())
    """
    all_data = []
    
    for file_path in file_paths:
        try:
            # Parse the XML file
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Find ALL ORDER elements (not just the first one)
            order_elements = root.findall('.//ORDER')  # Find all ORDER elements at any level
            
            if order_elements:
                # Process each ORDER element as a separate row
                for order_element in order_elements:
                    # Extract all tag values from the ORDER element
                    order_data = {}
                    
                    # Add filename as a column for reference
                    order_data['filename'] = Path(file_path).name
                    
                    # Extract all child elements (tags) from ORDER
                    for child in order_element:
                        tag_name = child.tag
                        tag_value = child.text if child.text is not None else ''
                        order_data[tag_name] = tag_value
                    
                    all_data.append(order_data)
            else:
                print(f"Warning: No ORDER element found in {file_path}")
                
        except ET.ParseError as e:
            print(f"Error parsing {file_path}: {e}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    # Create DataFrame from all collected data
    if all_data:
        df = pd.DataFrame(all_data)
        return df
    else:
        # Return empty DataFrame with appropriate structure if no data
        return pd.DataFrame()


def process_lc_input(input_path: Union[str, List[str]], recursive: bool = False) -> tuple:
    """
    Process LC XML files from various input types: single file, single folder, or multiple folders.
    
    Args:
        input_path: Can be:
                   - A single file path (string): Path to an LC XML file
                   - A single folder path (string): Path to a folder containing LC XML files
                   - Multiple folder paths (list of strings): List of folder paths
        recursive: If True, search subdirectories when processing folders (default: False)
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: DataFrame with rows = order items (one per file), columns = XML tags
            - list: List of column names in the processed dataframe
    
    Example:
        # Process a single file
        df, columns = process_lc_input("input/LC12345.xml")
        
        # Process a single folder
        df, columns = process_lc_input("input/folder1")
        
        # Process multiple folders
        df, columns = process_lc_input(["input/folder1", "input/folder2", "input/folder3"])
    """
    all_file_paths = []
    
    # Handle input from "input/" folder
    input_folder = "input"
    
    # Normalize input to list
    if isinstance(input_path, str):
        input_paths = [input_path]
    elif isinstance(input_path, list):
        input_paths = input_path
    else:
        raise ValueError(f"Input must be a string (file/folder path) or list of strings (folder paths), got {type(input_path)}")
    
    # Process each input path
    for path in input_paths:
        # Construct full path from input folder
        if not os.path.isabs(path):
            full_path = os.path.join(input_folder, path)
        else:
            full_path = path
        
        path_obj = Path(full_path)
        
        # Check if path exists
        if not path_obj.exists():
            print(f"Warning: Path does not exist: {full_path}")
            continue
        
        # If it's a file
        if path_obj.is_file():
            filename = path_obj.name
            # Check if it's an LC XML file
            if filename.upper().startswith('LC') and filename.upper().endswith('.XML'):
                all_file_paths.append(str(path_obj.resolve()))
            else:
                print(f"Warning: File {full_path} does not appear to be an LC XML file (should start with 'LC' and end with '.xml')")
        
        # If it's a folder
        elif path_obj.is_dir():
            lc_files = find_lc_xml_files(str(full_path), recursive=recursive)
            all_file_paths.extend(lc_files)
        else:
            print(f"Warning: Path is neither a file nor a directory: {full_path}")
    
    # Remove duplicates and sort
    all_file_paths = sorted(list(set(all_file_paths)))
    
    if not all_file_paths:
        print("No LC XML files found to process.")
        return pd.DataFrame(), []
    
    print(f"Found {len(all_file_paths)} LC XML file(s) to process")
    
    # Create DataFrame from all found files
    df = create_dataframe_from_xml_files(all_file_paths)
    
    # Ensure required columns exist (add as empty if missing from XML)
    required_columns = ['SHIP_CITY', 'CUST_CITY', 'SHIP_STATE', 'CUST_STATE']
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
            print(f"   Added missing column: {col}")
    
    # Get list of column names
    column_names = df.columns.tolist()
    
    return df, column_names


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


if __name__ == "__main__":
    folder_path = "lc_rhenus.xml"
    lc_dataframe, lc_column_names = process_lc_input(folder_path)
    save_dataframe_to_excel(lc_dataframe, "lc_processed.xlsx")

