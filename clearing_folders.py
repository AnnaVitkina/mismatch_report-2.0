import os
import shutil

def clean_folder(folder_path):
    """
    Deletes all files and subfolders in the specified folder.
    Does not delete the folder itself.
    Returns a list of deleted items.
    """
    deleted_items = []
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    deleted_items.append(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    deleted_items.append(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
    return deleted_items

def clean_input_and_output_folders():
    """
    Cleans the 'input', 'output', and 'partly_df' folders in the current directory.
    Handles Colab environment where __file__ is not defined.
    """
    # Handle Colab environment where __file__ is not defined
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # In Colab or interactive environments, use current working directory
        script_dir = os.getcwd()
    
    input_folder = os.path.join(script_dir, "input")
    output_folder = os.path.join(script_dir, "output")
    partly_df_folder = os.path.join(script_dir, "partly_df")

    deleted_input = clean_folder(input_folder)
    deleted_output = clean_folder(output_folder)
    deleted_partly_df = clean_folder(partly_df_folder)

    print(f"Deleted from input: {len(deleted_input)} item(s)")
    print(f"Deleted from output: {len(deleted_output)} item(s)")
    print(f"Deleted from partly_df: {len(deleted_partly_df)} item(s)")
    
    if deleted_input:
        print(f"  Input items: {deleted_input[:5]}{'...' if len(deleted_input) > 5 else ''}")
    if deleted_output:
        print(f"  Output items: {deleted_output[:5]}{'...' if len(deleted_output) > 5 else ''}")
    if deleted_partly_df:
        print(f"  Partly_df items: {deleted_partly_df[:5]}{'...' if len(deleted_partly_df) > 5 else ''}")

# Example usage:
if __name__ == "__main__":
    clean_input_and_output_folders()
