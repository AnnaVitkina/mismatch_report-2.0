"""
Mismatch Analysis - Gradio Interface

This script provides a web interface for the mismatch analysis workflow.
Upload your input files and run the complete analysis.

Required inputs:
- ETOF file
- LC file(s)
- Rate Card file(s)
- Mismatch report
- Shipper name

Optional inputs:
- Order file
- Ignore rate card columns
- Include positive discrepancy
"""

import os
import sys
import shutil
import gradio as gr
from pathlib import Path
from datetime import datetime


def get_script_directory():
    """Get the script directory, handling Colab and exec() environments."""
    # Try __file__ first (normal execution)
    try:
        if '__file__' in globals() and __file__:
            return os.path.dirname(os.path.abspath(__file__))
    except:
        pass
    
    # Check for Colab Mismatch_report folder - explicit paths
    colab_paths = [
        '/content/Mismatch_report',
        '/content/mismatch_report',
        '/content/mismatch_report-2.0',
        '/content/mismatch-report-2.0',
        '/content/Mismatch_report-2.0',
        os.path.join(os.getcwd(), 'Mismatch_report'),
        os.path.join(os.getcwd(), 'mismatch_report-2.0'),
    ]
    for path in colab_paths:
        if os.path.exists(path) and os.path.isdir(path):
            # Verify it has our expected files
            if os.path.exists(os.path.join(path, 'result.py')) or os.path.exists(os.path.join(path, 'matching.py')):
                return path
    
    # Dynamic search: look for any folder in /content that has our files
    content_dir = '/content'
    if os.path.exists(content_dir):
        for item in os.listdir(content_dir):
            item_path = os.path.join(content_dir, item)
            if os.path.isdir(item_path):
                # Check if this folder has our expected files
                if (os.path.exists(os.path.join(item_path, 'part1_etof_file_processing.py')) or 
                    os.path.exists(os.path.join(item_path, 'matching.py'))):
                    print(f"🔍 Found project folder: {item_path}")
                    return item_path
    
    # Check if current directory has our files
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, 'matching.py')) or os.path.exists(os.path.join(cwd, 'result.py')):
        return cwd
    
    # Last resort
    return cwd


def setup_python_path():
    """Setup Python path to include the script directory for imports."""
    try:
        script_dir = get_script_directory()
        
        if script_dir and script_dir not in sys.path:
            sys.path.insert(0, script_dir)
            print(f"📁 Added to Python path: {script_dir}")
        
        # Change to script directory for relative imports
        if script_dir and os.getcwd() != script_dir:
            os.chdir(script_dir)
            print(f"📁 Changed working directory to: {script_dir}")
            
    except Exception as e:
        print(f"⚠️ Warning: Could not auto-detect script directory: {e}")


# Run setup when module is imported
setup_python_path()


# ============================================================
# WORKFLOW FUNCTIONS (from main.py)
# ============================================================

def setup_folders(script_dir):
    """Create input, output, partly_df, and result folders if they don't exist."""
    folders = {
        'input': Path(script_dir) / 'input',
        'output': Path(script_dir) / 'output',
        'partly_df': Path(script_dir) / 'partly_df'
    }
    
    for name, folder in folders.items():
        folder.mkdir(exist_ok=True)
    
    return folders


def log_step(step_num, message, level="info"):
    """Log a step with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = {
        "info": "📄",
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "section": "="*60
    }.get(level, "  ")
    
    if level == "section":
        print(f"\n{prefix}")
        print(f"STEP {step_num}: {message}")
        print(f"{prefix}")
    else:
        print(f"[{timestamp}] {prefix} {message}")


def validate_inputs(etof_file, lc_files, rate_card_files, mismatch_file, shipper_name):
    """Validate that all required inputs are provided."""
    errors = []
    
    if not etof_file:
        errors.append("ETOF file is required")
    elif not os.path.exists(os.path.join("input", etof_file)):
        errors.append(f"ETOF file not found: input/{etof_file}")
    
    if not lc_files:
        errors.append("LC file(s) are required")
    else:
        lc_list = lc_files if isinstance(lc_files, list) else [lc_files]
        for lc_file in lc_list:
            if not os.path.exists(os.path.join("input", lc_file)):
                errors.append(f"LC file not found: input/{lc_file}")
    
    if not rate_card_files:
        errors.append("Rate Card file(s) are required")
    else:
        rc_list = rate_card_files if isinstance(rate_card_files, list) else [rate_card_files]
        for rc_file in rc_list:
            if not os.path.exists(os.path.join("input", rc_file)):
                errors.append(f"Rate Card file not found: input/{rc_file}")
    
    if not mismatch_file:
        errors.append("Mismatch report file is required")
    elif not os.path.exists(os.path.join("input", mismatch_file)):
        errors.append(f"Mismatch file not found: input/{mismatch_file}")
    
    if not shipper_name:
        errors.append("Shipper name is required")
    
    return errors


def run_workflow(
    etof_file,
    lc_files,
    rate_card_files,
    mismatch_file,
    shipper_name,
    order_file=None,
    ignore_rate_card_columns=None,
    include_positive_discrepancy=False,
    script_dir=None,
    extra_columns=None
):
    """
    Run the complete mismatch analysis workflow.
    
    Args:
        etof_file: Path to ETOF file relative to input/ folder
        lc_files: Path(s) to LC file(s) relative to input/ folder
        rate_card_files: Path(s) to Rate Card file(s) relative to input/ folder
        mismatch_file: Path to mismatch report file relative to input/ folder
        shipper_name: Shipper identifier
        order_file: Optional path to order files export
        ignore_rate_card_columns: Optional list of column names to ignore
        include_positive_discrepancy: If True, include positive discrepancies
        script_dir: Script directory for path resolution
        extra_columns: Optional list of column names to add from lc_etof_with_comments
    
    Returns:
        str: Path to the final result file, or None if workflow failed
    """
    print("\n" + "="*80)
    print("MISMATCH ANALYSIS WORKFLOW")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Setup folders
    if script_dir is None:
        script_dir = get_script_directory()
    
    folders = setup_folders(script_dir)
    
    # Change to script directory
    original_cwd = os.getcwd()
    if os.getcwd() != script_dir:
        os.chdir(script_dir)
        print(f"   Changed to script directory: {script_dir}")
    
    # Add to sys.path for imports
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
        print(f"   Added to Python path: {script_dir}")
    
    try:
        # Validate inputs
        log_step(0, "Validating inputs...", "info")
        log_step(0, f"  etof_file: {etof_file}", "info")
        log_step(0, f"  lc_files: {lc_files}", "info")
        log_step(0, f"  rate_card_files: {rate_card_files}", "info")
        log_step(0, f"  mismatch_file: {mismatch_file}", "info")
        log_step(0, f"  shipper_name: {shipper_name}", "info")
        log_step(0, f"  Current dir: {os.getcwd()}", "info")
        log_step(0, f"  Input folder contents: {os.listdir('input') if os.path.exists('input') else 'NOT FOUND'}", "info")
        
        validation_errors = validate_inputs(etof_file, lc_files, rate_card_files, mismatch_file, shipper_name)
        
        if validation_errors:
            for error in validation_errors:
                log_step(0, error, "error")
            return None
        
        log_step(0, "All inputs validated", "success")
        
        # Convert to lists for consistent handling
        lc_list = lc_files if isinstance(lc_files, list) else [lc_files]
        rc_list = rate_card_files if isinstance(rate_card_files, list) else [rate_card_files]
        
        # ========================================
        # STEP 1: ETOF Processing
        # ========================================
        log_step(1, "ETOF FILE PROCESSING", "section")
        try:
            from part1_etof_file_processing import process_etof_file, save_dataframe_to_excel
            log_step(1, f"Processing ETOF file: {etof_file}", "info")
            etof_df, etof_columns = process_etof_file(etof_file)
            save_dataframe_to_excel(etof_df, "etof_processed.xlsx")
            log_step(1, f"ETOF processed: {etof_df.shape[0]} rows, {etof_df.shape[1]} columns", "success")
        except Exception as e:
            log_step(1, f"ETOF processing failed: {e}", "error")
            raise
        
        # ========================================
        # STEP 2: LC Processing
        # ========================================
        log_step(2, "LC FILE PROCESSING", "section")
        try:
            from part2_lc_processing import process_lc_input, save_dataframe_to_excel
            log_step(2, f"Processing {len(lc_list)} LC file(s)...", "info")
            lc_input_param = lc_list if len(lc_list) > 1 else lc_list[0]
            lc_df, lc_columns = process_lc_input(lc_input_param, recursive=False)
            save_dataframe_to_excel(lc_df, "lc_processed.xlsx")
            log_step(2, f"LC processed: {lc_df.shape[0]} rows, {lc_df.shape[1]} columns", "success")
        except Exception as e:
            log_step(2, f"LC processing failed: {e}", "error")
            raise
        
        # ========================================
        # STEP 3: Rate Card Processing
        # ========================================
        log_step(3, "RATE CARD PROCESSING", "section")
        try:
            from part4_rate_card_processing import process_multiple_rate_cards
            log_step(3, f"Processing {len(rc_list)} Rate Card file(s)...", "info")
            rc_results = process_multiple_rate_cards(rc_list)
            log_step(3, f"Rate Cards processed: {len(rc_results)} files", "success")
        except Exception as e:
            log_step(3, f"Rate Card processing failed: {e}", "error")
            raise
        
        # ========================================
        # STEP 4: LC-ETOF Mapping
        # ========================================
        log_step(4, "LC-ETOF MAPPING", "section")
        try:
            from part7_optional_order_lc_etof_mapping import process_lc_etof_mapping
            log_step(4, "Creating LC-ETOF mapping...", "info")
            lc_input_param = lc_list if len(lc_list) > 1 else lc_list[0]
            mapping_df, mapping_columns = process_lc_etof_mapping(
                lc_input_path=lc_input_param,
                etof_path=etof_file
            )
            log_step(4, f"Mapping completed: {mapping_df.shape[0]} rows", "success")
        except Exception as e:
            log_step(4, f"Mapping failed: {e}", "warning")
        
        # ========================================
        # STEP 5: Vocabulary Mapping
        # ========================================
        log_step(5, "VOCABULARY MAPPING", "section")
        try:
            from vocabular import process_all_rate_cards_from_mapping_file
            log_step(5, "Running vocabulary mapping for all agreements...", "info")
            
            # Parse ignore columns
            ignore_cols = None
            if ignore_rate_card_columns:
                if isinstance(ignore_rate_card_columns, str):
                    ignore_cols = [col.strip() for col in ignore_rate_card_columns.split(',') if col.strip()]
                else:
                    ignore_cols = ignore_rate_card_columns
            
            # Use lc_etof_mapping.xlsx (created in Step 4) to process all agreements
            vocab_results = process_all_rate_cards_from_mapping_file(
                mapping_filename="lc_etof_mapping.xlsx",
                ignore_rate_card_columns=ignore_cols,
                shipper_id=shipper_name
            )
            
            if vocab_results:
                log_step(5, f"Vocabulary mapping completed: {len(vocab_results)} agreement(s)", "success")
                for agreement in vocab_results.keys():
                    log_step(5, f"  - {agreement}_vocabulary_mapping.xlsx", "info")
            else:
                log_step(5, "No vocabulary mappings created", "warning")
        except Exception as e:
            log_step(5, f"Vocabulary mapping failed: {e}", "warning")
            import traceback
            traceback.print_exc()
        
        # ========================================
        # STEP 6: Matching
        # ========================================
        log_step(6, "MATCHING", "section")
        try:
            from matching import run_matching_all_agreements, create_lc_etof_with_comments
            log_step(6, "Running matching process for all agreements...", "info")
            matching_results = run_matching_all_agreements()
            if matching_results:
                log_step(6, f"Matching completed: {len(matching_results)} agreement(s) matched", "success")
                for agreement, file_path in matching_results.items():
                    log_step(6, f"  - {agreement}: {file_path}", "info")
            else:
                log_step(6, "No agreements matched", "warning")
            
            # Create lc_etof_with_comments.xlsx from matched files
            log_step(6, "Creating lc_etof_with_comments.xlsx...", "info")
            comments_file = create_lc_etof_with_comments()
            if comments_file:
                log_step(6, f"Created: {comments_file}", "success")
            else:
                log_step(6, "Failed to create lc_etof_with_comments.xlsx", "warning")
        except Exception as e:
            log_step(6, f"Matching failed: {e}", "warning")
        
        # ========================================
        # STEP 7: Mismatch Report
        # ========================================
        log_step(7, "MISMATCH REPORT", "section")
        try:
            from mismatch_report import main as mismatch_report_main
            log_step(7, f"Generating mismatch report (include_positive={include_positive_discrepancy})...", "info")
            mismatch_df = mismatch_report_main(include_positive_discrepancy=include_positive_discrepancy)
            log_step(7, f"Mismatch report generated: {len(mismatch_df)} rows", "success")
        except Exception as e:
            log_step(7, f"Mismatch report failed: {e}", "warning")
        
        # ========================================
        # STEP 8: Rate Costs Analysis
        # ========================================
        log_step(8, "RATE COSTS ANALYSIS", "section")
        try:
            from rate_costs import process_multiple_rate_cards as rate_costs_process
            log_step(8, "Analyzing rate costs...", "info")
            rate_costs_results = rate_costs_process(rc_list)
            log_step(8, f"Rate costs analyzed: {len(rate_costs_results)} files", "success")
        except Exception as e:
            log_step(8, f"Rate costs analysis failed: {e}", "warning")
        
        # ========================================
        # STEP 9: Accessorial Costs Analysis
        # ========================================
        log_step(9, "ACCESSORIAL COSTS ANALYSIS", "section")
        try:
            from rate_accesorial_costs import process_multiple_rate_cards as accessorial_process
            log_step(9, "Analyzing accessorial costs...", "info")
            accessorial_results = accessorial_process(rc_list)
            log_step(9, f"Accessorial costs analyzed: {len(accessorial_results)} files", "success")
        except Exception as e:
            log_step(9, f"Accessorial costs analysis failed: {e}", "warning")
        
        # ========================================
        # STEP 10: Mismatches Filing
        # ========================================
        log_step(10, "MISMATCHES FILING", "section")
        try:
            from mismacthes_filing import main as mismatches_filing_main
            log_step(10, f"Filing mismatches (include_positive={include_positive_discrepancy})...", "info")
            filing_result = mismatches_filing_main(include_positive_discrepancy=include_positive_discrepancy)
            log_step(10, "Mismatches filed", "success")
        except Exception as e:
            log_step(10, f"Mismatches filing failed: {e}", "warning")
        
        # ========================================
        # STEP 11: Conditions Checking
        # ========================================
        log_step(11, "CONDITIONS CHECKING", "section")
        try:
            from conditions_checking import main as conditions_main
            log_step(11, "Checking conditions...", "info")
            conditions_result = conditions_main(debug=False)
            log_step(11, f"Conditions checked: {len(conditions_result)} rows", "success")
        except Exception as e:
            log_step(11, f"Conditions checking failed: {e}", "error")
            raise
        
        # ========================================
        # STEP 12: Cleaning and Final Result
        # ========================================
        log_step(12, "CLEANING AND FINAL RESULT", "section")
        try:
            from cleaning import main as cleaning_main
            log_step(12, "Creating final result...", "info")
            if extra_columns:
                log_step(12, f"Extra columns to add: {extra_columns}", "info")
            result_path = cleaning_main(extra_columns=extra_columns)
            log_step(12, f"Final result created: {result_path}", "success")
        except Exception as e:
            log_step(12, f"Cleaning failed: {e}", "error")
            raise
        
        # ========================================
        # WORKFLOW COMPLETE
        # ========================================
        print("\n" + "="*80)
        print("WORKFLOW COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nResult file: {result_path}")
        print("="*80)
        
        return str(result_path)
        
    except Exception as e:
        print("\n" + "="*80)
        print("WORKFLOW FAILED")
        print("="*80)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        # Restore original working directory
        os.chdir(original_cwd)


# ============================================================
# GRADIO INTERFACE
# ============================================================

def run_mismatch_analysis_gradio(
    etof_file,
    lc_files,
    rate_card_files,
    mismatch_file,
    shipper_name,
    order_file=None,
    ignore_rate_card_columns=None,
    include_positive_discrepancy=False,
    extra_columns=None
):
    """
    Main workflow for Gradio interface.
    Accepts uploaded files and user input; returns downloadable file and status messages.
    """
    # Capture status messages
    status_messages = []
    errors = []
    warnings = []
    
    def log_status(msg, level="info"):
        """Log status messages with different levels"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
        except:
            timestamp = ""
        
        formatted_msg = f"[{timestamp}] {msg}"
        status_messages.append(formatted_msg)
        
        if level == "error":
            errors.append(msg)
        elif level == "warning":
            warnings.append(msg)
        
        print(formatted_msg)
    
    def _handle_upload(uploaded, allow_multiple=False):
        """Handle file upload - convert Gradio file objects to paths."""
        if uploaded is None:
            return None if not allow_multiple else []
        
        if isinstance(uploaded, list):
            if not allow_multiple:
                return _handle_upload(uploaded[0] if uploaded else None, allow_multiple=False)
            result = []
            for item in uploaded:
                if item is None:
                    continue
                if hasattr(item, "name"):
                    result.append(item.name)
                elif isinstance(item, str):
                    result.append(item)
            return result if result else []
        
        if hasattr(uploaded, "name"):
            return uploaded.name
        if isinstance(uploaded, str):
            return uploaded
        return None if not allow_multiple else []
    
    # Convert file paths
    etof_path = _handle_upload(etof_file)
    lc_paths = _handle_upload(lc_files, allow_multiple=True)
    rate_card_paths = _handle_upload(rate_card_files, allow_multiple=True)
    mismatch_path = _handle_upload(mismatch_file)
    order_path = _handle_upload(order_file)
    
    # Validate required fields
    if not etof_path:
        error_msg = "❌ Error: ETOF File is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not lc_paths:
        error_msg = "❌ Error: LC File(s) are required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not rate_card_paths:
        error_msg = "❌ Error: Rate Card File(s) are required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not mismatch_path:
        error_msg = "❌ Error: Mismatch Report is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not shipper_name or not shipper_name.strip():
        error_msg = "❌ Error: Shipper Name is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    log_status("✅ Validation passed. Starting workflow...", "info")
    
    # Create directories
    script_dir = get_script_directory()
    
    input_dir = os.path.join(script_dir, "input")
    output_dir = os.path.join(script_dir, "output")
    partly_df_dir = os.path.join(script_dir, "partly_df")
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(partly_df_dir, exist_ok=True)
    
    # Copy uploaded files to input directory
    etof_filename = None
    lc_filenames = []
    rate_card_filenames = []
    mismatch_filename = None
    order_filename = None
    
    try:
        # ETOF file
        if etof_path:
            etof_filename = os.path.basename(etof_path)
            # Standardize name
            etof_ext = os.path.splitext(etof_filename)[1] or ".xlsx"
            etof_filename = f"etofs{etof_ext}"
            input_etof_path = os.path.join(input_dir, etof_filename)
            shutil.copy2(etof_path, input_etof_path)
            log_status(f"✓ ETOF file ready: {etof_filename}", "info")
        
        # LC files
        for idx, lc_path in enumerate(lc_paths):
            if lc_path:
                lc_filename = os.path.basename(lc_path)
                input_lc_path = os.path.join(input_dir, lc_filename)
                shutil.copy2(lc_path, input_lc_path)
                lc_filenames.append(lc_filename)
        log_status(f"✓ {len(lc_filenames)} LC file(s) ready", "info")
        
        # Rate card files
        for idx, rc_path in enumerate(rate_card_paths):
            if rc_path:
                rc_filename = os.path.basename(rc_path)
                input_rc_path = os.path.join(input_dir, rc_filename)
                shutil.copy2(rc_path, input_rc_path)
                rate_card_filenames.append(rc_filename)
        log_status(f"✓ {len(rate_card_filenames)} Rate Card file(s) ready", "info")
        
        # Mismatch file
        if mismatch_path:
            mismatch_filename = os.path.basename(mismatch_path)
            # Standardize name
            mismatch_ext = os.path.splitext(mismatch_filename)[1] or ".xlsx"
            mismatch_filename = f"mismatch{mismatch_ext}"
            input_mismatch_path = os.path.join(input_dir, mismatch_filename)
            shutil.copy2(mismatch_path, input_mismatch_path)
            log_status(f"✓ Mismatch file ready: {mismatch_filename}", "info")
        
        # Order file (optional)
        if order_path:
            order_filename = os.path.basename(order_path)
            input_order_path = os.path.join(input_dir, order_filename)
            shutil.copy2(order_path, input_order_path)
            log_status(f"✓ Order file ready: {order_filename}", "info")
            
    except Exception as e:
        error_msg = f"❌ Error copying files: {e}"
        log_status(error_msg, "error")
        return None, error_msg
    
    # Run the workflow
    final_file_path = None
    try:
        log_status("🚀 Starting mismatch analysis workflow...", "info")
        
        # Parse ignore columns
        ignore_cols = None
        if ignore_rate_card_columns and ignore_rate_card_columns.strip():
            ignore_cols = [col.strip() for col in ignore_rate_card_columns.split(',') if col.strip()]
        
        # Prepare LC files parameter
        lc_param = lc_filenames if len(lc_filenames) > 1 else (lc_filenames[0] if lc_filenames else None)
        
        log_status(f"   ETOF: {etof_filename}", "info")
        log_status(f"   LC: {lc_param}", "info")
        log_status(f"   Rate Cards: {rate_card_filenames}", "info")
        log_status(f"   Mismatch: {mismatch_filename}", "info")
        log_status(f"   Shipper: {shipper_name.strip()}", "info")
        
        # Run the workflow
        # Parse extra columns (already a list from CheckboxGroup)
        extra_cols = None
        if extra_columns and len(extra_columns) > 0:
            extra_cols = extra_columns  # Already a list
        
        result_file = run_workflow(
            etof_file=etof_filename,
            lc_files=lc_param,
            rate_card_files=rate_card_filenames,
            mismatch_file=mismatch_filename,
            shipper_name=shipper_name.strip(),
            order_file=order_filename,
            ignore_rate_card_columns=ignore_cols,
            include_positive_discrepancy=include_positive_discrepancy,
            script_dir=script_dir,
            extra_columns=extra_cols
        )
        
        log_status(f"   Workflow returned: {result_file}", "info")
        
        if result_file and os.path.exists(result_file):
            log_status(f"✅ Workflow completed successfully!", "info")
            log_status(f"📁 Result file: {result_file}", "info")
            
            # Copy to output directory for download
            output_result = os.path.join(output_dir, "Result.xlsx")
            shutil.copy2(result_file, output_result)
            
            final_file_path = output_result
        elif result_file:
            log_status(f"⚠️ Result file path returned but file doesn't exist: {result_file}", "warning")
            # Try to find the result file in expected locations
            possible_paths = [
                os.path.join(output_dir, "result.xlsx"),
                os.path.join(script_dir, "output", "result.xlsx"),
                os.path.join(partly_df_dir, "conditions_checked.xlsx"),
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    log_status(f"   Found result at: {path}", "info")
                    output_result = os.path.join(output_dir, "Result.xlsx")
                    shutil.copy2(path, output_result)
                    final_file_path = output_result
                    break
        else:
            log_status("⚠️ Workflow returned None - check error messages above", "warning")
            
    except Exception as e:
        import traceback
        error_msg = f"❌ Workflow failed: {e}"
        log_status(error_msg, "error")
        log_status(f"Traceback: {traceback.format_exc()}", "error")
    
    # Prepare status summary
    status_summary = []
    status_summary.append("=" * 60)
    status_summary.append("WORKFLOW SUMMARY")
    status_summary.append("=" * 60)
    status_summary.append("")
    
    if final_file_path and os.path.exists(final_file_path):
        status_summary.append(f"✅ SUCCESS: Output file created")
        status_summary.append(f"   Location: {final_file_path}")
    else:
        status_summary.append(f"❌ Workflow did not complete successfully")
    
    status_summary.append("")
    
    if errors:
        status_summary.append(f"❌ ERRORS ({len(errors)}):")
        for i, error in enumerate(errors[:10], 1):
            status_summary.append(f"  {i}. {error}")
        if len(errors) > 10:
            status_summary.append(f"  ... and {len(errors) - 10} more errors")
        status_summary.append("")
    
    if warnings:
        status_summary.append(f"⚠️ WARNINGS ({len(warnings)}):")
        for i, warning in enumerate(warnings[:10], 1):
            status_summary.append(f"  {i}. {warning}")
        if len(warnings) > 10:
            status_summary.append(f"  ... and {len(warnings) - 10} more warnings")
        status_summary.append("")
    
    # Add key status messages
    key_messages = [msg for msg in status_messages if any(keyword in msg for keyword in 
                    ['✓', '✅', '❌', '⚠️', 'Error', 'Warning', 'SUCCESS', 'completed', 'failed', 'STEP'])]
    
    if key_messages:
        status_summary.append("Key Steps:")
        status_summary.append("-" * 60)
        status_summary.extend(key_messages[-20:])
    
    status_text = "\n".join(status_summary)
    
    return (final_file_path, status_text) if final_file_path and os.path.exists(final_file_path) else (None, status_text)


# ---- Gradio UI Definition ----
with gr.Blocks(title="Mismatch Analyzer", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📊 Mismatch Analyzer")
    gr.Markdown("### Analyze cost mismatches against rate cards")
    
    with gr.Accordion("📖 Instructions & Information", open=False):
        gr.Markdown("""
        ## How to Use This Workflow
        
        ### Step 1: Upload Required Files
        - **ETOF File** (Required): Excel file containing ETOF shipment data (.xlsx)
        - **LC File(s)** (Required): XML files with LC data (can upload multiple)
        - **Rate Card File(s)** (Required): Excel file(s) containing rate card data (.xlsx)
        - **Mismatch Report** (Required): Excel file with mismatch data (.xlsx)
        - **Shipper Name** (Required): Enter the shipper identifier (e.g., "dairb")
        
        ### Step 2: Upload Optional Files
        - **Order Files Export** (Optional): Excel file with order data mapping
        
        ### Step 3: Configure Options
        - **Ignore Rate Card Columns**: Comma-separated column names to exclude
        - **Include Positive Discrepancy**: Check to include positive discrepancies in report
        
        ### Step 4: Run Workflow
        - Click "🚀 Run Analysis" button
        - Wait for processing to complete
        - Check the Status section for any issues
        - Download the Result.xlsx file when ready
        
        ## Workflow Steps
        1. **ETOF Processing**: Process ETOF shipment file
        2. **LC Processing**: Process LC XML file(s)
        3. **Rate Card Processing**: Process rate card file(s)
        4. **Mapping**: Create Order-LC-ETOF mapping
        5. **Vocabulary Mapping**: Map and rename columns
        6. **Matching**: Match shipments with rate card lanes
        7. **Mismatch Report**: Generate mismatch report
        8. **Rate Costs Analysis**: Analyze rate costs
        9. **Accessorial Costs**: Analyze accessorial costs
        10. **Mismatches Filing**: File mismatches
        11. **Conditions Checking**: Check conditions and add reasons
        12. **Cleaning**: Create final cleaned result
        
        ## Output File
        - **result.xlsx**: Final cleaned result with conditions checked
          - Separate tabs per carrier agreement
          - Pivot summary tabs
          - Color-coded by cost type
        """)
    
    gr.Markdown("---")
    gr.Markdown("### 📁 Required Files")
    
    with gr.Row():
        etof_input = gr.File(
            label="ETOF File (.xlsx) *Required",
            file_types=[".xlsx", ".xls"]
        )
        mismatch_input = gr.File(
            label="Mismatch Report (.xlsx) *Required",
            file_types=[".xlsx", ".xls"]
        )
        shipper_input = gr.Textbox(
            label="Shipper Name *Required",
            placeholder="e.g., dairb, apple, shipper"
        )
    
    with gr.Row():
        lc_input = gr.File(
            label="LC Files (.xml) *Required - can upload multiple",
            file_types=[".xml"],
            file_count="multiple"
        )
        rate_card_input = gr.File(
            label="Rate Card Files (.xlsx) *Required - can upload multiple",
            file_types=[".xlsx", ".xls"],
            file_count="multiple"
        )
    
    gr.Markdown("---")
    gr.Markdown("### 📁 Optional Files & Settings")
    
    with gr.Row():
        order_input = gr.File(
            label="Order Files Export (.xlsx) - Optional",
            file_types=[".xlsx", ".xls"]
        )
        ignore_columns_input = gr.Textbox(
            label="Ignore Rate Card Columns (Optional)",
            placeholder="Column1, Column2, Column3",
            info="Comma-separated column names to exclude from processing"
        )
        include_positive_input = gr.Checkbox(
            label="Include Positive Discrepancy",
            value=False,
            info="If checked, includes both positive and negative discrepancies"
        )
    
    with gr.Row():
        extra_columns_input = gr.CheckboxGroup(
            label="Extra Columns to Add (Optional)",
            choices=[
                "Invoice entity",
                "Carrier name",
                "Destination postal code",
                "Origin postal code",
                "Destination airport",
                "Equipment type",
                "Origin airport",
                "Business unit name",
                "Transport mode",
                "LDM",
                "CBM",
                "Weight",
                "DANGEROUS Goods",
                "Charge weight",
                "House bill",
                "Master bill",
                "Roundtrip",
            ],
            info="Select columns to extract additionally and add to result (matched by ETOF Number)"
        )
    
    gr.Markdown("---")
    
    launch_button = gr.Button("🚀 Run Analysis", variant="primary", size="lg")
    
    with gr.Row():
        output_file = gr.File(label="📥 Result.xlsx (Download)")
        status_output = gr.Textbox(
            label="📋 Status & Logs",
            lines=25,
            max_lines=40,
            interactive=False,
            placeholder="Workflow status and messages will appear here...",
            show_copy_button=True
        )
    
    def launch_workflow(etof_file, lc_files, rate_card_files, mismatch_file, 
                       shipper_name, order_file, ignore_columns, include_positive, extra_columns):
        try:
            result_file, status_text = run_mismatch_analysis_gradio(
                etof_file=etof_file,
                lc_files=lc_files,
                rate_card_files=rate_card_files,
                mismatch_file=mismatch_file,
                shipper_name=shipper_name,
                order_file=order_file,
                ignore_rate_card_columns=ignore_columns,
                include_positive_discrepancy=include_positive,
                extra_columns=extra_columns
            )
            return result_file, status_text
        except Exception as e:
            import traceback
            error_details = f"❌ CRITICAL ERROR:\n{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return None, error_details
    
    launch_button.click(
        launch_workflow,
        inputs=[
            etof_input, lc_input, rate_card_input, mismatch_input,
            shipper_input, order_input, ignore_columns_input, include_positive_input,
            extra_columns_input
        ],
        outputs=[output_file, status_output]
    )


if __name__ == "__main__":
    # Create folders when program starts
    script_dir = get_script_directory()
    
    input_dir = os.path.join(script_dir, "input")
    output_dir = os.path.join(script_dir, "output")
    partly_df_dir = os.path.join(script_dir, "partly_df")
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(partly_df_dir, exist_ok=True)
    
    print(f"📁 Input folder: {input_dir}")
    print(f"📁 Output folder: {output_dir}")
    
    # Check if running in Colab
    in_colab = 'google.colab' in sys.modules
    
    if in_colab:
        print("🚀 Launching Gradio interface for Google Colab...")
        demo.launch(server_name="0.0.0.0", share=False, debug=False, show_error=True)
    else:
        print("🚀 Launching Gradio interface locally...")
        print(f"💡 Upload your files through the web interface")
        demo.launch(server_name="127.0.0.1", share=False)
