import argparse
import json
import logging
import os
import sqlite3
import sys
import time
import textwrap
import csv
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from multiprocessing import Manager, Pool

from tabulate import tabulate
from tqdm import tqdm
from unstract.api_deployments.client import APIDeploymentsClient

logger = logging.getLogger(__name__)


# Dataclass for arguments
@dataclass
class Arguments:
    api_endpoint: str
    api_key: str
    api_timeout: int = 10
    poll_interval: int = 5
    input_folder_path: str = ""
    db_path: str = ""
    parallel_call_count: int = 5
    recurse_input_folder: bool = False
    retry_failed: bool = False
    retry_pending: bool = False
    skip_pending: bool = False
    skip_unprocessed: bool = False
    log_level: str = "INFO"
    print_report: bool = False
    csv_report: str = ""
    include_metadata: bool = True
    verify: bool = True


# Initialize SQLite DB
def init_db(args: Arguments):
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()

    # Create the table if it doesn't exist
    c.execute(
        """CREATE TABLE IF NOT EXISTS file_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT UNIQUE,
                    execution_status TEXT,
                    result TEXT,
                    time_taken REAL,
                    status_code INTEGER,
                    status_api_endpoint TEXT,
                    total_embedding_cost REAL,
                    total_embedding_tokens INTEGER,
                    total_llm_cost REAL,
                    total_llm_tokens INTEGER,
                    error_message TEXT,
                    updated_at TEXT,
                    created_at TEXT
                )"""
    )

    # Check existing columns in file_status table
    c.execute("PRAGMA table_info(file_status)")
    existing_columns = {row[1] for row in c.fetchall()}

    # Columns to add
    new_columns = {
        "total_embedding_cost": "REAL",
        "total_embedding_tokens": "INTEGER",
        "total_llm_cost": "REAL",
        "total_llm_tokens": "INTEGER",
        "error_message": "TEXT",
    }

    # Add missing columns
    for column, col_type in new_columns.items():
        if column not in existing_columns:
            c.execute(f"ALTER TABLE file_status ADD COLUMN {column} {col_type}")

    conn.commit()
    conn.close()

# Check if the file is already processed
def skip_file_processing(file_name, args: Arguments):
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()
    c.execute(
        "SELECT execution_status FROM file_status WHERE file_name = ?", (file_name,)
    )
    row = c.fetchone()
    conn.close()

    if not row:
        if args.skip_unprocessed:
            logger.warning(f"[{file_name}] Skipping due to the flag `skip_unprocessed`")
        return args.skip_unprocessed  # skip unprocessed files

    if row[0] == "ERROR":
        if not args.retry_failed:
            logger.warning(
                f"[{file_name}] Skipping due to the flag not set `retry_failed`"
            )
        return not args.retry_failed
    elif row[0] == "COMPLETED":
        return True
    else:
        if args.skip_pending:
            logger.warning(f"[{file_name}] Skipping due to the flag `skip_pending`")
        return args.skip_pending


# Update status in SQLite DB
def update_db(
    file_name,
    execution_status,
    result,
    time_taken,
    status_code,
    status_api_endpoint,
    args: Arguments
):

    total_embedding_cost = None
    total_embedding_tokens = None
    total_llm_cost = None
    total_llm_tokens = None
    error_message = None

    if result is not None:
        total_embedding_cost, total_llm_cost, total_embedding_tokens, total_llm_tokens = calculate_cost_and_tokens(result)

    if execution_status == "ERROR":
        error_message = extract_error_message(result)

    conn = sqlite3.connect(args.db_path)
    conn.set_trace_callback(
        lambda x: (
            logger.debug(f"[{file_name}] Executing statement: {x}")
            if x.strip() not in ["BEGIN", "COMMIT"]
            else None
        )
    )
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        """
        INSERT OR REPLACE INTO file_status (file_name, execution_status, result, time_taken, status_code, status_api_endpoint, total_embedding_cost, total_embedding_tokens, total_llm_cost, total_llm_tokens, error_message, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM file_status WHERE file_name = ?), ?))
        """,
        (
            file_name,
            execution_status,
            json.dumps(result),
            time_taken,
            status_code,
            status_api_endpoint,
            total_embedding_cost,
            total_embedding_tokens,
            total_llm_cost,
            total_llm_tokens,
            error_message,
            now,
            file_name,
            now,
        ),
    )
    conn.commit()
    conn.close()

# Calculate total cost and tokens for detailed report
def calculate_cost_and_tokens(result):

    total_embedding_cost = None
    total_embedding_tokens = None
    total_llm_cost = None
    total_llm_tokens = None
        
    # Extract 'extraction_result' from the result
    extraction_result = result.get("extraction_result", [])
        
    if not extraction_result:
        return total_embedding_cost, total_llm_cost, total_embedding_tokens, total_llm_tokens
        
    extraction_data = extraction_result[0].get("result", "")
    
    # If extraction_data is a string, attempt to parse it as JSON
    if isinstance(extraction_data, str):
        try:
            extraction_data = json.loads(extraction_data) if extraction_data else {}
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON for extraction data; defaulting to empty dictionary.")
            extraction_data = {}

    
    metadata = extraction_data.get("metadata", None)
    embedding_llm = metadata.get("embedding") if metadata else None
    extraction_llm = metadata.get("extraction_llm") if metadata else None

    #Process embedding costs and tokens if embedding_llm list exists and is not empty
    if embedding_llm:
        total_embedding_cost = 0.0
        total_embedding_tokens = 0
        for item in embedding_llm:
            total_embedding_cost += float(item.get("cost_in_dollars", "0"))
            total_embedding_tokens += item.get("embedding_tokens", 0)

    #Process embedding costs and tokens if extraction_llm list exists and is not empty
    if extraction_llm:
        total_llm_cost = 0.0
        total_llm_tokens = 0
        for item in extraction_llm:
            total_llm_cost += float(item.get("cost_in_dollars", "0"))
            total_llm_tokens += item.get("total_tokens", 0)
        
    return total_embedding_cost, total_llm_cost, total_embedding_tokens, total_llm_tokens

# Exract error message from the result JSON
def extract_error_message(result):
    # Check for error in extraction_result
    extraction_result = result.get("extraction_result", [])
    if extraction_result and isinstance(extraction_result, list):
        for item in extraction_result:
            if "error" in item and item["error"]:
                return item["error"]
    # Fallback to the parent error
    return result.get("error", "No error message found")

# Print final summary with count of each status and average time using a single SQL query
def print_summary(args: Arguments):
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()

    # Fetch count and average time for each status
    c.execute(
        """
        SELECT execution_status, COUNT(*) AS status_count
        FROM file_status
        GROUP BY execution_status
    """
    )
    summary = c.fetchall()
    conn.close()

    # Print the summary
    print("\nFinal Summary:")
    for row in summary:
        status = row[0]
        count = row[1]
        print(f"Status '{status}': {count}")


def print_report(args: Arguments):
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()

    # Fetch required fields, including total_cost and total_tokens
    c.execute(
        """
        SELECT file_name, execution_status, time_taken, total_embedding_cost, total_embedding_tokens, total_llm_cost, total_llm_tokens, error_message
        FROM file_status
    """
    )
    report_data = c.fetchall()
    conn.close()

    # Print the summary
    print("\nDetailed Report:")
    if report_data:
        # Tabulate the data with column headers
        headers = [
            "File Name", 
            "Execution Status", 
            "Time Elapsed (seconds)", 
            "Total Embedding Cost", 
            "Total Embedding Tokens", 
            "Total LLM Cost", 
            "Total LLM Tokens", 
            "Error Message"
        ]
        
        column_widths = {
            "File Name": 30,
            "Execution Status": 20,
            "Time Elapsed (seconds)": 20,
            "Total Embedding Cost": 20,
            "Total Embedding Tokens": 20,
            "Total LLM Cost": 20,
            "Total LLM Tokens": 20,
            "Error Message": 30,
        }

        formatted_data = []
        # Format and wrap each row's data to match column widths
        for row in report_data:
            formatted_row = []
            for idx, cell in enumerate(row):
                header = headers[idx]
                width = column_widths[header]
                cell_value = "None" if cell is None else str(cell)
                if header == "Error Message" and len(cell_value) > 50:
                    # Truncate long error messages
                    cell_value = textwrap.fill(cell_value[:100], width=width) + "..."
                else:
                    cell_value = textwrap.fill(cell_value, width=width)
                formatted_row.append(cell_value)
            formatted_data.append(formatted_row)

        # Print the table
        print(tabulate(formatted_data, headers=headers, tablefmt="pretty"))
    else:
        print("No records found in the database.")
        
    print("\nNote: For more detailed error messages, use the CSV report argument.")

def export_report_to_csv(args: Arguments):
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()

    c.execute(
        """
        SELECT file_name, execution_status, result, time_taken, total_embedding_cost, total_embedding_tokens, total_llm_cost, total_llm_tokens, error_message
        FROM file_status
        """
    )
    report_data = c.fetchall()
    conn.close()

    if not report_data:
        print("No data available to export as CSV.")
        return

    # Define the headers
    headers = [
        "File Name", "Execution Status", "Result", "Time Elapsed (seconds)",
        "Total Embedding Cost", "Total Embedding Tokens",
        "Total LLM Cost", "Total LLM Tokens", "Error Message"
    ]

    try:
        with open(args.csv_report, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)  # Write headers
            writer.writerows(report_data)  # Write data rows
        print(f"CSV successfully exported to '{args.csv_report}'")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")


def get_status_endpoint(file_path, client, args: Arguments):
    """Returns status_endpoint, status and response (if available)"""
    status_endpoint = None

    # If retry_pending is True, check if the status API endpoint is available
    conn = sqlite3.connect(args.db_path)
    c = conn.cursor()
    c.execute(
        "SELECT status_api_endpoint FROM file_status WHERE file_name = ? AND execution_status NOT IN ('COMPLETED', 'ERROR')",
        (file_path,),
    )
    row = c.fetchone()
    conn.close()
    logger.info(f"Status: {row}")
    if row:
        # Use the existing status API endpoint to get the status
        status_endpoint = row[0]

    # status_endpoint is only available for pending items. retry_pending will force retry and hence ignore existing.
    if args.retry_pending:
        status_endpoint = None

    if status_endpoint:
        logger.info(
            f"[{file_path}] Using the existing status endpoint: {status_endpoint}"
        )
        return status_endpoint, "PENDING", None

    # Fresh API call to process the file
    execution_status = "STARTING"
    update_db(file_path, execution_status, None, None, None, None, args=args)
    response = client.structure_file(file_paths=[file_path])
    logger.debug(f"[{file_path}] Response of initial API call: {response}")
    status_endpoint = response.get(
        "status_check_api_endpoint"
    )  # If ERROR or completed this will be None
    execution_status = response.get("execution_status")
    status_code = response.get("status_code")
    update_db(
        file_path,
        execution_status,
        response,
        None,
        status_code,
        status_endpoint,
        args=args
    )
    return status_endpoint, execution_status, response


def process_file(
    file_path, success_count, failure_count, skipped_count, args: Arguments
):
    logger.info(f"[{file_path}]: Processing started")

    # Any file which should be skipped will happen at this point.
    if skip_file_processing(file_name=file_path, args=args):
        logger.warning(f"[{file_path}]: Skipping processing.")
        skipped_count.value += 1
        return

    start_time = time.time()
    status_code = None
    status_endpoint = None

    try:
        client = APIDeploymentsClient(
            api_url=args.api_endpoint,
            api_key=args.api_key,
            api_timeout=args.api_timeout,
            logging_level=args.log_level,
            include_metadata=args.include_metadata,
            verify=args.verify,
        )

        status_endpoint, execution_status, response = get_status_endpoint(
            file_path=file_path, client=client, args=args
        )
        # Polling until status is COMPLETED or ERROR
        while execution_status not in ["COMPLETED", "ERROR"]:
            time.sleep(args.poll_interval)
            response = client.check_execution_status(status_endpoint)
            execution_status = response.get("execution_status")
            status_code = response.get("status_code")  # Default to 200 if not provided
            update_db(
                file_path, execution_status, None, None, status_code, status_endpoint, args=args
            )

        result = response
        logger.debug(f"[{file_path}] Response of final API call: {response}")
        success_count.value += 1

    except Exception as e:
        logger.error(
            f"Error while processing file: {file_path}: {e}",
            stack_info=True,
            exc_info=True,
        )
        execution_status = "ERROR"
        result = {"error": str(e)}
        failure_count.value += 1

    end_time = time.time()
    time_taken = round(end_time - start_time, 2)
    update_db(
        file_path, execution_status, result, time_taken, status_code, status_endpoint, args=args
    )
    logger.info(f"[{file_path}]: Processing completed: {execution_status}")


def load_folder(args: Arguments):
    files = []
    for root, _, filenames in os.walk(args.input_folder_path):
        for f in filenames:
            file_path = os.path.join(root, f)
            if os.path.isfile(file_path):
                files.append(file_path)
        if not args.recurse_input_folder:
            break
    logger.debug(f"Loaded '{len(files)}' files from '{args.input_folder_path}': {files}")

    with Manager() as manager, Pool(args.parallel_call_count) as executor:
        success_count = manager.Value("i", 0)  # Shared integer for success count
        failure_count = manager.Value("i", 0)  # Shared integer for failure count
        skipped_count = manager.Value("i", 0)  # Shared integer for skipped count

        desc = f"\033[92mSUCCESS: {success_count.value}\033[0m, \033[91mFAILURES: {failure_count.value}\033[0m, \033[93mSKIPPED: {skipped_count.value}\033[0m"
        pbar = tqdm(
            total=len(files),
            colour="blue",
            desc=desc,
            mininterval=0.1,
            maxinterval=2,
            miniters=1,
        )

        process_file_partial = partial(
            process_file,
            success_count=success_count,
            failure_count=failure_count,
            skipped_count=skipped_count,
            args=args,
        )

        for _ in executor.imap_unordered(process_file_partial, files):
            pbar.desc = f"\033[92mSUCCESS: {success_count.value}\033[0m, \033[91mFAILURES: {failure_count.value}\033[0m, \033[93mSKIPPED: {skipped_count.value}\033[0m"
            pbar.update()
            pbar.refresh()
            logger.debug("Got an update")

        pbar.close()


def api_deployment_batch_run(args: Arguments):
    logger.warning(f"Running with params: {args}")
    init_db(args=args)  # Initialize DB

    load_folder(args=args)

    print_summary(args=args)  # Print summary at the end
    if args.print_report:
        print_report(args=args)
        logger.warning(
            "Elapsed time calculation of a file which was resumed"
            " from pending state will not be correct"
        )
    
    if args.csv_report:
        export_report_to_csv(args=args)


def main():
    parser = argparse.ArgumentParser(description="Process files using Unstract's API deployment")
    parser.add_argument(
        "-e",
        "--api_endpoint",
        dest="api_endpoint",
        type=str,
        required=True,
        help="API Endpoint to use for processing the files",
    )
    parser.add_argument(
        "-k",
        "--api_key",
        dest="api_key",
        type=str,
        required=True,
        help="API Key for authenticating the calls",
    )
    parser.add_argument(
        "-t",
        "--api_timeout",
        dest="api_timeout",
        type=int,
        default=10,
        help="Time in seconds to wait before switching to async mode (default: 10)",
    )
    parser.add_argument(
        "-i",
        "--poll_interval",
        dest="poll_interval",
        type=int,
        default=5,
        help="Time in seconds the process will sleep between polls in async mode (default: 5)",
    )
    parser.add_argument(
        "-f",
        "--input_folder_path",
        dest="input_folder_path",
        type=str,
        required=True,
        help="Path where the files to process are present",
    )
    parser.add_argument(
        "-p",
        "--parallel_call_count",
        dest="parallel_call_count",
        type=int,
        default=5,
        help="Number of calls to be made in parallel (default: 5)",
    )
    parser.add_argument(
        "--db_path",
        dest="db_path",
        type=str,
        default="file_processing.db",
        help="Path where the SQlite DB file is stored (default: './file_processing.db)'",
    )
    parser.add_argument(
        '--csv_report',
        dest="csv_report",
        type=str,
        help='Path to export the detailed report as a CSV file',
    )
    parser.add_argument(
        "--recursive",
        dest="recurse_input_folder",
        action="store_true",
        help="Recursively identify and process files from the input folder path (default: False)",
    )
    parser.add_argument(
        "--retry_failed",
        dest="retry_failed",
        action="store_true",
        help="Retry processing of failed files (default: True)",
    )
    parser.add_argument(
        "--retry_pending",
        dest="retry_pending",
        action="store_true",
        help="Retry processing of pending files as new request (Without this it will try to fetch the results using status API) (default: True)",
    )
    parser.add_argument(
        "--skip_pending",
        dest="skip_pending",
        action="store_true",
        help="Skip processing of pending files (overrides --retry-pending) (default: True)",
    )
    parser.add_argument(
        "--skip_unprocessed",
        dest="skip_unprocessed",
        action="store_true",
        help="Skip unprocessed files while retry processing of failed files (default: True)",
    )
    parser.add_argument(
        "--log_level",
        dest="log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARN", "ERROR"],
        help="Minimum loglevel for logging",
    )
    parser.add_argument(
        "--print_report",
        dest="print_report",
        action="store_true",
        help="Print a detailed report of all file processed (default: True)",
    )
    parser.add_argument(
        "--exclude_metadata",
        dest="include_metadata",
        action="store_false",
        help="Exclude metadata on tokens consumed and the context passed to LLMs for prompt studio exported tools in the result for each file (default: False)",
    )
    parser.add_argument(
        "--no_verify",
        dest="verify",
        action="store_false",
        help="Disable SSL certificate verification (default: False)",
    )

    args = Arguments(**vars(parser.parse_args()))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(args.log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logging.basicConfig(level=args.log_level, handlers=[ch])

    api_deployment_batch_run(args=args)


if __name__ == "__main__":
    main()
