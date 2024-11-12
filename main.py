import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from multiprocessing import Manager, Pool

from tabulate import tabulate
from tqdm import tqdm
from unstract.api_deployments.client import APIDeploymentsClient

DB_NAME = "file_processing.db"
global_arguments = None
logger = logging.getLogger(__name__)


# Dataclass for arguments
@dataclass
class Arguments:
    api_endpoint: str
    api_key: str
    api_timeout: int = 10
    poll_interval: int = 5
    input_folder_path: str = ""
    parallel_call_count: int = 5
    retry_failed: bool = False
    retry_pending: bool = False
    skip_pending: bool = False
    skip_unprocessed: bool = False
    log_level: str = "INFO"
    print_report: bool = False
    include_metadata: bool = False
    verify: bool = True


# Initialize SQLite DB
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS file_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT UNIQUE,
                    execution_status TEXT,
                    result TEXT,
                    time_taken REAL,
                    status_code INTEGER,
                    status_api_endpoint TEXT,
                    updated_at TEXT,
                    created_at TEXT
                )"""
    )
    conn.commit()
    conn.close()


# Check if the file is already processed
def skip_file_processing(file_name, args: Arguments):
    conn = sqlite3.connect(DB_NAME)
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
):
    conn = sqlite3.connect(DB_NAME)
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
        INSERT OR REPLACE INTO file_status (file_name, execution_status, result, time_taken, status_code, status_api_endpoint, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM file_status WHERE file_name = ?), ?))
    """,
        (
            file_name,
            execution_status,
            json.dumps(result),
            time_taken,
            status_code,
            status_api_endpoint,
            now,
            file_name,
            now,
        ),
    )
    conn.commit()
    conn.close()


# Print final summary with count of each status and average time using a single SQL query
def print_summary():
    conn = sqlite3.connect("file_processing.db")
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


def print_report():
    conn = sqlite3.connect("file_processing.db")
    c = conn.cursor()

    # Fetch count and average time for each status
    c.execute(
        """
        SELECT file_name, execution_status, time_taken
        FROM file_status
    """
    )
    report_data = c.fetchall()
    conn.close()

    # Print the summary
    print("\nDetailed Report:")
    if report_data:
        # Tabulate the data with column headers
        headers = ["File Name", "Execution Status", "Time Elapsed (seconds)"]
        print(tabulate(report_data, headers=headers, tablefmt="pretty"))
    else:
        print("No records found in the database.")


def get_status_endpoint(file_path, client, args: Arguments):
    """Returns status_endpoint, status and response (if available)"""
    status_endpoint = None

    # If retry_pending is True, check if the status API endpoint is available
    conn = sqlite3.connect(DB_NAME)
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
    update_db(file_path, execution_status, None, None, None, None)
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
                file_path, execution_status, None, None, status_code, status_endpoint
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
        file_path, execution_status, result, time_taken, status_code, status_endpoint
    )
    logger.info(f"[{file_path}]: Processing completed: {execution_status}")


def load_folder(args: Arguments):
    files = [
        os.path.join(args.input_folder_path, f)
        for f in os.listdir(args.input_folder_path)
        if os.path.isfile(os.path.join(args.input_folder_path, f))
    ]

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


def main():
    parser = argparse.ArgumentParser(description="Process files using the API.")
    parser.add_argument(
        "-e",
        "--api_endpoint",
        dest="api_endpoint",
        type=str,
        required=True,
        help="API Endpoint to use for processing the files.",
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
        help="Time in seconds to wait before switching to async mode.",
    )
    parser.add_argument(
        "-i",
        "--poll_interval",
        dest="poll_interval",
        type=int,
        default=5,
        help="Time in seconds the process will sleep between polls in async mode.",
    )
    parser.add_argument(
        "-f",
        "--input_folder_path",
        dest="input_folder_path",
        type=str,
        required=True,
        help="Path where the files to process are present.",
    )
    parser.add_argument(
        "-p",
        "--parallel_call_count",
        dest="parallel_call_count",
        type=int,
        default=5,
        help="Number of calls to be made in parallel.",
    )
    parser.add_argument(
        "--retry_failed",
        dest="retry_failed",
        action="store_true",
        help="Retry processing of failed files.",
    )
    parser.add_argument(
        "--retry_pending",
        dest="retry_pending",
        action="store_true",
        help="Retry processing of pending files as new request (Without this it will try to fetch the results using status API).",
    )
    parser.add_argument(
        "--skip_pending",
        dest="skip_pending",
        action="store_true",
        help="Skip processing of pending files (Over rides --retry-pending).",
    )
    parser.add_argument(
        "--skip_unprocessed",
        dest="skip_unprocessed",
        action="store_true",
        help="Skip unprocessed files while retry processing of failed files.",
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
        help="Print a detailed report of all file processed.",
    )

    parser.add_argument(
        "--include_metadata",
        dest="include_metadata",
        action="store_true",
        help="Include metadata on tokens consumed and the context passed to LLMs for prompt studio exported tools in the result for each file.",
    )

    parser.add_argument(
        "--no_verify",
        dest="verify",
        action="store_false",
        help="Disable SSL certificate verification.",
    )


    args = Arguments(**vars(parser.parse_args()))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(args.log_level)
    logging.basicConfig(level=args.log_level, handlers=[ch])

    logger.warning(f"Running with params: {args}")

    init_db()  # Initialize DB

    load_folder(args=args)

    print_summary()  # Print summary at the end
    if args.print_report:
        print_report()
        logger.warning(
            "Elapsed time calculation of a file which was resumed"
            " from pending state will not be correct"
        )


if __name__ == "__main__":
    main()


