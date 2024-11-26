# File Processing Script

This script processes files in a specified directory using an API, logs results in a local SQLite database, and provides options for retrying failed or pending files. It includes features for skipping specific files, generating reports, and running multiple API calls in parallel.

## Features

- **Parallel Processing**: Process files in parallel, with the number of parallel calls configurable.
- **Status Tracking**: Tracks the execution status, results, and time taken for each file in an SQLite database.
- **Retry Logic**: Options to retry failed or pending files, or to skip them.
- **Detailed Reporting**: Prints a summary of file processing and provides a detailed report.
- **Polling**: Polls the API until the result is complete, with customizable intervals.

## Dependencies

Ensure you have the required dependencies installed:

```bash
pip install -r requirements.txt
```

## SQLite Database Schema

The script uses a local SQLite database (`file_processing.db`) with the following schema:

- **file_status**:
  - `id` (INTEGER): Primary key
  - `file_name` (TEXT): Unique name of the file
  - `execution_status` (TEXT): Status of the file (`STARTING`, `COMPLETED`, `ERROR`, etc.)
  - `result` (TEXT): API result in JSON format
  - `time_taken` (REAL): Time taken to process the file
  - `status_code` (INTEGER): API status code
  - `status_api_endpoint` (TEXT): API endpoint for checking status
  - `total_embedding_cost` (REAL): Total cost incurred for embeddings.
  - `total_embedding_tokens` (INTEGER): Total tokens used for embeddings.
  - `total_llm_cost` (REAL): Total cost incurred for LLM operations.
  - `total_llm_tokens` (INTEGER): Total tokens used for LLM operations.
  - `error_message` (TEXT): Details of errors if `execution_status` is `ERROR`; otherwise NULL.
  - `updated_at` (TEXT): Last updated timestamp
  - `created_at` (TEXT): Creation timestamp

## Command Line Arguments

Run the script with the following options:

```bash
python main.py -h
```

This will display detailed usage information.

### Required Arguments:

- `-e`, `--api_endpoint`: API endpoint for processing files.
- `-k`, `--api_key`: API key for authenticating API calls.
- `-f`, `--input_folder_path`: Folder path containing the files to process.

### Optional Arguments:

- `-t`, `--api_timeout`: Timeout (in seconds) for API requests (default: 10).
- `-i`, `--poll_interval`: Interval (in seconds) between API status polls (default: 5).
- `-p`, `--parallel_call_count`: Number of parallel API calls (default: 10).
- `--retry_failed`: Retry processing of failed files.
- `--retry_pending`: Retry processing of pending files by making new requests.
- `--skip_pending`: Skip processing of pending files.
- `--skip_unprocessed`: Skip unprocessed files when retrying failed files.
- `--log_level`: Log level (default: `INFO`).
- `--print_report`: Print a detailed report of all processed files at the end.
- `--exclude_metadata`: Exclude metadata on tokens consumed and the context passed to LLMs for prompt studio exported tools in the result for each file.
- `--no_verify`: Disable SSL certificate verification. (By default, SSL verification is enabled.)
- `--csv_report`: Path to export the detailed report as a CSV file.

## Usage Examples

### Basic Usage

To process files in the directory `/path/to/files` using the provided API:

```bash
python main.py -e https://api.example.com/process -k your_api_key -f /path/to/files
```

### Retry Failed Files

To retry files that previously encountered errors:

```bash
python main.py -e https://api.example.com/process -k your_api_key -f /path/to/files --retry_failed
```

### Skip Pending Files

To skip files that are still pending:

```bash
python main.py -e https://api.example.com/process -k your_api_key -f /path/to/files --skip_pending
```

### Parallel Processing

To process 20 files in parallel:

```bash
python main.py -e https://api.example.com/process -k your_api_key -f /path/to/files -p 20
```

### Print Detailed Report

To generate and display a detailed report at the end of the run:

```bash
python main.py -e https://api.example.com/process -k your_api_key -f /path/to/files --print_report
```

## Database and Logging

- **Database**: Results and statuses are stored in a local SQLite database (`file_processing.db`).
- **Logging**: Logs are printed to stdout with configurable log levels (e.g., `DEBUG`, `INFO`, `ERROR`).

## Example Output

```
Status 'COMPLETED': 50
Status 'ERROR': 10
Status 'PENDING': 5
```

For more detailed output, you can use the `--print_report` option to get a per-file breakdown.


## Status Definitions

The following statuses are tracked for each file during processing:

- **STARTING**: Initial state when processing begins.
- **EXECUTING**: File is currently being processed.
- **PENDING**: File processing is pending or waiting for external actions.
- **ERROR**: File processing encountered an error.
- **COMPLETED**: File was processed successfully and will not be processed again unless forced by rerun options.

For more about statuses : [API Docs](https://docs.unstract.com/unstract/unstract_platform/api_deployment/unstract_api_deployment_execution_api/#possible-execution-status)


## Questions and Feedback

On Slack, [join great conversations](https://join-slack.unstract.com/) around LLMs, their ecosystem and leveraging them to automate the previously unautomatable!

[Unstract client](https://github.com/Zipstack/unstract-python-client): Learn more about Unstract API clint.

[Unstract Cloud](https://unstract.com/): Signup and Try!.

[Unstract developer documentation](https://docs.unstract.com/): Learn more about Unstract and its API.
