import time
import pytz
import requests
import pandas as pd
from datetime import datetime, timedelta # Added timedelta
import sys # Used for flushing output
import os # Added for creating directory and getting absolute paths

# --- Configuration ---
api_key = "" # Replace with your actual API key if needed
API_URL = "https://api.datasource.cybotrade.rs"
MAX_LIMIT_PER_REQUEST = 60000  # Reduced limit as requested
OUTPUT_DIR = "fetched_crypto_data" # Directory for saving files

# Define the single endpoint we want to fetch
endpoints = [
    "cryptoquant|btc/exchange-flows/in-house-flow?window=block&exchange=all_exchange"
]

# Set our start time to 1 year ago from the current UTC time
# Using timezone-aware datetime for better accuracy
current_time_utc = datetime.now(pytz.utc)
one_year_ago_utc = current_time_utc - timedelta(days=365)
INITIAL_START_TIME_MS = int(one_year_ago_utc.timestamp() * 1000)

# Create the output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
# ------------------------

# --- Quota Tracking Variables ---
current_quota_remaining = None
quota_reset_timestamp_ms = 0
total_quota_limit = 10000 # Initial guess, will be updated by first response header
# ------------------------------

# --- Data Storage ---
all_fetched_data = {} # Dictionary to store the final DataFrame for each topic
# --------------------

print(f"Starting data fetch process at {datetime.now()}")
# Print the calculated start time in UTC for confirmation
print(f"Fetching data from: {datetime.utcfromtimestamp(INITIAL_START_TIME_MS / 1000)} UTC")
print("-" * 30)

# Process the single endpoint defined above
for topic in endpoints:
    print(f"Processing topic: {topic}")
    print("-" * 30)

    # Reset variables for the topic
    topic_start_time_ms = INITIAL_START_TIME_MS
    all_data_frames_for_topic = []
    total_records_fetched_for_topic = 0
    batch_num = 0

    while True: # Loop to handle pagination for the current topic
        batch_num += 1
        print(f"\nAttempting Batch {batch_num} for topic: {topic}")
        # Use utcfromtimestamp to display the start time correctly in UTC
        print(f"Current start_time (ms): {topic_start_time_ms} ({datetime.utcfromtimestamp(topic_start_time_ms / 1000)} UTC)")

        # --- Quota Check ---
        print(f"Quota Status - Limit: {total_quota_limit}, Remaining: {current_quota_remaining}, Reset Timestamp: {quota_reset_timestamp_ms} ({datetime.utcfromtimestamp(quota_reset_timestamp_ms / 1000) if quota_reset_timestamp_ms > 0 else 'N/A'} UTC)")
        sys.stdout.flush()

        if total_quota_limit is not None and current_quota_remaining is not None and current_quota_remaining <= 0:
            current_time_ms = int(time.time() * 1000)
            if quota_reset_timestamp_ms > current_time_ms:
                sleep_duration_ms = quota_reset_timestamp_ms - current_time_ms
                # Add a small buffer (e.g., 5 seconds) to the sleep time
                sleep_duration_s = (sleep_duration_ms / 1000) + 5
                print(f"Quota likely exceeded. Sleeping for {sleep_duration_s:.2f} seconds until reset time ({datetime.utcfromtimestamp(quota_reset_timestamp_ms / 1000)} UTC).")
                sys.stdout.flush()
                time.sleep(sleep_duration_s)
                # Assume quota is reset after sleeping
                current_quota_remaining = total_quota_limit
            else:
                # Reset time is in the past or invalid, likely means we hit the limit exactly at reset or header is wrong
                print("Quota likely exceeded, but reset time appears to be in the past or invalid. Waiting 60 seconds before retrying cautiously.")
                sys.stdout.flush()
                time.sleep(60)
                # Assume quota might be reset after waiting
                current_quota_remaining = total_quota_limit

        # --- Prepare and Make API Request ---
        try:
            provider = topic.split("|")[0]
            endpoint_path = topic.split("|")[-1] # Renamed to avoid conflict with 'endpoints' list
            url = f"{API_URL}/{provider}/{endpoint_path}&start_time={topic_start_time_ms}&limit={MAX_LIMIT_PER_REQUEST}"
            print(f"Fetching URL: {url}")
            sys.stdout.flush()

            response = requests.get(
                url,
                headers={"X-API-KEY": api_key},
                timeout=60 # Increased timeout for potentially large requests
            )

            print(f"Response Status Code: {response.status_code}")
            print(f"Response Reason: {response.reason}")
            sys.stdout.flush()

            # --- Update Quota Info From Headers ---
            if 'X-Api-Limit' in response.headers:
                try: total_quota_limit = int(response.headers["X-Api-Limit"])
                except ValueError: print("Warning: Could not parse X-Api-Limit header.")
            if 'X-Api-Limit-Remaining' in response.headers:
                try: current_quota_remaining = int(response.headers["X-Api-Limit-Remaining"])
                except ValueError: print("Warning: Could not parse X-Api-Limit-Remaining header.")
            if 'X-Api-Limit-Reset-Timestamp' in response.headers:
                try: quota_reset_timestamp_ms = int(response.headers["X-Api-Limit-Reset-Timestamp"])
                except ValueError: print("Warning: Could not parse X-Api-Limit-Reset-Timestamp header.")

            print(f"Updated Quota - Limit: {total_quota_limit}, Remaining: {current_quota_remaining}, Reset Timestamp: {quota_reset_timestamp_ms} ({datetime.utcfromtimestamp(quota_reset_timestamp_ms / 1000) if quota_reset_timestamp_ms > 0 else 'N/A'} UTC)")
            sys.stdout.flush()
            # --------------------------------------

            response.raise_for_status() # Check for HTTP errors (4xx or 5xx)

            # --- Process Successful Response ---
            response_data = response.json()

            if "data" not in response_data or not isinstance(response_data["data"], list):
                print(f"Error: Unexpected response format for {topic}. 'data' key missing or not a list.")
                print(f"Response Text: {response.text[:500]}...") # Print beginning of response
                sys.stdout.flush()
                break # Stop fetching for this topic

            data_batch = response_data["data"]
            batch_size = len(data_batch)
            print(f"Fetched {batch_size} records in this batch.")
            sys.stdout.flush()

            if batch_size == 0:
                print(f"No more data returned for topic {topic} starting from timestamp {topic_start_time_ms}. Fetching complete.")
                sys.stdout.flush()
                break # Exit the while loop for this topic

            df_batch = pd.DataFrame(data_batch)
            all_data_frames_for_topic.append(df_batch)
            total_records_fetched_for_topic += batch_size

            # --- Prepare for Next Iteration: Update Start Time ---
            # Identify the timestamp column (more robustly)
            timestamp_col = None
            if 'start_time' in df_batch.columns: timestamp_col = 'start_time'
            elif 'ts' in df_batch.columns: timestamp_col = 'ts'
            elif 'timestamp' in df_batch.columns: timestamp_col = 'timestamp'
            else:
                 # Try to find any column ending with 'time' or 'ts' as a fallback
                potential_ts_cols = [col for col in df_batch.columns if col.lower().endswith('time') or col.lower().endswith('ts')]
                if potential_ts_cols:
                    timestamp_col = potential_ts_cols[0] # Pick the first one found
                    print(f"Warning: Standard timestamp column not found, using fallback '{timestamp_col}'.")
                else:
                    print(f"Error: Cannot find a suitable timestamp column in DataFrame for topic {topic}. Cannot paginate.")
                    print(f"Columns found: {df_batch.columns}")
                    sys.stdout.flush()
                    break # Stop fetching for this topic

            try:
                # Ensure the timestamp column is numeric before finding the max
                if not pd.api.types.is_numeric_dtype(df_batch[timestamp_col]):
                    print(f"Converting timestamp column '{timestamp_col}' to numeric...")
                    df_batch[timestamp_col] = pd.to_numeric(df_batch[timestamp_col])

                last_timestamp_ms = df_batch[timestamp_col].iloc[-1]
                # Increment the timestamp by 1 ms for the next request's start_time
                topic_start_time_ms = last_timestamp_ms + 1
            except (ValueError, TypeError) as conv_err:
                 print(f"Error converting timestamp column '{timestamp_col}' to numeric or getting last value for topic {topic}: {conv_err}")
                 print(f"Last few values of '{timestamp_col}': {df_batch[timestamp_col].tail().tolist()}")
                 print("Cannot determine last timestamp for pagination. Stopping fetch for this topic.")
                 sys.stdout.flush()
                 break # Stop fetching for this topic
            except IndexError:
                 print(f"Error: Could not access last element of timestamp column '{timestamp_col}'. DataFrame might be empty despite batch_size > 0?")
                 print("Stopping fetch for this topic.")
                 sys.stdout.flush()
                 break # Stop fetching for this topic


        # --- Error Handling ---
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            # Status code and response text are already printed above or will be if response object exists
            if hasattr(response, 'status_code'): print(f"Status Code: {response.status_code}")
            if hasattr(response, 'text'): print(f"Response Text: {response.text[:500]}...") # Print snippet
            sys.stdout.flush()

            if response.status_code == 429: # Too Many Requests
                print("Received 429 (Too Many Requests). Checking reset time from headers...")
                sys.stdout.flush()
                if total_quota_limit is not None and quota_reset_timestamp_ms > 0:
                    current_time_ms = int(time.time() * 1000)
                    if quota_reset_timestamp_ms > current_time_ms:
                        sleep_duration_ms = quota_reset_timestamp_ms - current_time_ms
                        sleep_duration_s = (sleep_duration_ms / 1000) + 5 # Add buffer
                        print(f"Sleeping for {sleep_duration_s:.2f} seconds based on reset timestamp.")
                        time.sleep(sleep_duration_s)
                        current_quota_remaining = total_quota_limit # Assume reset
                    else:
                        print("Reset time is in the past. Waiting 60 seconds as a fallback.")
                        time.sleep(60)
                        current_quota_remaining = total_quota_limit # Assume reset
                else:
                    print("No valid quota reset time available in headers. Waiting 60 seconds.")
                    time.sleep(60)
                    # Don't assume reset if headers were missing
            elif response.status_code in [500, 502, 503, 504]: # Server-side errors
                print(f"Received server error ({response.status_code}). Waiting 30 seconds before retrying...")
                sys.stdout.flush()
                time.sleep(30)
            else: # Other client-side errors (401, 403, 404, etc.) or unhandled server errors
                print(f"Unhandled HTTP error ({response.status_code}). Stopping fetch for this topic.")
                sys.stdout.flush()
                break # Stop fetching for this topic

        except requests.exceptions.RequestException as req_err:
            # E.g., Connection error, Timeout
            print(f"Request error occurred: {req_err}")
            sys.stdout.flush()
            print("Waiting 60 seconds before retrying...")
            time.sleep(60) # Wait before retrying the same request

        except KeyError as key_err:
             # Error parsing the JSON response
            print(f"Failed to parse JSON response. Missing key: {key_err}")
            if 'response' in locals() and hasattr(response, 'text'):
                 print(f"Response Text: {response.text[:500]}...")
            sys.stdout.flush()
            print(f"Stopping fetch for topic {topic} due to unexpected response format.")
            break # Stop fetching for this topic

        except Exception as e:
            # Catch-all for any other unexpected errors during the request/processing
            status_code_info = f"(Status: {response.status_code})" if 'response' in locals() and hasattr(response, 'status_code') else '(Status: N/A)'
            print(f"An unexpected error occurred {status_code_info}: {e}")
            if 'response' in locals() and hasattr(response, 'text'):
                 print(f"Response Text: {response.text[:500]}...")
            sys.stdout.flush()
            print(f"Stopping fetch for topic {topic} due to unexpected error.")
            break # Stop fetching for this topic


        # --- Inter-Request Delay ---
        # Add a small delay between successful requests to be polite to the API
        print("Waiting 1 second before next request...")
        sys.stdout.flush()
        time.sleep(1)
        # --------------------------

    # --- Post-Loop Processing for the Topic ---
    if all_data_frames_for_topic:
        print(f"\nConcatenating {len(all_data_frames_for_topic)} fetched batches for topic {topic}...")
        sys.stdout.flush()
        try:
            final_df_for_topic = pd.concat(all_data_frames_for_topic, ignore_index=True)
            print(f"Successfully concatenated data.")
            print(f"Total records fetched: {total_records_fetched_for_topic} (DataFrame shape: {final_df_for_topic.shape})")

            # --- Sort the final DataFrame by timestamp ---
            timestamp_col_sort = None # Find timestamp column again for sorting
            if 'start_time' in final_df_for_topic.columns: timestamp_col_sort = 'start_time'
            elif 'ts' in final_df_for_topic.columns: timestamp_col_sort = 'ts'
            elif 'timestamp' in final_df_for_topic.columns: timestamp_col_sort = 'timestamp'
            else:
                potential_ts_cols = [col for col in final_df_for_topic.columns if col.lower().endswith('time') or col.lower().endswith('ts')]
                if potential_ts_cols:
                    timestamp_col_sort = potential_ts_cols[0]

            if timestamp_col_sort:
                 print(f"Sorting final DataFrame by '{timestamp_col_sort}'...")
                 try:
                    # Ensure numeric type before sorting
                    if not pd.api.types.is_numeric_dtype(final_df_for_topic[timestamp_col_sort]):
                         final_df_for_topic[timestamp_col_sort] = pd.to_numeric(final_df_for_topic[timestamp_col_sort])
                    # Sort and reset index
                    final_df_for_topic.sort_values(by=timestamp_col_sort, inplace=True)
                    final_df_for_topic.reset_index(drop=True, inplace=True)
                    print("Sorting complete.")
                 except (ValueError, TypeError) as sort_err:
                    print(f"Warning: Could not sort by '{timestamp_col_sort}' due to data type issues: {sort_err}")
                 except Exception as sort_e:
                    print(f"Warning: An unexpected error occurred during sorting by '{timestamp_col_sort}': {sort_e}")
            else:
                 print(f"Warning: Cannot sort final DataFrame, suitable timestamp column not found.")
            # --------------------------------------------

            # --- Optional: Remove duplicates based on timestamp ---
            if timestamp_col_sort:
                initial_rows = len(final_df_for_topic)
                final_df_for_topic.drop_duplicates(subset=[timestamp_col_sort], keep='first', inplace=True)
                rows_removed = initial_rows - len(final_df_for_topic)
                if rows_removed > 0:
                    print(f"Removed {rows_removed} duplicate records based on '{timestamp_col_sort}'.")
            # ------------------------------------------------------

            print(f"\nSample of final data for topic {topic} (first 5 rows):")
            print(final_df_for_topic.head())
            print(f"\nSample of final data for topic {topic} (last 5 rows):")
            print(final_df_for_topic.tail())

            all_fetched_data[topic] = final_df_for_topic # Store the final processed DataFrame
            sys.stdout.flush()

        except Exception as concat_err:
             # Error during concatenation, sorting, or duplicate removal
            print(f"\nError during final processing (concatenation/sorting/deduplication) for topic {topic}: {concat_err}")
            # Store an empty DataFrame to indicate failure for this topic
            all_fetched_data[topic] = pd.DataFrame()
            sys.stdout.flush()

    else: # No data frames were fetched for this topic
        print(f"\nNo data was successfully fetched or processed for topic: {topic}")
        all_fetched_data[topic] = pd.DataFrame() # Store empty DataFrame

    print("-" * 30) # Separator after processing each topic

# ==============================================================================
# Final Summary and Saving
# ==============================================================================

print("\n=============================================")
print("Data Fetching Process Complete.")
print("=============================================")
print("Summary of fetched data:")
for topic, df in all_fetched_data.items():
    if df is not None:
        print(f"- Topic: {topic}, Records: {len(df)}, Columns: {df.shape[1]}")
    else:
        print(f"- Topic: {topic}, Records: 0 (Error during processing)")


# --- Save DataFrames to CSV Files ---
print("\n=============================================")
print("Saving Fetched Data to CSV...")
# --- MODIFICATION: Get and print the absolute path of the output directory ---
try:
    absolute_output_dir = os.path.abspath(OUTPUT_DIR)
    print(f"Output Directory (Absolute): {absolute_output_dir}")
except Exception as path_err:
    print(f"Warning: Could not determine absolute path for Output Directory '{OUTPUT_DIR}': {path_err}")
    print(f"Output Directory (Relative): {OUTPUT_DIR}") # Fallback to relative
# --- END MODIFICATION ---
print("=============================================")

for topic, df in all_fetched_data.items():
    # Check if df is not None and not empty
    if df is not None and not df.empty:
        try:
            # Sanitize topic name for use as a filename
            safe_filename_base = topic.replace('|','_').replace('/','-').replace('?','_').replace('=','_').replace('&','_').replace(':','_')
            safe_filename = f"{safe_filename_base}.csv"
            # Construct the relative or absolute path for saving
            output_path = os.path.join(OUTPUT_DIR, safe_filename)

            # --- MODIFICATION: Get the absolute path for printing ---
            try:
                absolute_output_path = os.path.abspath(output_path)
                print_path = absolute_output_path # Use absolute path for printing
            except Exception as path_err:
                print(f"Warning: Could not determine absolute path for '{output_path}': {path_err}. Using relative path for messages.")
                print_path = output_path # Fallback to original path for printing
            # --- END MODIFICATION ---

            print(f"Saving data for topic '{topic}' to '{print_path}'...")
            sys.stdout.flush()

            # Attempt to save the DataFrame to CSV
            df.to_csv(output_path, index=False)

            # --- MODIFICATION: Confirm saving using the determined print path ---
            print(f"Successfully saved {print_path} ({len(df)} records)")
            # --- END MODIFICATION ---
            sys.stdout.flush()

        except IOError as io_err:
            # Specific error for file saving issues
            print(f"Error saving file for topic '{topic}' to '{output_path}': {io_err}")
            sys.stdout.flush()
        except Exception as e:
            # Catch any other errors during filename creation or saving
            print(f"An unexpected error occurred while saving data for topic '{topic}': {e}")
            # Try to print the path where saving was attempted
            if 'output_path' in locals():
                 print(f"(Attempted path: {output_path})")
            sys.stdout.flush()
    elif df is None:
         print(f"Skipping saving for topic '{topic}' as an error occurred during data processing.")
         sys.stdout.flush()
    else: # df is an empty DataFrame
        print(f"Skipping saving for topic '{topic}' as no data was fetched.")
        sys.stdout.flush()

print("\nCSV Saving Process Complete.")
print("=============================================")
# ------------------------------------