from google.cloud import bigquery
from google.oauth2 import service_account
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import httplib2
import pandas as pd
import decimal
import time
import datetime
import math
import json
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SA_KEY_PATH = Path(__file__).resolve().parent / "creds" / "maneuver-marketing-test-625a0985781e.json"


def run_bq_query(
    query: str,
    project_id: str = "maneuver-marketing-test"
) -> pd.DataFrame:
    """
    Run a BigQuery query and return the results as a pandas dataframe.
    """
    credentials = service_account.Credentials.from_service_account_file(
        str(SA_KEY_PATH),
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = bq_client.query(query)
    results = query_job.result()
    return results.to_dataframe()

class BigQueryToSheets:
    def __init__(
        self,
        client: str,
        spreadsheet_id: str,
        spreadsheet_details: dict,
        start_row: int = 1,
        chunk_size: int = 10000,
        debug_logs: bool = False
    ):
        """Initialize class with authentication & environment variables."""
        
        # Set debug logging if requested
        self.debug_logs = debug_logs
        if self.debug_logs:
            logger.setLevel(logging.DEBUG)
            # Also enable DEBUG for googleapiclient to see API request/response details
            logging.getLogger('googleapiclient').setLevel(logging.DEBUG)
            logger.debug("🐛 Debug logging enabled")

        with open(SA_KEY_PATH) as f:
            service_account_info = json.load(f)

        # Authenticate using the service account JSON
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery"  
            ]
        )

        self.sheets_service = build("sheets", "v4", credentials=self.credentials)
        logger.info("✅ Successfully authenticated using the service account key.")

        # Set class variables
        self.client = client
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_details = spreadsheet_details
        self.start_row = start_row
        self.chunk_size = chunk_size

    def _format_range(
        self,
        cell_range: str,
        sheet_name: str
    ) -> str:
        """Formats a range reference with the sheet name safely quoted."""
        escaped_name = sheet_name.replace("'", "''") if sheet_name else self.sheet_name.replace("'", "''")
        if cell_range:
            return f"'{escaped_name}'!{cell_range}"
        return f"'{escaped_name}'"

    def _ensure_grid_size(
        self, 
        required_rows: int, 
        required_cols: int,
        spreadsheet_id: str,
        sheet_name: str
    ):
        """Expands the sheet grid if additional rows or columns are needed."""
        if self.debug_logs:
            logger.debug(f"🔍 _ensure_grid_size called: required_rows={required_rows}, required_cols={required_cols}, sheet_name='{sheet_name}'")
            logger.debug(f"   Spreadsheet ID: {spreadsheet_id}")
        
        start_time = time.time()
        try:
            if self.debug_logs:
                logger.debug(f"📖 Calling spreadsheets().get() to read spreadsheet metadata...")
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                includeGridData=False
            ).execute()
            elapsed = time.time() - start_time
            if self.debug_logs:
                logger.debug(f"✅ Spreadsheet.get() completed in {elapsed:.2f} seconds")
                logger.debug(f"   Retrieved {len(spreadsheet.get('sheets', []))} sheets")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ Error in spreadsheets().get() after {elapsed:.2f} seconds: {type(e).__name__}: {e}")
            if self.debug_logs:
                import traceback
                logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
            raise

        matching_sheet = next(
            (sheet for sheet in spreadsheet.get("sheets", []) if sheet["properties"]["title"] == sheet_name),
            None
        )

        if not matching_sheet:
            available_sheets = [sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])]
            logger.error(f"❌ Sheet '{sheet_name}' not found in spreadsheet {spreadsheet_id}.")
            if self.debug_logs:
                logger.debug(f"   Available sheets: {available_sheets}")
            raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet {spreadsheet_id}.")

        properties = matching_sheet["properties"]
        grid_props = properties.get("gridProperties", {})
        current_rows = grid_props.get("rowCount", 0)
        current_cols = grid_props.get("columnCount", 0)
        
        if self.debug_logs:
            logger.debug(f"📊 Current grid size: {current_rows} rows x {current_cols} cols")
            logger.debug(f"📊 Required grid size: {required_rows} rows x {required_cols} cols")

        target_rows = max(required_rows, current_rows)
        target_cols = max(required_cols, current_cols)

        if target_rows == current_rows and target_cols == current_cols:
            if self.debug_logs:
                logger.debug(f"✅ Grid size already sufficient, no update needed")
            return

        if self.debug_logs:
            logger.debug(f"🔧 Updating grid size to: {target_rows} rows x {target_cols} cols")
        requests = [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": properties["sheetId"],
                    "gridProperties": {
                        "rowCount": target_rows,
                        "columnCount": target_cols
                    }
                },
                "fields": "gridProperties(rowCount,columnCount)"
            }
        }]

        # Use a longer timeout for batchUpdate; resizing large sheets can exceed 60s default.
        long_timeout_http = AuthorizedHttp(
            self.credentials,
            http=httplib2.Http(timeout=300),
        )
        start_time = time.time()
        try:
            if self.debug_logs:
                logger.debug(f"📝 Calling batchUpdate() to resize grid...")
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute(http=long_timeout_http)
            elapsed = time.time() - start_time
            if self.debug_logs:
                logger.debug(f"✅ batchUpdate() completed in {elapsed:.2f} seconds")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ Error in batchUpdate() after {elapsed:.2f} seconds: {type(e).__name__}: {e}")
            if self.debug_logs:
                import traceback
                logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
            raise

    def clear_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        retries=3,
        delay=5
    ):
        """Clears existing data from a Google Sheet before writing new data."""
        if self.debug_logs:
            logger.debug(f"🧹 Starting to clear sheet '{sheet_name}' in spreadsheet '{spreadsheet_id}'")
        
        for attempt in range(retries):
            try:
                range_to_clear = self._format_range(cell_range="", sheet_name=sheet_name)
                if self.debug_logs:
                    logger.debug(f"   Attempt {attempt + 1}/{retries}: Clearing range '{range_to_clear}'")
                clear_start_time = time.time()
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=range_to_clear
                ).execute()
                clear_elapsed = time.time() - clear_start_time
                if self.debug_logs:
                    logger.info(f"✅ Cleared data in range: {range_to_clear} in {clear_elapsed:.2f} seconds.")
                else:
                    logger.info(f"✅ Cleared data in range: {range_to_clear}.")
                return
            except Exception as e:
                clear_elapsed = time.time() - clear_start_time if 'clear_start_time' in locals() else 0
                logger.error(f"⚠️ Error clearing sheet {sheet_name} after {clear_elapsed:.2f} seconds: {type(e).__name__}: {e}")
                if self.debug_logs:
                    logger.debug(f"   Range attempted: {range_to_clear if 'range_to_clear' in locals() else 'N/A'}")
                    import traceback
                    logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
                if attempt < retries - 1:
                    logger.info(f"   Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                    time.sleep(delay)
        raise TimeoutError(f"❌ Failed to clear range in sheet: {sheet_name} after {retries} retries.")

    def write_to_sheets(
        self, 
        dataframe,
        spreadsheet_id: str,
        sheet_name: str
        ):
        """Writes a pandas DataFrame to Google Sheets in chunks."""
        if self.debug_logs:
            logger.debug(f"✍️ write_to_sheets called for sheet '{sheet_name}'")
            logger.debug(f"   DataFrame shape: {dataframe.shape}")
            logger.debug(f"   Spreadsheet ID: {spreadsheet_id}")
            logger.debug(f"   Start row: {self.start_row}")
            logger.debug(f"   Chunk size: {self.chunk_size}")
        
        try:
            # Prepare header and values
            if self.debug_logs:
                logger.debug(f"📋 Preparing header and values from DataFrame...")
            header = dataframe.columns.tolist()
            values = dataframe.values.tolist()
            if self.debug_logs:
                logger.debug(f"   Header columns: {len(header)}")
                logger.debug(f"   Data rows: {len(values)}")
                logger.debug(f"   Header: {header}")

            required_rows = self.start_row + len(values)
            required_cols = max(len(header), 1)
            if self.debug_logs:
                logger.debug(f"🔧 Ensuring grid size: {required_rows} rows x {required_cols} cols")
            
            grid_start_time = time.time()
            self._ensure_grid_size(required_rows, required_cols, spreadsheet_id, sheet_name)
            grid_elapsed = time.time() - grid_start_time
            if self.debug_logs:
                logger.debug(f"✅ Grid size ensured in {grid_elapsed:.2f} seconds")

            # Write header first
            if self.debug_logs:
                logger.debug(f"📝 Writing header to row {self.start_row}...")
            header_body = {"values": [header]}
            header_range = self._format_range(cell_range=f"A{self.start_row}", sheet_name=sheet_name)
            if self.debug_logs:
                logger.debug(f"   Header range: {header_range}")
            
            header_start_time = time.time()
            try:
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=header_range,
                    valueInputOption="RAW",
                    body=header_body
                ).execute()
                header_elapsed = time.time() - header_start_time
                if self.debug_logs:
                    logger.info(f"✅ Header successfully written to {sheet_name} in {header_elapsed:.2f} seconds.")
                else:
                    logger.info(f"✅ Header successfully written to {sheet_name}.")
            except Exception as e:
                header_elapsed = time.time() - header_start_time
                logger.error(f"❌ Error writing header after {header_elapsed:.2f} seconds: {type(e).__name__}: {e}")
                if self.debug_logs:
                    import traceback
                    logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
                raise

            # Calculate number of chunks
            total_rows = len(values)
            num_chunks = math.ceil(total_rows / self.chunk_size)
            if self.debug_logs:
                logger.debug(f"📊 Total rows: {total_rows}, will write in {num_chunks} chunks")

            for i in range(num_chunks):
                chunk_start_idx = i * self.chunk_size
                chunk_end_idx = min((i + 1) * self.chunk_size, total_rows)
                chunk = values[chunk_start_idx:chunk_end_idx]
                start_row = self.start_row + 1 + chunk_start_idx  # +1 for header
                range_to_write = self._format_range(cell_range=f"A{start_row}", sheet_name=sheet_name)
                
                if self.debug_logs:
                    logger.debug(f"📦 Chunk {i+1}/{num_chunks}: rows {chunk_start_idx+1}-{chunk_end_idx}, writing to {range_to_write}")
                    logger.debug(f"   Chunk size: {len(chunk)} rows")

                body = {"values": chunk}

                retries = 3
                for attempt in range(retries):
                    try:
                        chunk_start_time = time.time()
                        if self.debug_logs:
                            logger.debug(f"   Attempt {attempt+1}/{retries}: Calling values().update() for chunk {i+1}...")
                        self.sheets_service.spreadsheets().values().update(
                            spreadsheetId=spreadsheet_id,
                            range=range_to_write,
                            valueInputOption="RAW",
                            body=body
                        ).execute()
                        chunk_elapsed = time.time() - chunk_start_time
                        if self.debug_logs:
                            logger.info(f"✅ Chunk {i+1}/{num_chunks} written to {range_to_write} in {chunk_elapsed:.2f} seconds.")
                        else:
                            logger.info(f"✅ Chunk {i+1}/{num_chunks} written to {range_to_write}.")
                        break
                    except HttpError as error:
                        chunk_elapsed = time.time() - chunk_start_time
                        logger.error(f"⚠️ Attempt {attempt+1} failed for chunk {i+1} after {chunk_elapsed:.2f} seconds: {type(error).__name__}: {error}")
                        if self.debug_logs:
                            logger.debug(f"   Error details: {error.error_details if hasattr(error, 'error_details') else 'N/A'}")
                            logger.debug(f"   HTTP status: {error.resp.status if hasattr(error, 'resp') and hasattr(error.resp, 'status') else 'N/A'}")
                        if attempt < retries - 1:
                            wait_time = 5 * (attempt + 1)
                            if self.debug_logs:
                                logger.debug(f"   Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                    except Exception as error:
                        chunk_elapsed = time.time() - chunk_start_time
                        logger.error(f"⚠️ Unexpected error for chunk {i+1} after {chunk_elapsed:.2f} seconds: {type(error).__name__}: {error}")
                        if self.debug_logs:
                            import traceback
                            logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
                        if attempt < retries - 1:
                            wait_time = 5 * (attempt + 1)
                            if self.debug_logs:
                                logger.debug(f"   Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                else:
                    raise TimeoutError(f"❌ Failed to write chunk {i+1} after {retries} retries.")

        except HttpError as error:
            logger.error(f"❌ HttpError writing to sheet {sheet_name}: {type(error).__name__}: {error}")
            if self.debug_logs:
                logger.debug(f"   Error details: {error.error_details if hasattr(error, 'error_details') else 'N/A'}")
                logger.debug(f"   HTTP status: {error.resp.status if hasattr(error, 'resp') and hasattr(error.resp, 'status') else 'N/A'}")
                import traceback
                logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
            raise
        except Exception as error:
            logger.error(f"❌ Unexpected error writing to sheet {sheet_name}: {type(error).__name__}: {error}")
            if self.debug_logs:
                import traceback
                logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
            raise

    def clean_data(
        self, 
        value
        ):
        """Cleans BigQuery data before writing to Google Sheets."""
        excel_base_date = datetime.date(1900, 1, 1)

        if isinstance(value, decimal.Decimal):
            return float(value)
        if pd.isna(value):
            return ""
        if isinstance(value, datetime.date):
            delta = value - excel_base_date
            return delta.days + 2  # Adjust for Excel's leap year bug
        if isinstance(value, (pd.Timestamp, datetime.datetime)):
            # Check for NaT before converting to date
            if pd.isna(value):
                return ""
            delta = value.date() - excel_base_date
            return delta.days
        return value


    def load_bq_to_sheets(self):
        """Fetches BigQuery data and loads it into Google Sheets."""
        if self.debug_logs:
            logger.info(f"🚀 Starting load_bq_to_sheets for spreadsheet '{self.spreadsheet_id}'")
            logger.info(f"📊 Sheets to process: {list(self.spreadsheet_details.keys())}")
        
        for sheet_name in self.spreadsheet_details.keys():
            logger.info("=" * 100)
            logger.info(f"📄 Processing sheet: '{sheet_name}'")
            logger.info("=" * 100)

            query = self.spreadsheet_details[sheet_name]['query']
            if self.debug_logs:
                logger.debug(f"   Query: {query[:200]}..." if len(query) > 200 else f"   Query: {query}")
            
            attempts = 3
            for attempt in range(attempts):
                try:
                    logger.info(f"Attempt {attempt+1} of {attempts} to load data into Google Sheet `{sheet_name}`.")
                    if self.debug_logs:
                        logger.info(f"   Spreadsheet ID: {self.spreadsheet_id}")

                    if self.debug_logs:
                        logger.debug(f"📊 Running BigQuery query for sheet '{sheet_name}'...")
                    query_start_time = time.time()
                    df = run_bq_query(query)
                    query_elapsed = time.time() - query_start_time
                    if self.debug_logs:
                        logger.info(f"✅ BigQuery query completed in {query_elapsed:.2f} seconds. Retrieved {len(df)} rows, {len(df.columns)} columns")
                        logger.debug(f"   Columns: {list(df.columns)}")
                    else:
                        logger.info(f"✅ BigQuery query completed. Retrieved {len(df)} rows, {len(df.columns)} columns")
                    
                    if self.debug_logs:
                        logger.debug(f"🧹 Cleaning data for sheet '{sheet_name}'...")
                    clean_start_time = time.time()
                    df = df.applymap(self.clean_data)
                    clean_elapsed = time.time() - clean_start_time
                    if self.debug_logs:
                        logger.debug(f"✅ Data cleaning completed in {clean_elapsed:.2f} seconds")
                    
                    if self.debug_logs:
                        logger.debug(f"🧹 Clearing sheet '{sheet_name}'...")
                    clear_start_time = time.time()
                    self.clear_sheet(
                        spreadsheet_id=self.spreadsheet_id,
                        sheet_name=sheet_name
                    )
                    clear_elapsed = time.time() - clear_start_time
                    if self.debug_logs:
                        logger.debug(f"✅ Sheet cleared in {clear_elapsed:.2f} seconds")
                    
                    if self.debug_logs:
                        logger.debug(f"✍️ Writing data to sheet '{sheet_name}'...")
                    write_start_time = time.time()
                    self.write_to_sheets(
                        dataframe=df,
                        spreadsheet_id=self.spreadsheet_id,
                        sheet_name=sheet_name
                    )
                    write_elapsed = time.time() - write_start_time
                    if self.debug_logs:
                        logger.info(f"✅ Data successfully loaded to Google Sheet `{sheet_name}` in {write_elapsed:.2f} seconds.")
                    else:
                        logger.info(f"✅ Data successfully loaded to Google Sheet `{sheet_name}`.")
                    break
                except Exception as e:
                    logger.error(f"❌ Error loading data into Google Sheet `{sheet_name}`: {type(e).__name__}: {e}")
                    if self.debug_logs:
                        logger.error(f"   Spreadsheet ID: {self.spreadsheet_id}")
                        logger.error(f"   Sheet name: '{sheet_name}'")
                        import traceback
                        logger.debug(f"   Full traceback:\n{traceback.format_exc()}")
                    if attempt < attempts - 1:
                        wait_time = 5 * (attempt + 1)
                        logger.info(f"   Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
            else:
                if self.debug_logs:
                    logger.error(f"❌ All {attempts} attempts failed for sheet '{sheet_name}'")
                raise RuntimeError(f"❌ Failed to load data into Google Sheet `{sheet_name}` after {attempts} attempts.")

            time.sleep(15)
