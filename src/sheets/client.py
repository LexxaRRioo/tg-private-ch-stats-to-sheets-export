import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
from datetime import datetime, date
from gspread.exceptions import WorksheetNotFound


class SheetStorage:
    def __init__(self, credentials_path, spreadsheet_url):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_path, scope
        )
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_url(spreadsheet_url)
        self.logger = logging.getLogger(__name__)

    def _get_or_create_sheet(self, name):
        try:
            return self.spreadsheet.worksheet(name)
        except WorksheetNotFound:
            return self.spreadsheet.add_worksheet(name, 1000, 26)

    def _convert_dates_to_strings(self, df):
        """Convert all date/datetime columns to strings."""
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or \
               (not df.empty and isinstance(df[col].iloc[0], (datetime, date))):
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df

    def merge_data(self, sheet_name, new_data, config):
        self.logger.info(f"Starting merge for sheet: '{sheet_name}' ...")
        sheet = self._get_or_create_sheet(sheet_name)

        # Convert input data to DataFrame
        new_df = pd.DataFrame(new_data)

        # Handle empty DataFrame case
        if new_df.empty:
            self.logger.warning(f"No data to update in sheet '{sheet_name}'")
            return

        # Convert dates to strings in new data
        new_df = self._convert_dates_to_strings(new_df)

        # Handle channels_daily special case
        if sheet_name == "channels_daily":
            merged = (
                new_df.sort_values("processed_at")
                .groupby("channel_id")
                .last()
                .reset_index()
            )
        else:
            # Handle other sheets
            existing_data = pd.DataFrame(sheet.get_all_records())
            
            if not existing_data.empty:
                # Convert dates in existing data
                existing_data = self._convert_dates_to_strings(existing_data)
                # Merge with deduplication
                merged = pd.concat([existing_data, new_df]).drop_duplicates(
                    subset=config["key_columns"], keep="last"
                )
            else:
                merged = new_df

        # Update sheet
        sheet.clear()
        # Convert to nested list and ensure all values are strings
        data_to_update = [merged.columns.values.tolist()] + merged.values.tolist()
        data_to_update = [[str(cell) if isinstance(cell, (date, datetime)) else cell 
                          for cell in row] for row in data_to_update]
        
        sheet.update(data_to_update)
        self.logger.info(f"Successfully updated '{sheet_name}' \n")
