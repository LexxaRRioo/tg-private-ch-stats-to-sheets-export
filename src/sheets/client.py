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
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_url(spreadsheet_url)
        self.logger = logging.getLogger(__name__)

    def _get_or_create_sheet(self, name):
        try:
            return self.spreadsheet.worksheet(name)
        except WorksheetNotFound:
            return self.spreadsheet.add_worksheet(name, 1000, 26)

    def merge_data(self, sheet_name, new_data, config):
        self.logger.info(f"Starting merge for sheet: '{sheet_name}' ...")
        sheet = self._get_or_create_sheet(sheet_name)

        existing_data = pd.DataFrame(sheet.get_all_records())
        new_df = pd.DataFrame(new_data)

        for col in new_df.columns:
            if pd.api.types.is_datetime64_any_dtype(new_df[col]) or isinstance(
                new_df[col].iloc[0], (datetime, date)
            ):
                new_df[col] = pd.to_datetime(new_df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
                if not existing_data.empty and col in existing_data.columns:
                    existing_data[col] = pd.to_datetime(existing_data[col]).dt.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

        if not existing_data.empty:
            merged = pd.concat([existing_data, new_df]).drop_duplicates(
                subset=config["key_columns"], keep="last"
            )
        else:
            merged = new_df

        sheet.clear()
        sheet.update([merged.columns.values.tolist()] + merged.values.tolist())
        self.logger.info(f"Successfully updated '{sheet_name}' \n")