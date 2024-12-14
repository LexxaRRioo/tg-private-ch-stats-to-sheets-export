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

    def merge_data(self, sheet_name, new_data, config):
        self.logger.info(f"Starting merge for sheet: '{sheet_name}' ...")
        sheet = self._get_or_create_sheet(sheet_name)

        # For channels_daily, keep only the latest snapshot for each channel
        if sheet_name == "channels_daily":
            new_df = pd.DataFrame(new_data)

            # If new_df is not empty
            if not new_df.empty:
                # Keep timestamp columns in datetime for proper sorting
                if "processed_at" in new_df.columns:
                    new_df["processed_at"] = pd.to_datetime(new_df["processed_at"])

                # Take the latest record for each channel
                latest_snapshots = (
                    new_df.sort_values("processed_at")
                    .groupby("channel_id")
                    .last()
                    .reset_index()
                )

                # Convert timestamp back to string for sheet storage
                if "processed_at" in latest_snapshots.columns:
                    latest_snapshots["processed_at"] = latest_snapshots[
                        "processed_at"
                    ].dt.strftime("%Y-%m-%d %H:%M:%S")

                merged = latest_snapshots
            else:
                merged = new_df
        else:
            # Original logic for other sheets
            existing_data = pd.DataFrame(sheet.get_all_records())
            new_df = pd.DataFrame(new_data)

            if not new_df.empty:  # Only process if we have new data
                for col in new_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(new_df[col]) or isinstance(
                        new_df[col].iloc[0], (datetime, date)
                    ):
                        new_df[col] = pd.to_datetime(new_df[col]).dt.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if not existing_data.empty and col in existing_data.columns:
                            existing_data[col] = pd.to_datetime(
                                existing_data[col]
                            ).dt.strftime("%Y-%m-%d %H:%M:%S")

                if not existing_data.empty:
                    merged = pd.concat([existing_data, new_df]).drop_duplicates(
                        subset=config["key_columns"], keep="last"
                    )
                else:
                    merged = new_df
            else:
                merged = new_df

        # Only try to update sheet if we have data
        if not merged.empty:
            sheet.clear()
            sheet.update([merged.columns.values.tolist()] + merged.values.tolist())
        else:
            self.logger.warning(f"No data to update in sheet '{sheet_name}'")

        self.logger.info(f"Successfully updated '{sheet_name}' \n")

    def get_last_message_id(self, entity_id, sheet_name="chat_topics_hourly"):
        """Get last message ID for entity from specified sheet"""
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
            self.logger.info(df.head())
            self.logger.info(f"entity is: {entity_id}, sheet_name: {sheet_name}")

            if df.empty:
                return None

            if sheet_name == "channels_daily":
                mask = df["channel_id"] == entity_id
            else:
                mask = df["chat_id"] == entity_id

            filtered = df[mask]
            self.logger.info(filtered.head())

            if filtered.empty:
                return None

            return filtered["last_message_id"].max()

        except Exception as e:
            self.logger.error(
                f"Error getting last message ID for {entity_id} from {sheet_name}: {str(e)}"
            )
            return None
