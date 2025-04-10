import os
import csv
import io
from datetime import datetime, timedelta
import looker_sdk
from flask import Flask, jsonify, request
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Local or deployed environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

# Looker SDK env vars
os.environ["LOOKERSDK_BASE_URL"] = os.getenv("LOOKERSDK_BASE_URL", "https://steadymd.cloud.looker.com")
os.environ["LOOKERSDK_API_VERSION"] = os.getenv("LOOKERSDK_API_VERSION", "4.0")
os.environ["LOOKERSDK_VERIFY_SSL"] = os.getenv("LOOKERSDK_VERIFY_SSL", "true")
os.environ["LOOKERSDK_TIMEOUT"] = os.getenv("LOOKERSDK_TIMEOUT", "120")
os.environ["LOOKERSDK_CLIENT_ID"] = os.getenv("LOOKERSDK_CLIENT_ID", "kpDHcBG5vpNRCDsHpJhh")
os.environ["LOOKERSDK_CLIENT_SECRET"] = os.getenv("LOOKERSDK_CLIENT_SECRET", "MTfCvpXfcGG6cS98fbyqgKpD")

# Google Sheet ID and range
SHEET_ID = "1ELA4sOTPm0MKQzxnl0-hXhyBfwk0s2r09RDfraO84DA"
RANGE_NAME = "Sheet1!A1"

app = Flask(__name__)

def get_fields_data(sdk, explore_name, start_date, end_date, limit=200):
    try:
        query = {
            "model": "smd_dw",
            "view": explore_name,
            "fields": [
                "fact_partner.partner_name",
                "fact_consult.consult_guid",
                "fact_consult.consult_started_time",
                "fact_consult.consult_created_time",
                "fact_consult.consult_completed_time",
                "fact_clinician.full_name",
                "fact_consult.is_async",
                "fact_consult.is_scheduled",
                "fact_consult.started_during_shift"
            ],
            "filters": {
                "fact_consult.consult_created_time": f"{start_date} to {end_date}"
            },
        }
        if limit:
            query["limit"] = limit
        return sdk.run_inline_query(result_format="csv", body=query)
    except Exception as e:
        return f"Error fetching data: {str(e)}"

def write_csv_string_to_google_sheet(csv_string, sheet_id, range_name="Sheet1!A1"):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build("sheets", "v4", credentials=credentials)

        reader = csv.reader(io.StringIO(csv_string))
        values = list(reader)

        body = {"values": values}
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()
        return True, f"✅ {result.get('updatedCells')} cells written to Google Sheet."
    except Exception as e:
        return False, f"❌ Failed to write to Google Sheets: {str(e)}"

@app.route('/fetch_and_send', methods=['POST'])
def handler():
    try:
        data = request.get_json()
        explore_name = data.get("explore", "fact_clinician_calendar_qtr_hr")
        limit = data.get("limit", 200)
        no_limit = data.get("noLimit", False)
        start_date = data.get("startDate")
        end_date = data.get("endDate")

        if no_limit:
            limit = None
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = "9999-12-31"

        sdk = looker_sdk.init40()
        print(f"Streaming data for {explore_name} from {start_date} to {end_date} with limit {limit}...")

        csv_data = get_fields_data(sdk, explore_name, start_date, end_date, limit)

        if isinstance(csv_data, str):
            num_records = len(csv_data.splitlines()) - 1
            success, message = write_csv_string_to_google_sheet(csv_data, SHEET_ID, RANGE_NAME)

            return jsonify({
                "message": message,
                "records_exported": num_records,
                "sheet_url": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid=0"
            }), 200
        else:
            return jsonify({"error": csv_data}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host='0.0.0.0', port=port)
