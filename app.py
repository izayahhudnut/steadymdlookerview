import os
import requests
from datetime import datetime, timedelta
import looker_sdk
from flask import Flask, jsonify, request

# Load Google credentials if not already set
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/etc/secrets/credentials.json")


# Set environment variables for Looker SDK
os.environ["LOOKERSDK_BASE_URL"] = os.getenv("LOOKERSDK_BASE_URL", "https://steadymd.cloud.looker.com")
os.environ["LOOKERSDK_API_VERSION"] = os.getenv("LOOKERSDK_API_VERSION", "4.0")
os.environ["LOOKERSDK_VERIFY_SSL"] = os.getenv("LOOKERSDK_VERIFY_SSL", "true")
os.environ["LOOKERSDK_TIMEOUT"] = os.getenv("LOOKERSDK_TIMEOUT", "120")
os.environ["LOOKERSDK_CLIENT_ID"] = os.getenv("LOOKERSDK_CLIENT_ID", "kpDHcBG5vpNRCDsHpJhh")
os.environ["LOOKERSDK_CLIENT_SECRET"] = os.getenv("LOOKERSDK_CLIENT_SECRET", "MTfCvpXfcGG6cS98fbyqgKpD")

# Zapier webhook URL
ZAPIER_WEBHOOK = os.getenv("ZAPIER_WEBHOOK", "https://hooks.zapier.com/hooks/catch/22420495/20tp3y7/")

def get_fields_data(sdk, explore_name, start_date, end_date, limit=200):
    """Get data with date filters and optional limit"""
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

        result = sdk.run_inline_query(result_format="csv", body=query)
        return result
    except Exception as e:
        return f"Error fetching data: {str(e)}"

def send_to_zapier(file_path):
    try:
        files = {
            'file': (os.path.basename(file_path), open(file_path, 'rb'), 'text/csv')
        }
        data = {'timestamp': datetime.now().isoformat()}
        response = requests.post(ZAPIER_WEBHOOK, files=files, data=data)

        if response.status_code == 200:
            return True, "CSV file successfully sent to Zapier"
        else:
            return False, f"Error sending to Zapier: Status code {response.status_code}"
    except Exception as e:
        return False, f"Error sending to Zapier: {str(e)}"

app = Flask(__name__)

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

        # Use default if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = '9999-12-31'

        sdk = looker_sdk.init40()
        print(f"\nFetching data for {explore_name} from {start_date} to {end_date} with limit {limit}...")
        csv_data = get_fields_data(sdk, explore_name, start_date, end_date, limit)

        if isinstance(csv_data, str):
            current_date = datetime.now().strftime('%Y-%m-%d')
            output_file = f'{explore_name}_{current_date}.csv'

            with open(output_file, 'w', newline='') as csvfile:
                csvfile.write(csv_data)

            num_records = len(csv_data.splitlines()) - 1
            success, message = send_to_zapier(output_file)

            return jsonify({
                "message": message,
                "records_exported": num_records,
                "filename": output_file,
                "sheet_url": "https://docs.google.com/spreadsheets/d/1tFMOyCtEvQnOtsUyPe1HdYVejZOQfUaipFlLrWVIZP8/edit#gid=0"
            }), 200
        else:
            return jsonify({"error": csv_data}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
