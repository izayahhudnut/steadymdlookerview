import os
import requests
from datetime import datetime, timedelta
import looker_sdk
from flask import Flask, jsonify, request

# Explicitly set environment variables for Looker SDK
os.environ["LOOKERSDK_BASE_URL"] = os.getenv("LOOKERSDK_BASE_URL", "https://steadymd.cloud.looker.com")
os.environ["LOOKERSDK_API_VERSION"] = os.getenv("LOOKERSDK_API_VERSION", "4.0")
os.environ["LOOKERSDK_VERIFY_SSL"] = os.getenv("LOOKERSDK_VERIFY_SSL", "true")
os.environ["LOOKERSDK_TIMEOUT"] = os.getenv("LOOKERSDK_TIMEOUT", "120")
os.environ["LOOKERSDK_CLIENT_ID"] = os.getenv("LOOKERSDK_CLIENT_ID", "kpDHcBG5vpNRCDsHpJhh")
os.environ["LOOKERSDK_CLIENT_SECRET"] = os.getenv("LOOKERSDK_CLIENT_SECRET", "MTfCvpXfcGG6cS98fbyqgKpD")

# Zapier webhook URL
ZAPIER_WEBHOOK = os.getenv("ZAPIER_WEBHOOK", "https://hooks.zapier.com/hooks/catch/22420495/20tp3y7/")

# Calculate date 7 days ago
seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

def get_fields_data(sdk, explore_name, limit=200):
    """Get data for multiple fields with date filter"""
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
                "fact_consult.consult_created_time": f"{seven_days_ago} to 9999-12-31"
            },
            "limit": limit
        }

        # Request CSV format directly
        result = sdk.run_inline_query(
            result_format="csv",
            body=query
        )
        return result
    except Exception as e:
        return f"Error fetching data: {str(e)}"

def send_to_zapier(file_path):
    """Send CSV file to Zapier webhook"""
    try:
        # Prepare the file for upload
        files = {
            'file': (os.path.basename(file_path), open(file_path, 'rb'), 'text/csv')
        }

        # Add additional data
        data = {
            'timestamp': datetime.now().isoformat()
        }

        # Send POST request to Zapier webhook with file
        response = requests.post(ZAPIER_WEBHOOK, files=files, data=data)

        # Check if request was successful
        if response.status_code == 200:
            return True, "CSV file successfully sent to Zapier"
        else:
            return False, f"Error sending to Zapier: Status code {response.status_code}"

    except Exception as e:
        return False, f"Error sending to Zapier: {str(e)}"

# Create Flask app
app = Flask(__name__)

# Main function to handle the API request
@app.route('/fetch_and_send', methods=['POST'])
def handler():
    try:
        # Get the parameters from the frontend (as JSON)
        data = request.get_json()

        explore_name = data.get("explore", "fact_clinician_calendar_qtr_hr")
        limit = data.get("limit", 200)
        no_limit = data.get("noLimit", False)

        if no_limit:
            limit = None  # Set to None if no limit is selected

        print("Initializing Looker SDK...")
        sdk = looker_sdk.init40()
        print("SDK initialized successfully")

        print(f"\nFetching data for {explore_name} with limit {limit}...")
        csv_data = get_fields_data(sdk, explore_name, limit)

        if isinstance(csv_data, str):
            # Create filename with view name and current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            output_file = f'{explore_name}_{current_date}.csv'
           
            # Write the CSV data to file
            with open(output_file, 'w', newline='') as csvfile:
                csvfile.write(csv_data)

            # Count the number of lines (subtract 1 for header)
            num_records = len(csv_data.splitlines()) - 1
            print(f"\nData successfully exported to {output_file}")
            print(f"Total records exported: {num_records}")

            # Send to Zapier
            print("\nSending CSV file to Zapier...")
            success, message = send_to_zapier(output_file)
            return jsonify({"message": message, "records_exported": num_records, "filename": output_file}), 200
        else:
            return jsonify({"error": csv_data}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the Flask app with proper host and port for Render
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render automatically sets this port
    app.run(host='0.0.0.0', port=port)