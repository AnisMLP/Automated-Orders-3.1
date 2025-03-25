from flask import Flask, request, jsonify
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os

# Initialize Flask app
app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1LIU3utVTmAgy9KXm1D9XcM2YiNKq3d7eJDSYWK-SpF0'  # Replace with your Google Sheet ID
SHEET_NAME = 'Orders 3.1'
RANGE_NAME = f'{SHEET_NAME}!A1'

# Load credentials and secret key from environment or file
SECRET_KEY = os.getenv('SECRET_KEY', 'your_default_secret_key')  # Set in Render env vars
try:
    creds = service_account.Credentials.from_service_account_file(
        'credentials.json', scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
except Exception as e:
    logger.error(f"Failed to initialize Google Sheets API: {str(e)}")
    service = None

# Helper function to format date
def format_date(date_str):
    date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return f"{date.year}-{str(date.month).zfill(2)}-{str(date.day).zfill(2)}"

# Helper function to group SKUs by vendor
def group_skus_by_vendor(line_items):
    sku_by_vendor = {}
    for item in line_items:
        sku, vendor = item['sku'], item['vendor']
        if vendor not in sku_by_vendor:
            sku_by_vendor[vendor] = [sku]
        else:
            sku_by_vendor[vendor].append(sku)
    return sku_by_vendor

# Route to handle incoming POST requests from Flow
@app.route('/webhook', methods=['POST'])
def handle_webhook():
    if not service:
        return jsonify({"error": "Google Sheets API not initialized"}), 500

    # Check secret key
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        action = request.args.get('action', '')
        if data.get("backup_shipping_note"):
            return add_backup_shipping_note(data)
        elif action == 'addNewOrders':
            return add_new_orders(data)
        elif action == 'removeFulfilledSKU':
            return remove_fulfilled_sku(data)
        else:
            return jsonify({"error": "No valid action or backup note provided"}), 400

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"error": str(e)}), 500

def add_new_orders(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    sku_by_vendor = group_skus_by_vendor(line_items)
    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country]
        for vendor, skus in sku_by_vendor.items()
    ]

    # Append to sheet
    body = {'values': rows_data}
    result = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='RAW', body=body
    ).execute()

    apply_formulas()
    delete_rows()
    delete_duplicate_rows()

    return jsonify({"status": "success", "message": "Data successfully written to the sheet"}), 200

def add_backup_shipping_note(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    backup_note = data.get("backup_shipping_note")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    sku_by_vendor = group_skus_by_vendor(line_items)
    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", backup_note]
        for vendor, skus in sku_by_vendor.items()
    ]

    # Ensure 11 columns exist (A to K), then append
    body = {'values': rows_data}
    result = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A1:K',
        valueInputOption='RAW', body=body
    ).execute()

    apply_formulas()
    delete_rows()
    delete_duplicate_rows()

    return jsonify({"status": "success", "message": "Data with backup shipping note added successfully"}), 200

def remove_fulfilled_sku(data):
    order_number = data.get("order_number")
    line_items = data.get("line_items", [])

    # Fetch current sheet data
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
    ).execute()
    values = result.get('values', [])

    for i, row in enumerate(values):
        if len(row) > 1 and row[1] == order_number:
            skus = row[3].split(', ') if len(row) > 3 else []
            for item in line_items:
                if item['sku'] in skus:
                    skus.remove(item['sku'])
            if not skus:
                # Delete row by clearing it (API doesn't support direct row deletion)
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:K{i+1}'
                ).execute()
            else:
                values[i][3] = ', '.join(skus)
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:K{i+1}',
                    valueInputOption='RAW', body={'values': [values[i]]}
                ).execute()
            break

    return jsonify({"status": "success", "message": "Fulfilled SKUs removed"}), 200

def apply_formulas():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
    ).execute()
    values = result.get('values', [])
    last_row = len(values) + 1 if values else 2

    formulas = []
    for row in range(2, last_row + 1):
        formula = (
            f'=IFNA(IF(F{row} = "US", IFNA(XLOOKUP(E{row}, assign_types!D:D, assign_types!E:E), '
            f'XLOOKUP(E{row}, assign_types!A:A, assign_types!B:B)), XLOOKUP(E{row}, assign_types!A:A, assign_types!B:B)), "")'
        )
        formulas.append([formula])

    if formulas:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!G2:G{last_row}',
            valueInputOption='USER_ENTERED', body={'values': formulas}
        ).execute()

def delete_rows():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
    ).execute()
    values = result.get('values', [])
    rows_to_clear = []

    for i, row in enumerate(values):
        sku_cell = row[3] if len(row) > 3 else ''
        if sku_cell in ['Tip', 'MLP-AIR-FRESHENER', '']:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:K{i+1}')

    for row_range in rows_to_clear:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=row_range
        ).execute()

def delete_duplicate_rows():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
    ).execute()
    values = result.get('values', [])
    unique_rows = {}
    rows_to_clear = []

    for i, row in enumerate(values):
        row_str = ','.join(row)
        if row_str in unique_rows:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:K{i+1}')
        else:
            unique_rows[row_str] = True

    for row_range in rows_to_clear:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=row_range
        ).execute()

# Health check route
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)