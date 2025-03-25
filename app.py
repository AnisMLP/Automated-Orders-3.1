from flask import Flask, request, jsonify
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv
import queue
import threading
import time
from googleapiclient.errors import HttpError

# Load .env file
load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1LIU3utVTmAgy9KXm1D9XcM2YiNKq3d7eJDSYWK-SpF0'
SHEET_NAME = 'Orders 3.1'
RANGE_NAME = f'{SHEET_NAME}!A1'
SECRET_KEY = os.getenv('SECRET_KEY', 'your_default_secret_key')
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')
IS_RENDER = os.getenv('RENDER') == 'true'

try:
    if IS_RENDER:
        if not GOOGLE_CREDENTIALS:
            raise ValueError("GOOGLE_CREDENTIALS environment variable is not set on Render")
        logger.info("Loading credentials from GOOGLE_CREDENTIALS on Render")
        creds = service_account.Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDENTIALS), scopes=SCOPES)
    else:
        logger.info("Running locally, falling back to credentials.json")
        creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    logger.info("Google Sheets API initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Google Sheets API: {str(e)}")
    service = None

# Queue and list for orders
order_queue = queue.Queue()
queued_orders = []

# Helper functions
def format_date(date_str):
    date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return f"{date.year}-{str(date.month).zfill(2)}-{str(date.day).zfill(2)}"

def group_skus_by_vendor(line_items):
    sku_by_vendor = {}
    for item in line_items:
        sku, vendor = item['sku'], item.get('vendor', '')  # Default to '' if vendor missing
        if vendor not in sku_by_vendor:
            sku_by_vendor[vendor] = [sku]
        else:
            sku_by_vendor[vendor].append(sku)
    return sku_by_vendor

def get_last_row():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
    ).execute()
    values = result.get('values', [])
    return len(values) + 1 if values else 1

# Background worker
def process_queue():
    while True:
        try:
            data, action = order_queue.get()
            logger.info(f"Processing order: {data.get('order_number')}")

            max_retries = 5
            for attempt in range(max_retries):
                try:
                    if action == "add_new_orders":
                        add_new_orders(data)
                    elif action == "add_backup_shipping_note":
                        add_backup_shipping_note(data)
                    elif action == "remove_fulfilled_sku":
                        remove_fulfilled_sku(data)

                    queued_orders.remove((data, action))
                    logger.info(f"Successfully processed and removed: {data.get('order_number')}")
                    break
                except HttpError as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"API error for {data.get('order_number')}, attempt {attempt + 1}/{max_retries}: {str(e)}. Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                        break
                except Exception as e:
                    logger.error(f"Error processing {data.get('order_number')}: {str(e)}")
                    break

            order_queue.task_done()
            time.sleep(1)  # 1-second delay between orders

        except Exception as e:
            logger.error(f"Queue processing error: {str(e)}")
            order_queue.task_done()

# Start worker
worker_thread = threading.Thread(target=process_queue, daemon=True)
worker_thread.start()

# Webhook route
@app.route('/webhook', methods=['POST'])
def handle_webhook():
    if not service:
        return jsonify({"error": "Google Sheets API not initialized"}), 500

    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403

    try:
        data = request.get_json(silent=True)
        if data is None:
            logger.error("Invalid JSON received")
            return jsonify({"error": "Invalid JSON data provided"}), 400

        if not isinstance(data, dict) or not data:
            logger.error("Empty or invalid data")
            return jsonify({"error": "No valid data provided"}), 400

        logger.info(f"Received data: {data}")
        action = request.args.get('action', '')

        if data.get("backup_shipping_note"):
            order_queue.put((data, "add_backup_shipping_note"))
            queued_orders.append((data, "add_backup_shipping_note"))
            logger.info(f"Queued backup shipping note: {data.get('order_number')}")
            return jsonify({"status": "queued", "message": "Backup shipping note queued"}), 202
        elif action == 'addNewOrders':
            order_queue.put((data, "add_new_orders"))
            queued_orders.append((data, "add_new_orders"))
            logger.info(f"Queued new order: {data.get('order_number')}")
            return jsonify({"status": "queued", "message": "Order queued"}), 202
        elif action == 'removeFulfilledSKU':
            order_queue.put((data, "remove_fulfilled_sku"))
            queued_orders.append((data, "remove_fulfilled_sku"))
            logger.info(f"Queued SKU removal: {data.get('order_number')}")
            return jsonify({"status": "queued", "message": "SKU removal queued"}), 202
        else:
            logger.error(f"No valid action: {action}")
            return jsonify({"error": "No valid action or backup note provided"}), 400

    except Exception as e:
        logger.error(f"Error in webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Sheet functions (from old code)
def add_new_orders(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id", "").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    sku_by_vendor = group_skus_by_vendor(line_items)
    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country]
        for vendor, skus in sku_by_vendor.items()
    ]

    start_row = get_last_row()
    range_to_write = f'{SHEET_NAME}!A{start_row}'
    body = {'values': rows_data}
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_to_write,
        valueInputOption='RAW', body=body
    ).execute()

    apply_formulas()
    delete_rows()
    delete_duplicate_rows()

def add_backup_shipping_note(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id", "").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    backup_note = data.get("backup_shipping_note")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    sku_by_vendor = group_skus_by_vendor(line_items)
    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", backup_note]
        for vendor, skus in sku_by_vendor.items()
    ]

    start_row = get_last_row()
    range_to_write = f'{SHEET_NAME}!A{start_row}:K{start_row + len(rows_data) - 1}'
    body = {'values': rows_data}
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_to_write,
        valueInputOption='RAW', body=body
    ).execute()

    apply_formulas()
    delete_rows()
    delete_duplicate_rows()

def remove_fulfilled_sku(data):
    order_number = data.get("order_number")
    line_items = data.get("line_items", [])

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
        row_str = ','.join(map(str, row))
        if row_str in unique_rows:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:K{i+1}')
        else:
            unique_rows[row_str] = True

    for row_range in rows_to_clear:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=row_range
        ).execute()

# Queue viewer
@app.route('/orders/queued', methods=['GET'])
def get_queued_orders():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403

    queued_list = [
        {
            "order_number": order_data.get("order_number"),
            "action": action,
            "order_created": order_data.get("order_created"),
            "line_items": order_data.get("line_items", []),
            "backup_shipping_note": order_data.get("backup_shipping_note", None)
        }
        for order_data, action in queued_orders
    ]

    return jsonify({
        "status": "success",
        "queued_orders": queued_list,
        "queue_size": len(queued_list)
    }), 200

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)