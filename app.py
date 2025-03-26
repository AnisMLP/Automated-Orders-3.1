from flask import Flask, request, jsonify
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv
import re
import time

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1LIU3utVTmAgy9KXm1D9XcM2YiNKq3d7eJDSYWK-SpF0'
SHEET_NAME = 'Orders 3.1'
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

logger.info("App started with trailing comma fix v2.2 + queue")

# File to store queued orders
QUEUE_FILE = '/tmp/order_queue.json' if IS_RENDER else 'order_queue.json'

def fix_trailing_commas(json_str):
    """Remove trailing commas from JSON arrays."""
    fixed_str = re.sub(r',(\s*[\]}])', r'\1', json_str)
    return fixed_str

def load_queue():
    """Load the queue from file."""
    try:
        if os.path.exists(QUEUE_FILE):
            with open(QUEUE_FILE, 'r') as f:
                return json.load(f)
        logger.info(f"No queue file found at {QUEUE_FILE}, starting empty")
        return []
    except Exception as e:
        logger.error(f"Error loading queue: {str(e)}")
        return []

def save_queue(queue):
    """Save the queue to file."""
    try:
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f)
        logger.info(f"Queue saved successfully. Size: {len(queue)}")
    except Exception as e:
        logger.error(f"Error saving queue: {str(e)}")

def format_date(date_str):
    try:
        date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{date.year}-{str(date.month).zfill(2)}-{str(date.day).zfill(2)}"
    except Exception as e:
        logger.error(f"Error formatting date {date_str}: {str(e)}")
        return "Invalid Date"

def group_skus_by_vendor(line_items):
    sku_by_vendor = {}
    for item in line_items:
        sku, vendor = item.get('sku', 'Unknown SKU'), item.get('vendor', 'Unknown Vendor')
        if vendor not in sku_by_vendor:
            sku_by_vendor[vendor] = [sku]
        else:
            sku_by_vendor[vendor].append(sku)
    return sku_by_vendor

def get_last_row():
    if not service:
        logger.error("Google Sheets service not initialized")
        return 1
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:K'
        ).execute()
        values = result.get('values', [])
        return len(values) + 1 if values else 1
    except Exception as e:
        logger.error(f"Error getting last row: {str(e)}")
        return 1

def order_exists_in_sheet(order_number):
    """Check if order is already in Google Sheets."""
    if not service:
        return False
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!B:B'
        ).execute()
        values = result.get('values', [])
        for row in values:
            if row and row[0] == order_number:
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking if order {order_number} exists: {str(e)}")
        return False

def process_order(data):
    """Process a single order and write to Google Sheets."""
    if not service:
        logger.error("Cannot process order: Google Sheets service not initialized")
        return False
    try:
        order_number = data.get("order_number", "Unknown")
        if order_exists_in_sheet(order_number):
            logger.info(f"Order {order_number} already in Google Sheets, skipping")
            return True

        order_id = data.get("order_id", "").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
        order_country = data.get("order_country", "Unknown")
        order_created = format_date(data.get("order_created", ""))
        line_items = data.get("line_items", [])

        logger.info(f"Preparing order {order_number} for processing")
        sku_by_vendor = group_skus_by_vendor(line_items)
        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country]
            for vendor, skus in sku_by_vendor.items()
        ]

        start_row = get_last_row()
        range_to_write = f'{SHEET_NAME}!A{start_row}'
        body = {'values': rows_data}
        logger.info(f"Writing order {order_number} to Google Sheets at {range_to_write}")
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range_to_write,
            valueInputOption='RAW', body=body
        ).execute()

        logger.info(f"Order {order_number} written to Google Sheets, applying formulas")
        apply_formulas()
        logger.info(f"Formulas applied for order {order_number}, deleting rows")
        delete_rows()
        logger.info(f"Rows deleted for order {order_number}, deleting duplicates")
        delete_duplicate_rows()

        logger.info(f"Successfully processed order {order_number}")
        return True
    except Exception as e:
        logger.error(f"Error processing order {data.get('order_number', 'Unknown')}: {str(e)}")
        return False

def process_queue():
    """Process one order from the queue with a delay."""
    queue = load_queue()
    if queue:
        order = queue.pop(0)
        order_number = order.get('order_number', 'Unknown')
        logger.info(f"Starting to process order {order_number} from queue")
        if process_order(order):
            save_queue(queue)
            logger.info(f"Order {order_number} removed from queue. Queue size now: {len(queue)}")
        else:
            logger.warning(f"Failed to process order {order_number}, keeping in queue")
            queue.insert(0, order)
            save_queue(queue)
        logger.info("Waiting 5 seconds before next process to avoid Google Sheets overlap")
        time.sleep(5)  # Increased delay to prevent gaps

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    if not service:
        return jsonify({"error": "Google Sheets API not initialized"}), 500

    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        logger.error(f"Access denied: Invalid key - {provided_key}")
        return jsonify({"error": "Access Denied"}), 403

    raw_body = request.get_data(as_text=True)
    logger.info(f"Raw webhook request body: {raw_body}")
    fixed_body = fix_trailing_commas(raw_body)
    if raw_body != fixed_body:
        logger.info(f"Fixed trailing commas in JSON: {fixed_body}")

    try:
        data = json.loads(fixed_body)
        if not data or 'order_number' not in data:
            logger.error(f"Missing order_number or empty data: {fixed_body}")
            return jsonify({"error": "Invalid or no data provided"}), 400

        action = request.args.get('action', '')
        order_number = data.get('order_number', 'Unknown')
        logger.info(f"Processing webhook for order {order_number} with action {action}")

        if data.get("backup_shipping_note"):
            return add_backup_shipping_note(data)
        elif action == 'addNewOrders':
            queue = load_queue()
            if any(o.get('order_number') == order_number for o in queue):
                logger.info(f"Order {order_number} already in queue, skipping")
                return jsonify({"status": "queued", "message": "Order already in queue"}), 200
            queue.append(data)
            save_queue(queue)
            logger.info(f"Order {order_number} added to queue. Queue size: {len(queue)}")
            return jsonify({"status": "queued", "message": "Order added to queue"}), 200
        elif action == 'removeFulfilledSKU':
            return remove_fulfilled_sku(data)
        else:
            logger.error(f"Invalid action for order {order_number}: {action}")
            return jsonify({"error": "No valid action or backup note provided"}), 400
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {fixed_body}. Error: {str(e)}")
        return jsonify({"error": "Invalid JSON payload"}), 400
    except Exception as e:
        logger.error(f"Unexpected error processing webhook request: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/process', methods=['GET'])
def process_endpoint():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        logger.error(f"Access denied: Invalid key - {provided_key}")
        return jsonify({"error": "Access Denied"}), 403
    process_queue()
    queue = load_queue()
    logger.info(f"Process endpoint completed. Queue size: {len(queue)}")
    return jsonify({"status": "processed", "queue_size": len(queue)}), 200

@app.route('/queue', methods=['GET'])
def view_queue():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        logger.error(f"Access denied: Invalid key - {provided_key}")
        return jsonify({"error": "Access Denied"}), 403
    queue = load_queue()
    logger.info(f"Queue accessed. Size: {len(queue)}")
    return jsonify({"queue_size": len(queue), "orders": queue}), 200

@app.route('/reset-queue', methods=['GET'])
def reset_queue():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        logger.error(f"Access denied: Invalid key - {provided_key}")
        return jsonify({"error": "Access Denied"}), 403
    save_queue([])
    logger.info("Queue reset to empty")
    return jsonify({"status": "reset", "queue_size": 0}), 200

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

    return jsonify({"status": "success", "message": "Data with backup shipping note added successfully"}), 200

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
        row_str = ','.join(map(str, row))
        if row_str in unique_rows:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:K{i+1}')
        else:
            unique_rows[row_str] = True

    for row_range in rows_to_clear:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=row_range
        ).execute()

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)