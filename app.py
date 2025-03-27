from flask import Flask, request, jsonify
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv
import time
import re

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1maWDz6_g-9qOgTPwFvZsAmUPlO-d3lP4J6U4JFUgkRE'
SHEET_NAME = 'Orders 3.2'
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

# File to store queued orders
QUEUE_FILE = '/tmp/order_queue.json' if IS_RENDER else 'order_queue.json'

def load_queue():
    """Load the queue from file."""
    try:
        if os.path.exists(QUEUE_FILE):
            with open(QUEUE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error loading queue: {str(e)}")
        return []

def save_queue(queue):
    """Save the queue to file."""
    try:
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f)
    except Exception as e:
        logger.error(f"Error saving queue: {str(e)}")

def clean_json(raw_data):
    """Clean up common JSON syntax errors like trailing commas and empty lines."""
    try:
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
        lines = [line.strip() for line in raw_data.splitlines() if line.strip()]
        cleaned = '\n'.join(lines)
        cleaned = re.sub(r',(\s*[\]}])', r'\1', cleaned)
        return cleaned
    except Exception as e:
        logger.error(f"Error cleaning JSON: {str(e)}")
        return raw_data

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
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:M'
        ).execute()
        values = result.get('values', [])
        return len(values) + 1 if values else 1
    except Exception as e:
        logger.error(f"Error getting last row: {str(e)}")
        return 1

def process_order(data):
    """Process a single order and write to Google Sheets with new column structure."""
    if not service:
        logger.error("Cannot process order: Google Sheets service not initialized")
        return False
    try:
        order_number = data.get("order_number", "Unknown")
        logger.info(f"Processing order {order_number}")
        order_id = data.get("order_id", "").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
        order_country = data.get("order_country", "Unknown")
        order_created = format_date(data.get("order_created", ""))
        line_items = data.get("line_items", [])

        if not line_items:
            logger.warning(f"Order {order_number} has no line items, skipping Google Sheets write")
            return True

        sku_by_vendor = group_skus_by_vendor(line_items)
        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", "", "", ""]
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

        apply_formulas()
        delete_rows()
        delete_duplicate_rows()

        logger.info(f"Successfully processed order {order_number} to Google Sheets")
        return True
    except Exception as e:
        logger.error(f"Error processing order {data.get('order_number', 'Unknown')}: {str(e)}")
        return False

def process_queue():
    """Process all valid orders in the queue, skipping error entries."""
    queue = load_queue()
    if not queue:
        logger.info("Queue is empty, nothing to process")
        return

    updated_queue = []
    for order in queue:
        order_number = order.get("order_number", "Unknown")
        logger.info(f"Inspecting queued order {order_number}")

        if "error" in order:
            logger.info(f"Order {order_number} has an error, keeping in queue: {order['error']}")
            updated_queue.append(order)
            continue

        logger.info(f"Attempting to process valid order {order_number}")
        if process_order(order):
            logger.info(f"Order {order_number} processed successfully, removing from queue")
        else:
            logger.warning(f"Order {order_number} failed processing, keeping in queue for inspection")
            updated_queue.append(order)

    save_queue(updated_queue)
    logger.info(f"Queue processing complete. New queue size: {len(updated_queue)}")
    time.sleep(2)

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    if not service:
        return jsonify({"error": "Google Sheets API not initialized"}), 500

    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403

    logger.info(f"Raw request data: {request.data}")
    logger.info(f"Request headers: {request.headers}")

    queue = load_queue()
    order_number = "Unknown"
    raw_data = request.data

    cleaned_data = clean_json(raw_data)
    try:
        data = json.loads(cleaned_data)
        if not data:
            logger.error("No valid JSON data after cleaning")
            queue.append({"error": "No valid JSON data after cleaning", "order_number": order_number, "raw_data": raw_data.decode('utf-8')})
            save_queue(queue)
            return jsonify({"status": "queued", "message": "Order queued with error: No valid JSON data"}), 200

        order_number = data.get("order_number", "Unknown")
        action = request.args.get('action', '')
        if data.get("backup_shipping_note"):
            return add_backup_shipping_note(data)
        elif action == 'addNewOrders':
            queue.append(data)
            save_queue(queue)
            logger.info(f"Order {order_number} added to queue. Queue size: {len(queue)}")
            process_queue()
            return jsonify({"status": "queued", "message": f"Order {order_number} added to queue"}), 200
        elif action == 'removeFulfilledSKU':
            return remove_fulfilled_sku(data)
        else:
            logger.error(f"Invalid action: {action}")
            queue.append({"error": f"Invalid action: {action}", "order_number": order_number, "raw_data": raw_data.decode('utf-8')})
            save_queue(queue)
            return jsonify({"status": "queued", "message": f"Order {order_number} queued with error: Invalid action"}), 200
    except ValueError as e:
        logger.error(f"Failed to parse JSON even after cleaning: {str(e)}")
        queue.append({"error": f"Invalid JSON: {str(e)}", "order_number": order_number, "raw_data": raw_data.decode('utf-8')})
        save_queue(queue)
        return jsonify({"status": "queued", "message": f"Order {order_number} queued with error: Invalid JSON"}), 200
    except Exception as e:
        logger.error(f"Error processing webhook request: {str(e)}")
        queue.append({"error": str(e), "order_number": order_number, "raw_data": raw_data.decode('utf-8')})
        save_queue(queue)
        return jsonify({"status": "queued", "message": f"Order {order_number} queued with error: {str(e)}"}), 200

@app.route('/queue', methods=['GET'])
def view_queue():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403
    queue = load_queue()
    logger.info(f"Queue accessed. Size: {len(queue)}")
    return jsonify({"queue_size": len(queue), "orders": queue}), 200

def add_backup_shipping_note(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    backup_note = data.get("backup_shipping_note")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    sku_by_vendor = group_skus_by_vendor(line_items)
    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", "", "", backup_note]
        for vendor, skus in sku_by_vendor.items()
    ]

    start_row = get_last_row()
    range_to_write = f'{SHEET_NAME}!A{start_row}:M{start_row + len(rows_data) - 1}'
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
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:M'
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
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:M{i+1}'
                ).execute()
            else:
                values[i][3] = ', '.join(skus)
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:M{i+1}',
                    valueInputOption='RAW', body={'values': [values[i]]}
                ).execute()
            break

    return jsonify({"status": "success", "message": "Fulfilled SKUs removed"}), 200

def apply_formulas():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:M'
    ).execute()
    values = result.get('values', [])
    last_row = len(values) + 1 if values else 2

    assign_type_formulas = []
    pic_formulas = []
    for row in range(2, last_row + 1):
        # Assign Type (G)
        assign_type_formula = (
            f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!D:D,assign_types!E:E),'
            f'XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),"")'
        )
        # PIC (I)
        pic_formula = (
            f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!E:E,assign_types!F:F),'
            f'XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),"")'
        )
        assign_type_formulas.append([assign_type_formula])
        pic_formulas.append([pic_formula])

    # Assign Type formulas to column G
    if assign_type_formulas:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!G2:G{last_row}',
            valueInputOption='USER_ENTERED', body={'values': assign_type_formulas}
        ).execute()

    # PIC formulas to column I
    if pic_formulas:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!I2:I{last_row}',
            valueInputOption='USER_ENTERED', body={'values': pic_formulas}
        ).execute()

def delete_rows():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:M'
    ).execute()
    values = result.get('values', [])
    rows_to_clear = []

    for i, row in enumerate(values):
        sku_cell = row[3] if len(row) > 3 else ''
        if sku_cell in ['Tip', 'MLP-AIR-FRESHENER', '']:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:M{i+1}')

    for row_range in rows_to_clear:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=row_range
        ).execute()

def delete_duplicate_rows():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:M'
    ).execute()
    values = result.get('values', [])
    unique_rows = {}
    rows_to_clear = []

    for i, row in enumerate(values):
        row_str = ','.join(map(str, row))
        if row_str in unique_rows:
            rows_to_clear.append(f'{SHEET_NAME}!A{i+1}:M{i+1}')
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