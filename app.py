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
import uuid
import fcntl
from googleapiclient.errors import HttpError

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
LOCK_FILE = '/tmp/queue.lock' if IS_RENDER else 'queue.lock'

def acquire_lock():
    """Acquire a file lock for queue operations."""
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return lock_fd
    except Exception as e:
        logger.error(f"Failed to acquire lock: {str(e)}")
        lock_fd.close()
        raise

def release_lock(lock_fd):
    """Release the file lock."""
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
    except Exception as e:
        logger.error(f"Failed to release lock: {str(e)}")

def load_queue():
    """Load the queue from file with locking."""
    lock_fd = acquire_lock()
    try:
        if os.path.exists(QUEUE_FILE):
            with open(QUEUE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error loading queue: {str(e)}")
        return []
    finally:
        release_lock(lock_fd)

def save_queue(queue):
    """Save the queue to file with locking."""
    lock_fd = acquire_lock()
    try:
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f)
    except Exception as e:
        logger.error(f"Error saving queue: {str(e)}")
    finally:
        release_lock(lock_fd)

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
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        return len(values) + 1 if values else 1
    except Exception as e:
        logger.error(f"Error getting last row: {str(e)}")
        return 1

def get_sheet_id():
    """Helper function to get the sheet ID for the SHEET_NAME."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == SHEET_NAME:
                return sheet['properties']['sheetId']
        raise ValueError(f"Sheet {SHEET_NAME} not found")
    except Exception as e:
        logger.error(f"Error getting sheet ID: {str(e)}")
        raise

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
        if not sku_by_vendor:
            logger.warning(f"Order {order_number} has no valid vendors, skipping")
            return True

        # Check for existing order in sheet to prevent duplicates
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        for row in values:
            if len(row) > 4 and row[1].lstrip("#") == order_number:
                vendor = row[4] if len(row) > 4 else ""
                if vendor in sku_by_vendor:
                    logger.info(f"Order {order_number} with vendor {vendor} already exists in sheet, skipping")
                    return True

        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", "", "", "", ""]
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

        # Log sheet state after deletions for debugging
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        logger.info(f"Sheet state after processing order {order_number}: {len(values)} rows")

        logger.info(f"Successfully processed order {order_number} to Google Sheets")
        return True
    except Exception as e:
        logger.error(f"Error processing order {data.get('order_number', 'Unknown')}: {str(e)}")
        return False

def process_queue():
    """Process one order from the queue to avoid timeouts."""
    queue = load_queue()
    if not queue:
        logger.info("Queue is empty, nothing to process")
        return

    order = queue.pop(0)  # Process only the first order
    order_number = order.get("order_number", "Unknown")
    logger.info(f"Inspecting queued order {order_number}")

    if "error" in order:
        logger.info(f"Order {order_number} has an error, keeping in queue: {order['error']}")
        queue.append(order)
        save_queue(queue)
        return

    logger.info(f"Attempting to process valid order {order_number}")
    if process_order(order):
        logger.info(f"Order {order_number} processed successfully, removed from queue")
    else:
        logger.warning(f"Order {order_number} failed processing, re-queueing")
        queue.append(order)
    save_queue(queue)
    logger.info(f"Queue processing complete. New queue size: {len(queue)}")

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
        # Check for duplicate order to prevent Shopify retries
        if any(order.get("order_number") == order_number for order in queue):
            logger.info(f"Duplicate order {order_number} detected, skipping")
            return jsonify({"status": "success", "message": "Duplicate order ignored"}), 200

        action = request.args.get('action', '')
        if data.get("backup_shipping_note"):
            return add_backup_shipping_note(data)
        elif action == 'addNewOrders':
            queue.append(data)
            save_queue(queue)
            logger.info(f"Order {order_number} added to queue. Queue size: {len(queue)}")
            process_queue()  # Process one order to avoid timeout
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

@app.route('/clear_queue', methods=['POST'])
def clear_queue():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403
    save_queue([])
    logger.info("Queue cleared")
    return jsonify({"status": "success", "message": "Queue cleared"}), 200

def add_backup_shipping_note(data):
    order_number = data.get("order_number")
    order_id = data.get("order_id").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
    order_country = data.get("order_country")
    backup_note = data.get("backup_shipping_note")
    order_created = format_date(data.get("order_created"))
    line_items = data.get("line_items", [])

    if not line_items:
        logger.warning(f"Order {order_number} has no line items, skipping backup shipping note")
        return jsonify({"status": "success", "message": "No line items, skipped"}), 200

    sku_by_vendor = group_skus_by_vendor(line_items)
    if not sku_by_vendor:
        logger.warning(f"Order {order_number} has no valid vendors, skipping backup shipping note")
        return jsonify({"status": "success", "message": "No valid vendors, skipped"}), 200

    # Check for existing order to prevent duplicates
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
    ).execute()
    values = result.get('values', [])
    for row in values:
        if len(row) > 4 and row[1].lstrip("#") == order_number:
            vendor = row[4] if len(row) > 4 else ""
            if vendor in sku_by_vendor:
                logger.info(f"Order {order_number} with vendor {vendor} already exists, skipping backup shipping note")
                return jsonify({"status": "success", "message": "Order already exists, skipped"}), 200

    rows_data = [
        [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", "", "", backup_note, ""]
        for vendor, skus in sku_by_vendor.items()
    ]

    start_row = get_last_row()
    range_to_write = f'{SHEET_NAME}!A{start_row}:N{start_row + len(rows_data) - 1}'
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
    order_number = data.get("order_number", "Unknown").lstrip("#")
    line_items = data.get("line_items", [])
    logger.info(f"Processing remove_fulfilled_sku for order {order_number}, line_items: {line_items}")

    if not service:
        logger.error("Google Sheets service not initialized")
        return jsonify({"status": "error", "message": "Google Sheets API not initialized"}), 500

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        logger.info(f"Retrieved {len(values)} rows from sheet")

        rows_to_delete = []
        rows_to_update = []
        for i, row in enumerate(values):
            if len(row) > 1 and row[1].lstrip("#") == order_number:
                logger.info(f"Found matching row {i+1} for order {order_number}")
                skus = row[3].split(', ') if len(row) > 3 and row[3] else []
                if not line_items:
                    logger.info(f"No line items provided, marking row {i+1} for deletion")
                    rows_to_delete.append(i)
                    continue
                for item in line_items:
                    sku = item.get('sku')
                    if sku and sku in skus:
                        skus.remove(sku)
                        logger.info(f"Removed SKU {sku} from row {i+1}")
                if not skus:
                    logger.info(f"No SKUs remain in row {i+1}, marking for deletion")
                    rows_to_delete.append(i)
                else:
                    row[3] = ', '.join(skus)
                    rows_to_update.append((i, row))
                    logger.info(f"Updated row {i+1} with SKUs: {row[3]}")

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            try:
                request_body = {
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": get_sheet_id(),
                                    "dimension": "ROWS",
                                    "startIndex": i,
                                    "endIndex": i + 1
                                }
                            }
                        }
                        for i in rows_to_delete
                    ]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body=request_body
                ).execute()
                logger.info(f"Deleted rows: {rows_to_delete}")
            except Exception as e:
                logger.error(f"Error deleting rows for order {order_number}: {str(e)}")
                return jsonify({"status": "error", "message": f"Failed to delete rows: {str(e)}"}), 500

        for i, row in rows_to_update:
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:N{i+1}',
                    valueInputOption='RAW', body={'values': [row]}
                ).execute()
                logger.info(f"Updated row {i+1}")
            except Exception as e:
                logger.error(f"Error updating row {i+1}: {str(e)}")
                return jsonify({"status": "error", "message": f"Failed to update row: {str(e)}"}), 500

        if not rows_to_delete and not rows_to_update:
            logger.warning(f"No rows found for order {order_number}")
            return jsonify({"status": "success", "message": "No matching rows found"}), 200

        return jsonify({"status": "success", "message": "Fulfilled SKUs removed or rows deleted"}), 200
    except Exception as e:
        logger.error(f"Error in remove_fulfilled_sku for order {order_number}: {str(e)}")
        return jsonify({"status": "error", "message": f"Processing failed: {str(e)}"}), 500

def apply_formulas():
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
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
    """Delete rows with specific SKUs or completely empty rows."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.info("No rows to process for deletion")
            return

        rows_to_delete = []
        for i, row in enumerate(values):
            # Check for completely empty rows or rows with only whitespace
            if not row or all(cell.strip() == "" for cell in row):
                rows_to_delete.append(i)
                logger.info(f"Marked empty row {i+1} for deletion")
                continue
            # Check for specific SKUs or empty SKU cell
            sku_cell = row[3] if len(row) > 3 else ""
            if sku_cell in ["Tip", "MLP-AIR-FRESHENER", ""]:
                rows_to_delete.append(i)
                logger.info(f"Marked row {i+1} with SKU {sku_cell} for deletion")

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)  # Delete from bottom to top
            request_body = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": get_sheet_id(),
                                "dimension": "ROWS",
                                "startIndex": i,
                                "endIndex": i + 1
                            }
                        }
                    }
                    for i in rows_to_delete
                ]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request_body
            ).execute()
            logger.info(f"Deleted {len(rows_to_delete)} rows (empty or specific SKUs)")
        else:
            logger.info("No rows to delete")
    except Exception as e:
        logger.error(f"Error deleting rows: {str(e)}")

def delete_duplicate_rows():
    """Delete duplicate rows based on order_number, vendor, and SKUs with batching and retries."""
    try:
        # Fetch all rows
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.info("No rows to process for duplicates")
            return

        # Track unique rows and rows to delete
        seen = {}  # Key: (order_number, vendor, skus), Value: row index
        rows_to_delete = []

        for i, row in enumerate(values):
            if len(row) < 5:  # Skip rows with insufficient data
                continue
            order_number = row[1].lstrip("#") if len(row) > 1 else ""
            vendor = row[4] if len(row) > 4 else ""
            skus = row[3] if len(row) > 3 else ""
            key = (order_number, vendor, skus)

            if key in seen:
                rows_to_delete.append(i)
                logger.info(f"Found duplicate row {i+1} for order {order_number}, vendor {vendor}, SKUs {skus}")
            else:
                seen[key] = i

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)  # Delete from bottom to top
            batch_size = 50  # Process deletions in batches to avoid API limits
            for i in range(0, len(rows_to_delete), batch_size):
                batch = rows_to_delete[i:i + batch_size]
                request_body = {
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": get_sheet_id(),
                                    "dimension": "ROWS",
                                    "startIndex": row_idx,
                                    "endIndex": row_idx + 1
                                }
                            }
                        }
                        for row_idx in batch
                    ]
                }
                for attempt in range(3):  # Retry up to 3 times
                    try:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=SPREADSHEET_ID,
                            body=request_body
                        ).execute()
                        logger.info(f"Deleted batch of {len(batch)} duplicate rows: {batch}")
                        break
                    except HttpError as e:
                        if e.resp.status in [429, 503]:  # Rate limit or service unavailable
                            logger.warning(f"Rate limit or service error on attempt {attempt+1}: {str(e)}")
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            logger.error(f"Error deleting batch of duplicate rows: {str(e)}")
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error deleting batch of duplicate rows: {str(e)}")
                        raise
                else:
                    logger.error(f"Failed to delete batch of duplicate rows after 3 attempts: {batch}")
        else:
            logger.info("No duplicate rows found")
    except Exception as e:
        logger.error(f"Error in delete_duplicate_rows: {str(e)}")

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)