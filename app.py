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

# File paths
QUEUE_FILE = '/tmp/order_queue.json' if IS_RENDER else 'order_queue.json'
QUEUE_LOCK_FILE = '/tmp/queue.lock' if IS_RENDER else 'queue.lock'
SHEET_LOCK_FILE = '/tmp/sheet.lock' if IS_RENDER else 'sheet.lock'

def acquire_lock(lock_file):
    """Acquire a file lock."""
    lock_fd = open(lock_file, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        logger.info(f"Acquired lock on {lock_file}")
        return lock_fd
    except Exception as e:
        logger.error(f"Failed to acquire lock on {lock_file}: {str(e)}")
        lock_fd.close()
        raise

def release_lock(lock_fd, lock_file):
    """Release a file lock."""
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        logger.info(f"Released lock on {lock_file}")
        lock_fd.close()
    except Exception as e:
        logger.error(f"Failed to release lock on {lock_file}: {str(e)}")

def load_queue():
    """Load the queue from file with locking."""
    lock_fd = acquire_lock(QUEUE_LOCK_FILE)
    try:
        if os.path.exists(QUEUE_FILE):
            with open(QUEUE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error loading queue: {str(e)}")
        return []
    finally:
        release_lock(lock_fd, QUEUE_LOCK_FILE)

def save_queue(queue):
    """Save the queue to file with locking."""
    lock_fd = acquire_lock(QUEUE_LOCK_FILE)
    try:
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f)
    except Exception as e:
        logger.error(f"Error saving queue: {str(e)}")
    finally:
        release_lock(lock_fd, QUEUE_LOCK_FILE)

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
                sheet_id = sheet['properties']['sheetId']
                logger.info(f"Retrieved sheet ID {sheet_id} for sheet {SHEET_NAME}")
                return sheet_id
        raise ValueError(f"Sheet {SHEET_NAME} not found")
    except Exception as e:
        logger.error(f"Error getting sheet ID: {str(e)}")
        raise

def process_order(data):
    """Process a single order and write to Google Sheets with duplicate checking."""
    if not service:
        logger.error("Cannot process order: Google Sheets service not initialized")
        return False
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
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
        for attempt in range(3):
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
                ).execute()
                values = result.get('values', [])
                break
            except HttpError as e:
                if e.resp.status in [429, 503]:
                    logger.warning(f"Rate limit or service error reading sheet, attempt {attempt+1}: {str(e)}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Error reading sheet for duplicate check: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error reading sheet for duplicate check: {str(e)}")
                raise
        else:
            logger.error(f"Failed to read sheet for duplicate check after 3 attempts")
            return False

        for row in values:
            if len(row) > 4 and row[1].lstrip("#") == order_number.lstrip("#"):
                vendor = row[4].strip().lower() if len(row) > 4 else ""
                if vendor in [v.strip().lower() for v in sku_by_vendor.keys()]:
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
        for attempt in range(3):
            try:
                result = service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=range_to_write,
                    valueInputOption='RAW', body=body
                ).execute()
                break
            except HttpError as e:
                if e.resp.status in [429, 503]:
                    logger.warning(f"Rate limit or service error writing sheet, attempt {attempt+1}: {str(e)}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Error writing order {order_number} to sheet: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error writing order {order_number} to sheet: {str(e)}")
                raise
        else:
            logger.error(f"Failed to write order {order_number} to sheet after 3 attempts")
            return False

        logger.info(f"Applying formulas for order {order_number}")
        apply_formulas()
        logger.info(f"Deleting rows for order {order_number}")
        delete_rows()
        logger.info(f"Deleting duplicates for order {order_number}")
        delete_duplicate_rows()

        # Log sheet state
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
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

def process_queue():
    """Process all valid orders in the queue, with a limit to avoid timeouts."""
    queue = load_queue()
    if not queue:
        logger.info("Queue is empty, nothing to process")
        return

    updated_queue = []
    max_orders = 5  # Limit to avoid Render timeout
    processed = 0

    for order in queue:
        if processed >= max_orders:
            updated_queue.append(order)
            continue
        order_number = order.get("order_number", "Unknown")
        logger.info(f"Inspecting queued order {order_number}")

        if "error" in order:
            logger.info(f"Order {order_number} has an error, keeping in queue: {order['error']}")
            updated_queue.append(order)
            continue

        logger.info(f"Attempting to process valid order {order_number}")
        if process_order(order):
            logger.info(f"Order {order_number} processed successfully, removing from queue")
            processed += 1
        else:
            logger.warning(f"Order {order_number} failed processing, keeping in queue")
            updated_queue.append(order)

    save_queue(updated_queue)
    logger.info(f"Queue processing complete. Processed {processed} orders. New queue size: {len(updated_queue)}")

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
        # Check for duplicate in queue
        if any(order.get("order_number") == order_number for order in queue):
            logger.info(f"Duplicate order {order_number} detected in queue, skipping")
            return jsonify({"status": "success", "message": "Duplicate order ignored"}), 200

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

@app.route('/clear_queue', methods=['POST'])
def clear_queue():
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403
    save_queue([])
    logger.info("Queue cleared")
    return jsonify({"status": "success", "message": "Queue cleared"}), 200

def add_backup_shipping_note(data):
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
    try:
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

        # Check for existing order
        for attempt in range(3):
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
                ).execute()
                values = result.get('values', [])
                break
            except HttpError as e:
                if e.resp.status in [429, 503]:
                    logger.warning(f"Rate limit or service error reading sheet, attempt {attempt+1}: {str(e)}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Error reading sheet for duplicate check: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error reading sheet for duplicate check: {str(e)}")
                raise
        else:
            logger.error(f"Failed to read sheet for duplicate check after 3 attempts")
            return jsonify({"status": "error", "message": "Failed to read sheet"}), 500

        for row in values:
            if len(row) > 4 and row[1].lstrip("#") == order_number.lstrip("#"):
                vendor = row[4].strip().lower() if len(row) > 4 else ""
                if vendor in [v.strip().lower() for v in sku_by_vendor.keys()]:
                    logger.info(f"Order {order_number} with vendor {vendor} already exists, skipping backup shipping note")
                    return jsonify({"status": "success", "message": "Order already exists, skipped"}), 200

        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", "", "", "", backup_note, ""]
            for vendor, skus in sku_by_vendor.items()
        ]

        start_row = get_last_row()
        range_to_write = f'{SHEET_NAME}!A{start_row}:N{start_row + len(rows_data) - 1}'
        body = {'values': rows_data}
        for attempt in range(3):
            try:
                result = service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=range_to_write,
                    valueInputOption='RAW', body=body
                ).execute()
                break
            except HttpError as e:
                if e.resp.status in [429, 503]:
                    logger.warning(f"Rate limit or service error writing sheet, attempt {attempt+1}: {str(e)}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Error writing backup note for order {order_number}: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error writing backup note for order {order_number}: {str(e)}")
                raise
        else:
            logger.error(f"Failed to write backup note for order {order_number} after 3 attempts")
            return jsonify({"status": "error", "message": "Failed to write backup note"}), 500

        apply_formulas()
        delete_rows()
        delete_duplicate_rows()

        return jsonify({"status": "success", "message": "Data with backup shipping note added successfully"}), 200
    except Exception as e:
        logger.error(f"Error adding backup shipping note for order {data.get('order_number', 'Unknown')}: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to add backup note: {str(e)}"}), 500
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

def remove_fulfilled_sku(data):
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
    try:
        order_number = data.get("order_number", "Unknown").lstrip("#")
        line_items = data.get("line_items", [])
        logger.info(f"Processing remove_fulfilled_sku for order {order_number}, line_items: {line_items}")

        if not service:
            logger.error("Google Sheets service not initialized")
            return jsonify({"status": "error", "message": "Google Sheets API not initialized"}), 500

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
            for attempt in range(3):
                try:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=SPREADSHEET_ID,
                        body=request_body
                    ).execute()
                    logger.info(f"Deleted rows: {rows_to_delete}")
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error deleting rows, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error deleting rows for order {order_number}: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error deleting rows for order {order_number}: {str(e)}")
                    raise
            else:
                logger.error(f"Failed to delete rows for order {order_number} after 3 attempts")
                return jsonify({"status": "error", "message": "Failed to delete rows"}), 500

        for i, row in rows_to_update:
            for attempt in range(3):
                try:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A{i+1}:N{i+1}',
                        valueInputOption='RAW', body={'values': [row]}
                    ).execute()
                    logger.info(f"Updated row {i+1}")
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error updating row, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error updating row {i+1}: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error updating row {i+1}: {str(e)}")
                    raise
            else:
                logger.error(f"Failed to update row {i+1} after 3 attempts")
                return jsonify({"status": "error", "message": "Failed to update row"}), 500

        if not rows_to_delete and not rows_to_update:
            logger.warning(f"No rows found for order {order_number}")
            return jsonify({"status": "success", "message": "No matching rows found"}), 200

        return jsonify({"status": "success", "message": "Fulfilled SKUs removed or rows deleted"}), 200
    except Exception as e:
        logger.error(f"Error in remove_fulfilled_sku for order {order_number}: {str(e)}")
        return jsonify({"status": "error", "message": f"Processing failed: {str(e)}"}), 500
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

def apply_formulas():
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        last_row = len(values) + 1 if values else 2

        assign_type_formulas = []
        pic_formulas = []
        for row in range(2, last_row + 1):
            assign_type_formula = (
                f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!D:D,assign_types!E:E),'
                f'XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),"")'
            )
            pic_formula = (
                f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!E:E,assign_types!F:F),'
                f'XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),"")'
            )
            assign_type_formulas.append([assign_type_formula])
            pic_formulas.append([pic_formula])

        if assign_type_formulas:
            for attempt in range(3):
                try:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!G2:G{last_row}',
                        valueInputOption='USER_ENTERED', body={'values': assign_type_formulas}
                    ).execute()
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error applying assign type formulas, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error applying assign type formulas: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error applying assign type formulas: {str(e)}")
                    raise
            else:
                logger.error(f"Failed to apply assign type formulas after 3 attempts")
                raise Exception("Failed to apply assign type formulas")

        if pic_formulas:
            for attempt in range(3):
                try:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!I2:I{last_row}',
                        valueInputOption='USER_ENTERED', body={'values': pic_formulas}
                    ).execute()
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error applying pic formulas, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error applying pic formulas: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error applying pic formulas: {str(e)}")
                    raise
            else:
                logger.error(f"Failed to apply pic formulas after 3 attempts")
                raise Exception("Failed to apply pic formulas")
    except Exception as e:
        logger.error(f"Error in apply_formulas: {str(e)}")
        raise
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

def delete_rows():
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
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
            if not row or all(cell.strip() == "" for cell in row):
                rows_to_delete.append(i)
                logger.info(f"Marked empty row {i+1} for deletion")
                continue
            sku_cell = row[3] if len(row) > 3 else ""
            if sku_cell in ["Tip", "MLP-AIR-FRESHENER", ""]:
                rows_to_delete.append(i)
                logger.info(f"Marked row {i+1} with SKU {sku_cell} for deletion")

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            batch_size = 50
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
                for attempt in range(3):
                    try:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=SPREADSHEET_ID,
                            body=request_body
                        ).execute()
                        logger.info(f"Deleted batch of {len(batch)} rows: {batch}")
                        break
                    except HttpError as e:
                        if e.resp.status in [429, 503]:
                            logger.warning(f"Rate limit or service error deleting rows, attempt {attempt+1}: {str(e)}")
                            time.sleep(2 ** attempt)
                        else:
                            logger.error(f"Error deleting batch of rows: {str(e)}")
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error deleting batch of rows: {str(e)}")
                        raise
                else:
                    logger.error(f"Failed to delete batch of rows after 3 attempts: {batch}")
                    raise Exception("Failed to delete rows")
        else:
            logger.info("No rows to delete")
    except Exception as e:
        logger.error(f"Error in delete_rows: {str(e)}")
        raise
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

def delete_duplicate_rows():
    lock_fd = acquire_lock(SHEET_LOCK_FILE)
    try:
        logger.info("Starting duplicate row deletion")
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.info("No rows to process for duplicates")
            return

        seen = {}
        rows_to_delete = []
        for i, row in enumerate(values):
            if len(row) < 5:
                continue
            order_number = row[1].lstrip("#") if len(row) > 1 else ""
            vendor = row[4].strip().lower() if len(row) > 4 else ""
            skus = row[3] if len(row) > 3 else ""
            key = (order_number, vendor, skus)

            if key in seen:
                rows_to_delete.append(i)
                logger.info(f"Found duplicate row {i+1} for order {order_number}, vendor {vendor}, SKUs {skus}")
            else:
                seen[key] = i

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            batch_size = 50
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
                for attempt in range(3):
                    try:
                        service.spreadsheets().batchUpdate(
                            spreadsheetId=SPREADSHEET_ID,
                            body=request_body
                        ).execute()
                        logger.info(f"Deleted batch of {len(batch)} duplicate rows: {batch}")
                        break
                    except HttpError as e:
                        if e.resp.status in [429, 503]:
                            logger.warning(f"Rate limit or service error deleting duplicates, attempt {attempt+1}: {str(e)}")
                            time.sleep(2 ** attempt)
                        else:
                            logger.error(f"Error deleting batch of duplicate rows: {str(e)}")
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error deleting batch of duplicate rows: {str(e)}")
                        raise
                else:
                    logger.error(f"Failed to delete batch of duplicate rows after 3 attempts: {batch}")
                    raise Exception("Failed to delete duplicate rows")
        else:
            logger.info("No duplicate rows found")
    except Exception as e:
        logger.error(f"Error in delete_duplicate_rows: {str(e)}")
        raise
    finally:
        release_lock(lock_fd, SHEET_LOCK_FILE)

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)