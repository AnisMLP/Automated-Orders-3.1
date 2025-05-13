# 10 May 2025: code added seperate by vendors. TB NO for oders with tag an above 500

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
from googleapiclient.errors import HttpError

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1maWDz6_g-9qOgTPwFvZsAmUPlO-d3lP4J6U4JFUgkRE'
SHEET_NAME = 'Orders 3.2'
SECRET_KEY = os.getenv('SECRET_KEY', 'abc123')
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

# File to store queued orders and failed orders
QUEUE_FILE = '/tmp/order_queue.json' if IS_RENDER else 'order_queue.json'
FAILED_ORDERS_FILE = '/tmp/failed_orders.json' if IS_RENDER else 'failed_orders.json'

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
    """Format date string to YYYY-MM-DD."""
    try:
        date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{date.year}-{str(date.month).zfill(2)}-{str(date.day).zfill(2)}"
    except Exception as e:
        logger.error(f"Error formatting date {date_str}: {str(e)}")
        return "Invalid Date"

def group_skus_by_vendor(line_items):
    """Group SKUs by vendor from line items."""
    sku_by_vendor = {}
    has_vin_by_vendor = {}  # Track if any item for the vendor has a VIN
    for item in line_items:
        sku, vendor, vin = item.get('sku', 'Unknown SKU'), item.get('vendor', 'Unknown Vendor'), item.get('vin', '')
        if vendor not in sku_by_vendor:
            sku_by_vendor[vendor] = [sku]
            has_vin_by_vendor[vendor] = bool(vin)
        else:
            sku_by_vendor[vendor].append(sku)
            if vin:
                has_vin_by_vendor[vendor] = True
    return sku_by_vendor, has_vin_by_vendor

def get_sheet_id():
    """Get the sheetId for the specified SHEET_NAME."""
    if not service:
        logger.error("Google Sheets service not initialized")
        return None
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet.get('properties', {}).get('title') == SHEET_NAME:
                return sheet.get('properties', {}).get('sheetId')
        logger.error(f"Sheet {SHEET_NAME} not found in spreadsheet")
        return None
    except Exception as e:
        logger.error(f"Error getting sheet ID: {str(e)}")
        return None

def get_last_row():
    """Get the last row in the Sheet, ensuring new rows are appended."""
    if not service:
        logger.error("Google Sheets service not initialized")
        return 2  # Append at row 2 to preserve header
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        logger.info(f"get_last_row: Retrieved {len(values)} rows from {SHEET_NAME}!A:N")
        if not values:
            logger.info("Sheet is empty, appending at row 2 to preserve header")
            return 2  # Append at row 2
        return len(values) + 1  # Append after last row
    except HttpError as e:
        logger.error(f"HttpError getting last row: {str(e)}")
        return 2  # Append at row 2 on error
    except Exception as e:
        logger.error(f"Unexpected error getting last row: {str(e)}")
        return 2  # Append at row 2 on error

def update_sheet_with_retry(range_to_write, body, max_attempts=3, valueInputOption='RAW'):
    """Update Google Sheets with retry logic."""
    for attempt in range(max_attempts):
        try:
            return service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=range_to_write,
                valueInputOption=valueInputOption, body=body
            ).execute()
        except HttpError as e:
            logger.error(f"Attempt {attempt + 1} failed for range {range_to_write}: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
        except Exception as e:
            logger.error(f"Unexpected error updating sheet for range {range_to_write}: {str(e)}")
            raise

def process_order(data):
    """Process a single order and write to Google Sheets."""
    if not service:
        logger.error("Cannot process order: Google Sheets service not initialized")
        return False
    try:
        order_number = data.get("order_number", "Unknown")
        logger.info(f"Processing order {order_number} with data: {json.dumps(data, indent=2)}")
        
        # Check for duplicate order number in column B
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!B:B'
        ).execute()
        order_numbers = result.get('values', [])
        order_numbers = [row[0] for row in order_numbers if row and row[0]]
        
        if str(order_number) in order_numbers:
            logger.warning(f"Duplicate order number {order_number} found in column B, skipping processing")
            return True

        order_id = data.get("order_id", "").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
        order_country = data.get("order_country", "Unknown")
        order_created = format_date(data.get("order_created", ""))
        line_items = data.get("line_items", [])
        order_total = float(data.get("order_total", "0")) if data.get("order_total") else 0.0
        
        # Handle tags as string or list
        tags = data.get("tags", "")
        if isinstance(tags, str):
            tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        elif isinstance(tags, list):
            tags_list = [tag.strip() for tag in tags if isinstance(tag, str) and tag.strip()]
        else:
            tags_list = []
        logger.info(f"Parsed tags for order {order_number}: {tags_list}")
        has_vin_tag = any(tag == "Call for VIN Alert Sent" or tag == "VIN Request Email Sent" for tag in tags_list)
        status = "TBC (No)" if order_total > 500 and has_vin_tag else ""

        if not line_items:
            logger.warning(f"Order {order_number} has no line items, skipping Google Sheets write")
            return True

        sku_by_vendor, has_vin_by_vendor = group_skus_by_vendor(line_items)
        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", status, "", "Please Check VIN" if has_vin_by_vendor[vendor] else "", "", ""]
            for vendor, skus in sku_by_vendor.items()
        ]

        start_row = max(2, get_last_row())  # Ensure we append and don't overwrite header
        range_to_write = f'{SHEET_NAME}!A{start_row}'
        body = {'values': rows_data}
        logger.info(f"Writing order {order_number} to Google Sheets at {range_to_write}")
        update_sheet_with_retry(range_to_write, body)

        apply_formulas()
        delete_rows()
        delete_duplicate_rows()

        logger.info(f"Successfully processed order {order_number} to Google Sheets")
        return True
    except Exception as e:
        logger.error(f"Error processing order {order_number}: {str(e)}")
        return False

def process_queue():
    """Process all valid orders in the queue, moving failed orders to failed_orders.json."""
    queue = load_queue()
    if not queue:
        logger.info("Queue is empty, nothing to process")
        return

    updated_queue = []
    max_retries = 3
    for order in queue:
        order_number = order.get("order_number", "Unknown")
        logger.info(f"Inspecting queued order {order_number}")

        if "error" in order:
            logger.info(f"Order {order_number} has an error, keeping in queue: {order['error']}")
            updated_queue.append(order)
            continue

        retries = order.get("retries", 0)
        if retries >= max_retries:
            logger.error(f"Order {order_number} exceeded {max_retries} retries, moving to failed orders")
            try:
                with open(FAILED_ORDERS_FILE, 'a') as f:
                    json.dump(order, f)
                    f.write('\n')
            except Exception as e:
                logger.error(f"Error saving to failed orders: {str(e)}")
            continue

        order["retries"] = retries + 1
        logger.info(f"Attempting to process valid order {order_number}, retry {retries + 1}/{max_retries}")
        if process_order(order):
            logger.info(f"Order {order_number} processed successfully, removing from queue")
        else:
            logger.warning(f"Order {order_number} failed processing, keeping in queue")
            updated_queue.append(order)
        time.sleep(1)  # Add delay to avoid Google API quota limits

    save_queue(updated_queue)
    logger.info(f"Queue processing complete. New queue size: {len(updated_queue)}")
    time.sleep(2)

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Handle incoming webhook requests."""
    if not service:
        return jsonify({"error": "Google Sheets API not initialized"}), 500

    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403

    logger.info(f"Raw request data: {request.data.decode('utf-8')}")
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
    """View the current queue."""
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403
    queue = load_queue()
    logger.info(f"Queue accessed. Size: {len(queue)}")
    return jsonify({"queue_size": len(queue), "orders": queue}), 200

@app.route('/failed_orders', methods=['GET'])
def view_failed_orders():
    """View orders that failed processing after max retries."""
    provided_key = request.args.get('key')
    if provided_key != SECRET_KEY:
        return jsonify({"error": "Access Denied"}), 403
    try:
        failed_orders = []
        if os.path.exists(FAILED_ORDERS_FILE):
            with open(FAILED_ORDERS_FILE, 'r') as f:
                for line in f:
                    if line.strip():
                        failed_orders.append(json.loads(line))
        logger.info(f"Failed orders accessed. Count: {len(failed_orders)}")
        return jsonify({"failed_orders_count": len(failed_orders), "failed_orders": failed_orders}), 200
    except Exception as e:
        logger.error(f"Error reading failed orders: {str(e)}")
        return jsonify({"error": str(e)}), 500

def add_backup_shipping_note(data):
    """Add order with backup shipping note to Google Sheets."""
    try:
        order_number = data.get("order_number")
        
        # Check for duplicate order number in column B
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!B:B'
        ).execute()
        order_numbers = result.get('values', [])
        order_numbers = [row[0] for row in order_numbers if row and row[0]]
        
        if str(order_number) in order_numbers:
            logger.warning(f"Duplicate order number {order_number} found in column B, skipping processing")
            return jsonify({"status": "skipped", "message": f"Order {order_number} is a duplicate"}), 200

        order_id = data.get("order_id").replace("gid://shopify/Order/", "https://admin.shopify.com/store/mlperformance/orders/")
        order_country = data.get("order_country")
        backup_note = data.get("backup_shipping_note")
        order_created = format_date(data.get("order_created"))
        line_items = data.get("line_items", [])
        order_total = float(data.get("order_total", "0")) if data.get("order_total") else 0.0
        
        # Handle tags as string or list
        tags = data.get("tags", "")
        if isinstance(tags, str):
            tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        elif isinstance(tags, list):
            tags_list = [tag.strip() for tag in tags if isinstance(tag, str) and tag.strip()]
        else:
            tags_list = []
        logger.info(f"Parsed tags for order {order_number}: {tags_list}")
        has_vin_tag = any(tag == "Call for VIN Alert Sent" or tag == "VIN Request Email Sent" for tag in tags_list)
        status = "TBC (No)" if order_total > 500 and has_vin_tag else ""

        sku_by_vendor, has_vin_by_vendor = group_skus_by_vendor(line_items)
        rows_data = [
            [order_created, order_number, order_id, ', '.join(skus), vendor, order_country, "", "", "", status, "", "Please Check VIN" if has_vin_by_vendor[vendor] else "", backup_note, ""]
            for vendor, skus in sku_by_vendor.items()
        ]

        start_row = max(2, get_last_row())  # Ensure we append and don't overwrite header
        range_to_write = f'{SHEET_NAME}!A{start_row}:N{start_row + len(rows_data) - 1}'
        body = {'values': rows_data}
        logger.info(f"Writing order {order_number} to Google Sheets at {range_to_write}")
        update_sheet_with_retry(range_to_write, body)

        apply_formulas()
        delete_rows()
        delete_duplicate_rows()

        return jsonify({"status": "success", "message": "Data with backup shipping note added successfully"}), 200
    except Exception as e:
        logger.error(f"Error in add_backup_shipping_note for order {order_number}: {str(e)}")
        return jsonify({"status": "error", "message": f"Processing failed: {str(e)}"}), 500

def remove_fulfilled_sku(data):
    """Remove fulfilled SKUs from the Sheet."""
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

def apply_formulas():
    """Apply formulas to Assign Type (G), PIC (I), and Supplier (H) columns."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        last_row = len(values) + 1 if values else 2

        assign_type_formulas = []
        pic_formulas = []
        supplier_formulas = []
        for row in range(2, last_row + 1):
            assign_type_formula = (
                f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!D:D,assign_types!E:E),'
                f'XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),XLOOKUP(E{row},assign_types!A:A,assign_types!B:B)),"")'
            )
            pic_formula = (
                f'=IFNA(IF(F{row}="US",IFNA(XLOOKUP(E{row},assign_types!E:E,assign_types!F:F),'
                f'XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),XLOOKUP(E{row},assign_types!A:A,assign_types!C:C)),"")'
            )
            supplier_formula = (
                f'=IFNA(XLOOKUP(E{row},\'[Auto] Supplier\'!A:A,\'[Auto] Supplier\'!B:B),"")'
            )
            assign_type_formulas.append([assign_type_formula])
            pic_formulas.append([pic_formula])
            supplier_formulas.append([supplier_formula])

        if assign_type_formulas:
            update_sheet_with_retry(f'{SHEET_NAME}!G2:G{last_row}', {'values': assign_type_formulas}, valueInputOption='USER_ENTERED')

        if pic_formulas:
            update_sheet_with_retry(f'{SHEET_NAME}!I2:I{last_row}', {'values': pic_formulas}, valueInputOption='USER_ENTERED')

        if supplier_formulas:
            update_sheet_with_retry(f'{SHEET_NAME}!H2:H{last_row}', {'values': supplier_formulas}, valueInputOption='USER_ENTERED')

    except Exception as e:
        logger.error(f"Error applying formulas: {str(e)}")
        raise

def delete_rows():
    """Delete rows with specific SKUs."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        rows_to_delete = []

        for i, row in enumerate(values):
            sku_cell = row[3] if len(row) > 3 else ''
            if sku_cell in ['Tip', 'MLP-AIR-FRESHENER', '']:
                rows_to_delete.append(i)

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            sheet_id = get_sheet_id()
            if sheet_id is None:
                logger.error("Cannot delete rows: Sheet ID not found")
                raise Exception("Cannot delete rows: Sheet ID not found")
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": i,
                            "endIndex": i + 1
                        }
                    }
                }
                for i in rows_to_delete
            ]
            for attempt in range(3):
                try:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
                    ).execute()
                    logger.info(f"Deleted rows: {rows_to_delete}")
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error deleting rows, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error deleting rows: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error deleting rows: {str(e)}")
                    raise
            else:
                logger.error("Failed to delete rows after 3 attempts")
                raise Exception("Failed to delete rows")
    except Exception as e:
        logger.error(f"Error in delete_rows: {str(e)}")
        raise

def delete_duplicate_rows():
    """Delete duplicate rows in the Sheet."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A:N'
        ).execute()
        values = result.get('values', [])
        unique_rows = {}
        rows_to_delete = []

        for i, row in enumerate(values):
            row_str = ','.join(map(str, row))
            if row_str in unique_rows:
                rows_to_delete.append(i)
            else:
                unique_rows[row_str] = True

        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            sheet_id = get_sheet_id()
            if sheet_id is None:
                logger.error("Cannot delete rows: Sheet ID not found")
                raise Exception("Cannot delete rows: Sheet ID not found")
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": i,
                            "endIndex": i + 1
                        }
                    }
                }
                for i in rows_to_delete
            ]
            for attempt in range(3):
                try:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
                    ).execute()
                    logger.info(f"Deleted duplicate rows: {rows_to_delete}")
                    break
                except HttpError as e:
                    if e.resp.status in [429, 503]:
                        logger.warning(f"Rate limit or service error deleting rows, attempt {attempt+1}: {str(e)}")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"Error deleting duplicate rows: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error deleting duplicate rows: {str(e)}")
                    raise
            else:
                logger.error("Failed to delete duplicate rows after 3 attempts")
                raise Exception("Failed to delete duplicate rows")
    except Exception as e:
        logger.error(f"Error in delete_duplicate_rows: {str(e)}")
        raise

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)