
# Orders 3.2 — Ordering Necessities

This project streamlines order management by integrating Shopify with Google Sheets using Render. When an order is received, it is automatically processed and populated into a structured Google Sheet for the operations department to manage efficiently.

---

## 📌 Description

**Orders 3.2** automates the flow of incoming orders from Shopify. When a new order is detected:
1. Render processes the data.
2. The order is inserted into a Google Sheet with 15 predefined columns.
3. Additional logic is applied via formulas and automation to assist the OD (Order Department) with prioritizing and handling orders.

### Google Sheet Columns
The sheet includes the following columns:
```
Date | Order Number | URL | SKU | Brand | Country | Assign Type | Date Due | PIC | Status | PO Number | What's happening | Warning to Check! | Note | Supplier
```

- Render fills: `Date`, `Order Number`, `URL`, `SKU`, `Brand`, `Country`
- `Assign Type` and `PIC` are populated using XLOOKUP from the `assign_types` sheet.
- `Status` is manually filled by the OD team (with automation help).
- Additional alerts and notes are populated in the “What’s happening” column.

---

## ✅ Latest Updates

**5/5/2025**
- Added "Please Check VIN" in Column L for orders that contain a VIN number.
- Column J (`Status`) is automatically set to **"TBC (No)"** for orders over **$500** with specific tags.

---

## ℹ️ Notes on the `Status` Column (Column J)

You may notice the dropdown arrow is missing in **Orders 3.2** (unlike Orders 3.1). Here's why:

- Previously, we used **Google Apps Script** to manage dropdowns.
- Now, data is injected programmatically through **Render**, which bypasses the Google Sheets UI for dropdowns.
- The dropdown options **are still available**! Just **double-click** a cell in Column J to view and select them.

We haven’t lost functionality—just the visual indicator changed. :)

---

## ❓ What To Do If Orders Aren’t Coming Through

1. **Check the Render Queue:**  
   Orders might be queued up for processing.  
   View the queue here:  [Order Queue](https://automated-orders-3-1.onrender.com/queue?key=abc123)

2. **Re-run the order in Shopify Flow:**  
   Look for the flow: `OD Tracking [3.2]`.

3. **Ensure There Are Blank Rows:**  
   While Render can create new rows, it’s best to leave extra blank rows at the bottom of the sheet to prevent issues.

---

## 🧠 Automation Details

- If an order has a VIN number, `"Please Check VIN"` will be shown in **Column L** to notify OD staff.
- Orders using backup shipping rates will include the following in the same column:  
  `" [Automated]: Backup shipping rate applied. Please manually check on the rates before processing this order."`
- If an order is priced above **$500**, the **Status** (Column J) will be set to **"TBC (No)"** automatically.

---

## 📦 Requirements

- **Shopify Flow** (Trigger for new orders)
- **Render** (For backend processing and sheet injection)
- **Google Sheets** (Orders 3.2 Sheet + assign_types reference sheet)
- **XLOOKUP** (for Assign Type and PIC matching)

---

## 🗨️ Need Help?

Feel free to reach out if you have any questions or run into issues!
