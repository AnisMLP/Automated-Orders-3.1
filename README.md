This is for Orders 3.2 Ordering Necessiies.

Latest Updates:
5/5/2025:
- Added "Please Check VIN" in Column L
- Column J Status set to "TBC (No)" for orders above $500

Quick note about the 'Status' column (Column J) in Orders 3.2:
- You might notice the dropdown arrow is missing, unlike in Orders 3.1. This is because we used Google Apps Script before, but now we're using code connected to Render.
- Render fills in the data programmatically through code, but it doesn’t use Google Sheets’ built-in dropdown UI, so the arrow doesn’t appear.
- The dropdown options are still there! Just double-click a cell in Column J to see and select the options. We haven’t lost functionality—just the visual indicator changed.

What to do if Order's arent coming through?
1. If order number is retrying on shopify flow, wait for a few minutes. There might be a queue on render that's processing other orders.
   You can view the Orders queue here: https://automated-orders-3-1.onrender.com/queue?key=abc123
2. Re-Run the order in shopify flow: OD Tracking [3.2]
3. Make sure that there's extra blank rows at the bottom. Sure the code might make a new row, but this is just in case it cant detect any blank rows.

Any further questions, feel free to ask :)
