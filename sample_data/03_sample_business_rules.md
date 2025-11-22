# Sample Business Rules for AutoETL Testing

## Overview
These are realistic business rules you can use to test the AutoETL system. Each rule describes a transformation from source tables (DB1) to target tables (DB2) in natural language.

---

## Rule 1: Customer Summary Aggregation

### Business Rule (Natural Language):
```
Calculate comprehensive customer summaries from the orders and order_items tables.

For each active customer (status = 'active' and is_deleted = false):
- Calculate total number of orders
- Calculate total amount spent across all orders
- Calculate average order value
- Find the date of their first order
- Find the date of their last order
- Count total items purchased
- Calculate customer lifetime value (sum of all order amounts)
- Determine preferred payment method (most frequently used)
- Count cancelled orders
- Assign customer segment based on total spent:
  * VIP: > $5000
  * Regular: $1000 - $5000
  * New: < $1000

Store results in customer_summary table with columns:
customer_id, full_name (first_name + last_name), email, total_orders, 
total_amount, average_order_value, last_order_date, first_order_date,
total_items_purchased, customer_lifetime_value, preferred_payment_method,
total_cancelled_orders, status, customer_segment

Only include customers who have placed at least one order.
Filter out customers marked as deleted (is_deleted = true).
```

### Expected Output:
- Table: `customer_summary`
- ~950 records (active customers with orders)
- All amounts calculated correctly
- Customer segments assigned

### Validation Rules:
```
Verify that customer summaries are accurate:
1. Sum of total_amount matches sum of order amounts from source
2. No NULL values in total_orders or total_amount
3. Average_order_value equals total_amount / total_orders
4. All customer_ids from summary exist in source customers table
5. No duplicate customer_ids
6. Customer segments are correctly assigned based on total_amount
7. Only active, non-deleted customers are included
```

---

## Rule 2: Product Performance Analytics

### Business Rule (Natural Language):
```
Transform product and sales data into product performance analytics.

For each product in the products table:
- Calculate total units sold from order_items
- Calculate total revenue (sum of line_total)
- Calculate average sale price
- Count total number of orders containing this product
- Get current stock quantity
- Calculate stock turnover rate (units sold / average stock)
- Rank products by revenue (revenue_rank)
- Rank products within their category (category_rank)
- Find the last sale date
- Identify the best-selling month (month with highest sales)
- Calculate performance score (0-100 based on revenue and units sold)

Store in product_performance table.

Only include products that have been sold at least once.
Only include products that are currently active (is_active = true).
```

### Expected Output:
- Table: `product_performance`
- ~480 records (active products with sales)
- Rankings calculated correctly
- Performance scores between 0-100

### Validation Rules:
```
Verify product performance calculations:
1. Total revenue matches sum of line_totals from order_items
2. No products with zero total_units_sold
3. Average_sale_price equals total_revenue / total_units_sold
4. Revenue ranks are sequential with no gaps
5. All product_ids exist in source products table
6. No duplicate product_ids
7. Performance scores are between 0 and 100
8. Stock turnover rates are positive numbers
```

---

## Rule 3: Daily Sales Summary

### Business Rule (Natural Language):
```
Aggregate daily sales data from orders and order_items tables.

For each date in the orders table:
- Count total orders placed
- Calculate total revenue (sum of order amounts)
- Count total items sold from order_items
- Calculate average order value
- Count distinct customers who placed orders
- Count new customers (customers whose first order is on this date)
- Count cancelled orders
- Count successful orders (status = 'delivered')
- Sum total discounts given
- Sum total tax collected
- Sum total shipping costs

Store results in daily_sales_summary table with sales_date as primary key.

Include all dates from the earliest order to today.
Group calculations by order date (date part only, ignore time).
```

### Expected Output:
- Table: `daily_sales_summary`
- ~730 records (2 years of data)
- Daily metrics calculated correctly
- No missing dates

### Validation Rules:
```
Verify daily aggregations are correct:
1. Sum of total_revenue across all days matches sum of order amounts from source
2. Each date appears exactly once
3. Total_orders equals successful_orders + cancelled_orders
4. Average_order_value equals total_revenue / total_orders
5. No NULL values in total_orders or total_revenue
6. All dates are within the valid range (earliest order to today)
7. Total_customers count is accurate
```

---

## Rule 4: Monthly Sales Summary

### Business Rule (Natural Language):
```
Create monthly sales aggregations from orders data.

For each year-month combination:
- Count total orders
- Calculate total revenue
- Count distinct customers
- Count new customers (first order in that month)
- Calculate average order value
- Count total items sold
- Identify top-selling product (by revenue)
- Identify top-selling category
- Calculate month-over-month growth rate

Store in monthly_sales_summary table with year_month (YYYY-MM) as primary key.

Calculate growth rate as: ((current_month_revenue - previous_month_revenue) / previous_month_revenue) * 100

Include all months that have at least one order.
```

### Expected Output:
- Table: `monthly_sales_summary`
- ~24 records (2 years)
- Growth rates calculated
- Top products identified

### Validation Rules:
```
Verify monthly aggregations:
1. Sum of monthly revenues matches annual total
2. Each year-month appears exactly once
3. Growth rate calculations are accurate
4. Top products exist in products table
5. All months have at least one order
6. No NULL values in key metrics
```

---

## Rule 5: Financial Reconciliation

### Business Rule (Natural Language):
```
Reconcile financial transactions by payment gateway.

For each date and payment gateway combination:
- Count total transactions
- Count successful transactions (status = 'success')
- Count failed transactions (status = 'failed')
- Sum total transaction amounts
- Sum total refunds (transaction_type = 'refund')
- Calculate net amount (total - refunds)
- Calculate estimated transaction fees (2.5% of net amount)
- Calculate gateway success rate percentage

Store in financial_reconciliation table.

Group by transaction date and payment_gateway.
Only include transactions from the last 12 months.
```

### Expected Output:
- Table: `financial_reconciliation`
- ~1095 records (365 days × 3 gateways)
- Financial calculations accurate
- Success rates between 0-100%

### Validation Rules:
```
Verify financial reconciliation:
1. Sum of all transaction amounts matches source
2. Net amount equals total_amount minus total_refunds
3. Gateway success rate equals (successful / total) * 100
4. All amounts are positive or zero
5. Transaction fees are 2.5% of net amount
6. No duplicate date-gateway combinations
```

---

## Rule 6: Inventory Status Analysis

### Business Rule (Natural Language):
```
Calculate current inventory status for all products.

For each product:
- Get current stock quantity from products table
- Sum total inbound movements from inventory_movements
- Sum total outbound movements (as positive number)
- Get reorder level from products table
- Determine stock status:
  * Out of Stock: current_stock = 0
  * Low Stock: current_stock <= reorder_level
  * Adequate: current_stock > reorder_level AND < (reorder_level * 3)
  * Overstock: current_stock >= (reorder_level * 3)
- Calculate days of stock (current_stock / average daily sales)
- Find last inbound date
- Find last outbound date
- Calculate total inventory value (current_stock * unit_price)
- Determine if reorder is needed (current_stock <= reorder_level)

Store in inventory_status table.

Include all products, even if they have no inventory movements.
```

### Expected Output:
- Table: `inventory_status`
- 500 records (all products)
- Stock status correctly categorized
- Reorder flags accurate

### Validation Rules:
```
Verify inventory calculations:
1. Total inbound minus total outbound equals current stock (with adjustments)
2. Stock status correctly assigned based on thresholds
3. All product_ids exist in products table
4. No negative stock quantities
5. Needs_reorder flag is true when current_stock <= reorder_level
6. Total_value equals current_stock * unit_price
```

---

## Rule 7: Customer Orders Fact Table (Dimensional Model)

### Business Rule (Natural Language):
```
Create a fact table for dimensional analysis.

Combine data from orders, order_items, and customers:
- Extract order_id, customer_id, product_id
- Extract order date as separate year, month, quarter
- Include quantity, unit_price, line_total
- Include discount_amount, tax_amount
- Calculate net_amount (line_total - discount_amount)
- Include order_status and payment_method

Store in customer_orders_fact table.

Only include orders with status 'delivered' or 'shipped'.
Extract year, month, quarter from order_date.
Create one row per order item (denormalized).
```

### Expected Output:
- Table: `customer_orders_fact`
- ~14000 records (denormalized order items)
- Proper dimensional attributes
- Date parts extracted correctly

### Validation Rules:
```
Verify fact table structure:
1. Each order_item creates exactly one fact row
2. Year, month, quarter correctly extracted from order_date
3. Net_amount equals line_total minus discount_amount
4. All customer_ids exist in customers table
5. All product_ids exist in products table
6. Only delivered or shipped orders included
```

---

## Rule 8: Category Performance Summary

### Business Rule (Natural Language):
```
Summarize sales performance by product category.

For each category in products table:
- Count total products in category
- Count active products
- Calculate total revenue from order_items
- Sum total units sold
- Calculate average product price
- Count total orders containing category products
- Rank categories by revenue
- Calculate market share percentage (category revenue / total revenue * 100)
- Identify top-selling product in category

Store in category_performance table with category as primary key.

Only include categories that have at least one sale.
```

### Expected Output:
- Table: `category_performance`
- 5 records (5 categories)
- Market shares sum to 100%
- Rankings assigned correctly

### Validation Rules:
```
Verify category performance:
1. Sum of category revenues equals total sales revenue
2. Market shares sum to approximately 100%
3. Revenue ranks are sequential with no gaps
4. All top products exist in products table
5. Active_products count <= total_products count
6. No NULL values in key metrics
```

---

## How to Use These Rules

### Using the Web Interface:
1. Start the server: `./start_server.sh`
2. Open `web/index.html`
3. Go to **Combined Workflow** tab
4. Select source table (e.g., `customers`, `orders`)
5. Enter target table (e.g., `customer_summary`)
6. Paste one of the business rules above
7. Click "Execute Complete Workflow"
8. Wait for ETL and validation to complete
9. Download the HTML report

### Using the CLI:
1. Edit `main.py` and replace the `business_rule_text` variable with one of the rules above
2. Run: `python main.py`
3. Check the generated report in `reports/` directory

### Using the API:
```bash
curl -X POST http://localhost:8000/api/workflow/execute \
  -H "Content-Type: application/json" \
  -d '{
    "source_table": "customers",
    "target_table": "customer_summary",
    "business_rule": "YOUR_BUSINESS_RULE_HERE",
    "run_etl": true,
    "run_validation": true,
    "user_id": "test_user"
  }'
```

---

## Expected Results

All these transformations should:
- ✅ Process successfully without errors
- ✅ Generate 10-15 comprehensive test scenarios
- ✅ Pass 90%+ of validation tests
- ✅ Complete in under 2 minutes for sample data
- ✅ Produce detailed HTML reports with charts
- ✅ Show accurate metrics and statistics

---

## Troubleshooting

If a transformation fails:
1. Check database connections in `.env`
2. Verify source tables have data
3. Review business rule for clarity
4. Check API logs: `tail -f logs/app.log`
5. Test with smaller batch size
6. Verify AI provider (OpenAI/Gemini) is configured correctly
