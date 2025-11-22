# Sample Data for AutoETL System

## Overview
This directory contains SQL scripts to create realistic sample databases for testing the AutoETL validation system.

## Files

### 1. `01_create_source_database.sql`
Creates the **source database (DB1)** with:
- **1,000 customers** (active, inactive, deleted)
- **500 products** (various categories)
- **5,000 orders** (last 2 years)
- **~15,000 order items** (multiple items per order)
- **5,000 transactions** (payments, refunds)
- **~20,000 inventory movements** (inbound, outbound)

### 2. `02_create_target_database.sql`
Creates the **target database (DB2)** with empty tables:
- `customer_summary` - Customer aggregations
- `product_performance` - Product analytics
- `daily_sales_summary` - Daily metrics
- `monthly_sales_summary` - Monthly metrics
- `financial_reconciliation` - Financial data
- `inventory_status` - Inventory analytics
- `customer_orders_fact` - Fact table
- `category_performance` - Category analytics

### 3. `03_sample_business_rules.md`
Contains **8 realistic business rules** for testing transformations with validation requirements.

---

## Setup Instructions

### Option 1: PostgreSQL (Recommended)

#### Step 1: Create Databases
```bash
# Create source database
sudo -u postgres createdb source_db

# Create target database
sudo -u postgres createdb target_db
```

#### Step 2: Load Sample Data
```bash
# Load source data (takes ~30 seconds)
psql -U postgres -d source_db -f sample_data/01_create_source_database.sql

# Create target tables
psql -U postgres -d target_db -f sample_data/02_create_target_database.sql
```

#### Step 3: Verify Data Load
```bash
# Check source database
psql -U postgres -d source_db -c "SELECT 'Customers' as table_name, COUNT(*) FROM customers UNION ALL SELECT 'Orders', COUNT(*) FROM orders UNION ALL SELECT 'Products', COUNT(*) FROM products;"

# Should show:
# Customers: 1000
# Orders: 5000
# Products: 500
```

#### Step 4: Configure AutoETL
Edit `.env` file:
```env
# Source Database (DB1)
SOURCE_DB_TYPE=postgresql
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=source_db
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=your_password

# Target Database (DB2)
TARGET_DB_TYPE=postgresql
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_NAME=target_db
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=your_password
```

---

### Option 2: MySQL

#### Step 1: Create Databases
```bash
mysql -u root -p -e "CREATE DATABASE source_db;"
mysql -u root -p -e "CREATE DATABASE target_db;"
```

#### Step 2: Adapt SQL Scripts
```bash
# MySQL doesn't support generate_series, you'll need to adapt
# Or use the provided MySQL-compatible version (if created)
```

#### Step 3: Load Data
```bash
mysql -u root -p source_db < sample_data/01_create_source_database.sql
mysql -u root -p target_db < sample_data/02_create_target_database.sql
```

#### Step 4: Configure .env
```env
SOURCE_DB_TYPE=mysql
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=3306
SOURCE_DB_NAME=source_db
SOURCE_DB_USER=root
SOURCE_DB_PASSWORD=your_password

TARGET_DB_TYPE=mysql
TARGET_DB_HOST=localhost
TARGET_DB_PORT=3306
TARGET_DB_NAME=target_db
TARGET_DB_USER=root
TARGET_DB_PASSWORD=your_password
```

---

### Option 3: Using Docker

#### Step 1: Create docker-compose.yml
```yaml
version: '3.8'
services:
  source_db:
    image: postgres:15
    environment:
      POSTGRES_DB: source_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres123
    ports:
      - "5432:5432"
    volumes:
      - source_data:/var/lib/postgresql/data
      - ./sample_data/01_create_source_database.sql:/docker-entrypoint-initdb.d/01_init.sql
  
  target_db:
    image: postgres:15
    environment:
      POSTGRES_DB: target_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres123
    ports:
      - "5433:5432"
    volumes:
      - target_data:/var/lib/postgresql/data
      - ./sample_data/02_create_target_database.sql:/docker-entrypoint-initdb.d/02_init.sql

volumes:
  source_data:
  target_data:
```

#### Step 2: Start Containers
```bash
cd /home/adarsh/Documents/Projects/autoETL
docker-compose up -d
```

#### Step 3: Configure .env
```env
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=source_db
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=postgres123

TARGET_DB_HOST=localhost
TARGET_DB_PORT=5433
TARGET_DB_NAME=target_db
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=postgres123
```

---

## Testing the System

### Test 1: Customer Summary Transformation

```bash
# Start the API server
./start_server.sh

# In another terminal, test the API
curl -X POST http://localhost:8000/api/workflow/execute \
  -H "Content-Type: application/json" \
  -d '{
    "source_table": "customers",
    "target_table": "customer_summary",
    "business_rule": "Calculate total orders and revenue for each active customer. Group by customer_id and include only customers with at least one order.",
    "run_etl": true,
    "run_validation": true,
    "user_id": "test_user"
  }'
```

### Test 2: Using Web Interface

1. Open `web/index.html`
2. Go to **Stage 1: ETL Execution**
3. Select `customers` from source table dropdown
4. Enter `customer_summary` as target table
5. Paste business rule from `03_sample_business_rules.md`
6. Click "Execute ETL"

### Test 3: Using CLI

```bash
# Edit main.py to use one of the sample business rules
nano main.py

# Run
python main.py

# Check reports directory
ls -lh reports/
```

---

## Data Statistics

### Source Database (DB1)
```
Customers:              1,000 records
├── Active:            950 (95%)
├── Inactive:          40 (4%)
└── Deleted:           10 (1%)

Products:               500 records
├── Active:            484 (96.8%)
└── Inactive:          16 (3.2%)

Orders:                 5,000 records
├── Delivered:         3,500 (70%)
├── Pending:           500 (10%)
├── Processing:        500 (10%)
├── Shipped:           500 (10%)
└── Cancelled:         500 (10%)

Order Items:            ~15,000 records
Transactions:           5,000 records
Inventory Movements:    ~20,000 records

Total Records:          ~46,500 records
Database Size:          ~50 MB
```

### Target Database (DB2)
```
Initial State: Empty tables (structure only)

After ETL transformations:
- customer_summary:         ~950 records
- product_performance:      ~480 records
- daily_sales_summary:      ~730 records
- monthly_sales_summary:    ~24 records
- financial_reconciliation: ~1,095 records
- inventory_status:         500 records
- customer_orders_fact:     ~14,000 records
- category_performance:     5 records
```

---

## Sample Queries

### Check Top Customers
```sql
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as name,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.status = 'active'
GROUP BY c.customer_id, name
ORDER BY total_spent DESC
LIMIT 10;
```

### Check Top Products
```sql
SELECT 
    p.product_name,
    p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.line_total) as revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;
```

### Check Sales by Month
```sql
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as orders,
    SUM(total_amount) as revenue
FROM orders
GROUP BY month
ORDER BY month DESC;
```

---

## Troubleshooting

### Issue: "relation does not exist"
```bash
# Make sure you're connected to the right database
psql -U postgres -d source_db -c "\dt"
```

### Issue: "permission denied"
```bash
# Grant permissions
psql -U postgres -d source_db -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO your_user;"
```

### Issue: Data load is slow
```bash
# Increase work_mem for faster inserts
psql -U postgres -c "ALTER SYSTEM SET work_mem = '256MB';"
psql -U postgres -c "SELECT pg_reload_conf();"
```

### Issue: Out of disk space
```bash
# Check database size
psql -U postgres -c "SELECT pg_size_pretty(pg_database_size('source_db'));"

# Clean up if needed
psql -U postgres -c "VACUUM FULL;"
```

---

## Cleanup

### Remove Databases
```bash
# PostgreSQL
sudo -u postgres dropdb source_db
sudo -u postgres dropdb target_db

# MySQL
mysql -u root -p -e "DROP DATABASE source_db;"
mysql -u root -p -e "DROP DATABASE target_db;"

# Docker
docker-compose down -v
```

---

## Next Steps

1. ✅ Load sample data
2. ✅ Configure `.env` file
3. ✅ Start API server: `./start_server.sh`
4. ✅ Open web interface: `web/index.html`
5. ✅ Try business rules from `03_sample_business_rules.md`
6. ✅ View generated reports in `reports/` directory
7. ✅ Check metrics dashboard

---

## Additional Resources

- **Full Documentation**: `README_COMPREHENSIVE.md`
- **Usage Guide**: `USAGE_GUIDE.md`
- **Quick Reference**: `QUICK_REFERENCE.md`
- **Business Rules**: `sample_data/03_sample_business_rules.md`

---

**Happy Testing! 🚀**
