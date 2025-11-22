-- =====================================================
-- AutoETL Sample Data - Target Database (DB2)
-- =====================================================
-- This script creates target tables where transformed
-- data will be loaded by the ETL process
-- =====================================================

-- Database: target_db (DB2)

-- =====================================================
-- Target tables for ETL transformations
-- =====================================================

-- =====================================================
-- 1. CUSTOMER_SUMMARY (Target for customer aggregation)
-- =====================================================
DROP TABLE IF EXISTS customer_summary CASCADE;

CREATE TABLE customer_summary (
    customer_id INTEGER PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(200),
    total_orders INTEGER DEFAULT 0,
    total_amount DECIMAL(15, 2) DEFAULT 0.00,
    average_order_value DECIMAL(12, 2) DEFAULT 0.00,
    last_order_date TIMESTAMP,
    first_order_date TIMESTAMP,
    total_items_purchased INTEGER DEFAULT 0,
    customer_lifetime_value DECIMAL(15, 2) DEFAULT 0.00,
    preferred_payment_method VARCHAR(50),
    total_cancelled_orders INTEGER DEFAULT 0,
    status VARCHAR(50),
    customer_segment VARCHAR(50),  -- VIP, Regular, New, Inactive
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_summary_status ON customer_summary(status);
CREATE INDEX idx_customer_summary_segment ON customer_summary(customer_segment);

-- =====================================================
-- 2. PRODUCT_PERFORMANCE (Target for product analytics)
-- =====================================================
DROP TABLE IF EXISTS product_performance CASCADE;

CREATE TABLE product_performance (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    brand VARCHAR(100),
    total_units_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(15, 2) DEFAULT 0.00,
    average_sale_price DECIMAL(10, 2) DEFAULT 0.00,
    total_orders INTEGER DEFAULT 0,
    current_stock INTEGER DEFAULT 0,
    stock_turnover_rate DECIMAL(8, 2) DEFAULT 0.00,
    revenue_rank INTEGER,
    category_rank INTEGER,
    last_sale_date TIMESTAMP,
    best_selling_month VARCHAR(7),  -- YYYY-MM format
    performance_score DECIMAL(5, 2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_product_performance_category ON product_performance(category);
CREATE INDEX idx_product_performance_revenue_rank ON product_performance(revenue_rank);

-- =====================================================
-- 3. DAILY_SALES_SUMMARY (Target for daily aggregation)
-- =====================================================
DROP TABLE IF EXISTS daily_sales_summary CASCADE;

CREATE TABLE daily_sales_summary (
    sales_date DATE PRIMARY KEY,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(15, 2) DEFAULT 0.00,
    total_items_sold INTEGER DEFAULT 0,
    average_order_value DECIMAL(12, 2) DEFAULT 0.00,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    cancelled_orders INTEGER DEFAULT 0,
    successful_orders INTEGER DEFAULT 0,
    total_discount_given DECIMAL(12, 2) DEFAULT 0.00,
    total_tax_collected DECIMAL(12, 2) DEFAULT 0.00,
    total_shipping_cost DECIMAL(10, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_daily_sales_date ON daily_sales_summary(sales_date);

-- =====================================================
-- 4. MONTHLY_SALES_SUMMARY (Target for monthly aggregation)
-- =====================================================
DROP TABLE IF EXISTS monthly_sales_summary CASCADE;

CREATE TABLE monthly_sales_summary (
    year_month VARCHAR(7) PRIMARY KEY,  -- YYYY-MM
    year INTEGER,
    month INTEGER,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(15, 2) DEFAULT 0.00,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    average_order_value DECIMAL(12, 2) DEFAULT 0.00,
    total_items_sold INTEGER DEFAULT 0,
    top_product_id INTEGER,
    top_product_name VARCHAR(200),
    top_category VARCHAR(100),
    growth_rate DECIMAL(8, 2) DEFAULT 0.00,  -- Month-over-month growth
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_monthly_sales_year_month ON monthly_sales_summary(year_month);

-- =====================================================
-- 5. FINANCIAL_RECONCILIATION (Target for financial data)
-- =====================================================
DROP TABLE IF EXISTS financial_reconciliation CASCADE;

CREATE TABLE financial_reconciliation (
    reconciliation_date DATE,
    payment_gateway VARCHAR(100),
    total_transactions INTEGER DEFAULT 0,
    successful_transactions INTEGER DEFAULT 0,
    failed_transactions INTEGER DEFAULT 0,
    total_amount DECIMAL(15, 2) DEFAULT 0.00,
    total_refunds DECIMAL(15, 2) DEFAULT 0.00,
    net_amount DECIMAL(15, 2) DEFAULT 0.00,
    transaction_fees DECIMAL(12, 2) DEFAULT 0.00,
    gateway_success_rate DECIMAL(5, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (reconciliation_date, payment_gateway)
);

CREATE INDEX idx_financial_reconciliation_date ON financial_reconciliation(reconciliation_date);

-- =====================================================
-- 6. INVENTORY_STATUS (Target for inventory analytics)
-- =====================================================
DROP TABLE IF EXISTS inventory_status CASCADE;

CREATE TABLE inventory_status (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    current_stock INTEGER DEFAULT 0,
    total_inbound INTEGER DEFAULT 0,
    total_outbound INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 0,
    stock_status VARCHAR(50),  -- Adequate, Low Stock, Out of Stock, Overstock
    days_of_stock DECIMAL(8, 2) DEFAULT 0.00,
    last_inbound_date TIMESTAMP,
    last_outbound_date TIMESTAMP,
    total_value DECIMAL(15, 2) DEFAULT 0.00,
    needs_reorder BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_inventory_status_stock_status ON inventory_status(stock_status);
CREATE INDEX idx_inventory_status_needs_reorder ON inventory_status(needs_reorder);

-- =====================================================
-- 7. CUSTOMER_ORDERS_FACT (Target for dimensional model)
-- =====================================================
DROP TABLE IF EXISTS customer_orders_fact CASCADE;

CREATE TABLE customer_orders_fact (
    fact_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_date DATE,
    order_year INTEGER,
    order_month INTEGER,
    order_quarter INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    line_total DECIMAL(12, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    net_amount DECIMAL(12, 2),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_orders_fact_order_date ON customer_orders_fact(order_date);
CREATE INDEX idx_customer_orders_fact_customer_id ON customer_orders_fact(customer_id);
CREATE INDEX idx_customer_orders_fact_product_id ON customer_orders_fact(product_id);

-- =====================================================
-- 8. CATEGORY_PERFORMANCE (Target for category analytics)
-- =====================================================
DROP TABLE IF EXISTS category_performance CASCADE;

CREATE TABLE category_performance (
    category VARCHAR(100) PRIMARY KEY,
    total_products INTEGER DEFAULT 0,
    active_products INTEGER DEFAULT 0,
    total_revenue DECIMAL(15, 2) DEFAULT 0.00,
    total_units_sold INTEGER DEFAULT 0,
    average_product_price DECIMAL(10, 2) DEFAULT 0.00,
    total_orders INTEGER DEFAULT 0,
    revenue_rank INTEGER,
    market_share_percent DECIMAL(5, 2) DEFAULT 0.00,
    top_product_id INTEGER,
    top_product_name VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_category_performance_revenue_rank ON category_performance(revenue_rank);

-- =====================================================
-- Summary
-- =====================================================
SELECT 'Target Database Setup Complete' AS info;

SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'public'
    AND table_name IN (
        'customer_summary',
        'product_performance',
        'daily_sales_summary',
        'monthly_sales_summary',
        'financial_reconciliation',
        'inventory_status',
        'customer_orders_fact',
        'category_performance'
    )
ORDER BY table_name, ordinal_position;

-- These tables are now ready to receive transformed data from the ETL process!
