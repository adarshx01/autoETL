-- =====================================================
-- AutoETL Sample Data - Source Database (DB1)
-- =====================================================
-- This script creates sample tables with realistic data
-- for testing the ETL validation system
-- =====================================================

-- Database: source_db (DB1)

-- =====================================================
-- 1. CUSTOMERS TABLE
-- =====================================================
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',  -- active, inactive, deleted
    registration_date DATE,
    last_login_date TIMESTAMP,
    credit_limit DECIMAL(10, 2),
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 1000 sample customers
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, country, status, registration_date, last_login_date, credit_limit, is_deleted)
SELECT 
    'Customer_' || gs AS first_name,
    'LastName_' || gs AS last_name,
    'customer' || gs || '@example.com' AS email,
    '+1-555-' || LPAD(gs::TEXT, 4, '0') AS phone,
    gs || ' Main Street' AS address,
    CASE (gs % 10)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia'
        WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego'
        WHEN 8 THEN 'Dallas'
        ELSE 'San Jose'
    END AS city,
    CASE (gs % 5)
        WHEN 0 THEN 'NY'
        WHEN 1 THEN 'CA'
        WHEN 2 THEN 'TX'
        WHEN 3 THEN 'FL'
        ELSE 'IL'
    END AS state,
    LPAD((10000 + gs % 90000)::TEXT, 5, '0') AS zip_code,
    'USA' AS country,
    CASE 
        WHEN gs % 20 = 0 THEN 'inactive'
        WHEN gs % 50 = 0 THEN 'deleted'
        ELSE 'active'
    END AS status,
    CURRENT_DATE - (gs % 730) AS registration_date,
    CURRENT_TIMESTAMP - INTERVAL '1 day' * (gs % 90) AS last_login_date,
    1000.00 + (gs % 50) * 100 AS credit_limit,
    CASE WHEN gs % 50 = 0 THEN TRUE ELSE FALSE END AS is_deleted
FROM generate_series(1, 1000) AS gs;

-- =====================================================
-- 2. PRODUCTS TABLE
-- =====================================================
DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    cost_price DECIMAL(10, 2),
    stock_quantity INTEGER,
    reorder_level INTEGER,
    supplier_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    weight_kg DECIMAL(8, 2),
    dimensions VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 500 sample products
INSERT INTO products (product_name, category, subcategory, brand, unit_price, cost_price, stock_quantity, reorder_level, supplier_id, is_active, weight_kg)
SELECT 
    CASE (gs % 10)
        WHEN 0 THEN 'Laptop Model ' || gs
        WHEN 1 THEN 'Smartphone Pro ' || gs
        WHEN 2 THEN 'Tablet Air ' || gs
        WHEN 3 THEN 'Headphones Elite ' || gs
        WHEN 4 THEN 'Camera DSLR ' || gs
        WHEN 5 THEN 'Monitor 4K ' || gs
        WHEN 6 THEN 'Keyboard Mechanical ' || gs
        WHEN 7 THEN 'Mouse Wireless ' || gs
        WHEN 8 THEN 'Speaker Bluetooth ' || gs
        ELSE 'Router WiFi ' || gs
    END AS product_name,
    CASE (gs % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Computers'
        WHEN 2 THEN 'Audio'
        WHEN 3 THEN 'Accessories'
        ELSE 'Networking'
    END AS category,
    CASE (gs % 3)
        WHEN 0 THEN 'Premium'
        WHEN 1 THEN 'Standard'
        ELSE 'Budget'
    END AS subcategory,
    CASE (gs % 5)
        WHEN 0 THEN 'TechBrand'
        WHEN 1 THEN 'EliteTech'
        WHEN 2 THEN 'ProGear'
        WHEN 3 THEN 'SmartDevices'
        ELSE 'InnovateTech'
    END AS brand,
    (50 + (gs % 50) * 20)::DECIMAL(10, 2) AS unit_price,
    (30 + (gs % 50) * 12)::DECIMAL(10, 2) AS cost_price,
    (gs % 500) + 10 AS stock_quantity,
    (gs % 50) + 5 AS reorder_level,
    (gs % 20) + 1 AS supplier_id,
    CASE WHEN gs % 30 = 0 THEN FALSE ELSE TRUE END AS is_active,
    (0.5 + (gs % 20) * 0.3)::DECIMAL(8, 2) AS weight_kg
FROM generate_series(1, 500) AS gs;

-- =====================================================
-- 3. ORDERS TABLE
-- =====================================================
DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(50),  -- pending, processing, shipped, delivered, cancelled
    total_amount DECIMAL(12, 2),
    discount_amount DECIMAL(10, 2) DEFAULT 0.00,
    tax_amount DECIMAL(10, 2),
    shipping_cost DECIMAL(8, 2),
    payment_method VARCHAR(50),  -- credit_card, debit_card, paypal, bank_transfer
    shipping_address VARCHAR(255),
    billing_address VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 5000 sample orders (last 2 years)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, discount_amount, tax_amount, shipping_cost, payment_method, shipping_address, billing_address)
SELECT 
    (gs % 1000) + 1 AS customer_id,
    CURRENT_TIMESTAMP - INTERVAL '1 day' * (gs % 730) AS order_date,
    CASE (gs % 10)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'delivered'
        WHEN 4 THEN 'delivered'
        WHEN 5 THEN 'delivered'
        WHEN 6 THEN 'delivered'
        WHEN 7 THEN 'delivered'
        WHEN 8 THEN 'cancelled'
        ELSE 'delivered'
    END AS order_status,
    (50 + (gs % 500) * 2)::DECIMAL(12, 2) AS total_amount,
    CASE WHEN gs % 10 = 0 THEN (gs % 50)::DECIMAL ELSE 0 END AS discount_amount,
    ((50 + (gs % 500) * 2) * 0.08)::DECIMAL(10, 2) AS tax_amount,
    (5 + (gs % 10))::DECIMAL(8, 2) AS shipping_cost,
    CASE (gs % 4)
        WHEN 0 THEN 'credit_card'
        WHEN 1 THEN 'debit_card'
        WHEN 2 THEN 'paypal'
        ELSE 'bank_transfer'
    END AS payment_method,
    ((gs % 1000) + 1) || ' Main Street' AS shipping_address,
    ((gs % 1000) + 1) || ' Main Street' AS billing_address
FROM generate_series(1, 5000) AS gs;

-- =====================================================
-- 4. ORDER_ITEMS TABLE
-- =====================================================
DROP TABLE IF EXISTS order_items CASCADE;

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    line_total DECIMAL(12, 2),
    discount_amount DECIMAL(10, 2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert order items (1-5 items per order, ~15000 total items)
INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total, discount_amount)
SELECT 
    o.order_id,
    (((o.order_id * item_num) % 500) + 1) AS product_id,
    (item_num % 5) + 1 AS quantity,
    p.unit_price,
    p.unit_price * ((item_num % 5) + 1) AS line_total,
    CASE WHEN item_num = 1 AND o.order_id % 10 = 0 THEN p.unit_price * 0.1 ELSE 0 END AS discount_amount
FROM orders o
CROSS JOIN generate_series(1, 3) AS item_num
JOIN products p ON p.product_id = (((o.order_id * item_num) % 500) + 1)
WHERE o.order_id <= 5000;

-- =====================================================
-- 5. TRANSACTIONS TABLE (for financial reconciliation)
-- =====================================================
DROP TABLE IF EXISTS transactions CASCADE;

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_type VARCHAR(50),  -- payment, refund, adjustment
    amount DECIMAL(12, 2),
    payment_gateway VARCHAR(100),
    transaction_reference VARCHAR(200),
    status VARCHAR(50),  -- success, failed, pending, reversed
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert transactions for orders
INSERT INTO transactions (order_id, transaction_date, transaction_type, amount, payment_gateway, transaction_reference, status)
SELECT 
    o.order_id,
    o.order_date + INTERVAL '1 hour',
    CASE 
        WHEN o.order_status = 'cancelled' THEN 'refund'
        ELSE 'payment'
    END AS transaction_type,
    o.total_amount,
    CASE (o.order_id % 3)
        WHEN 0 THEN 'Stripe'
        WHEN 1 THEN 'PayPal'
        ELSE 'Square'
    END AS payment_gateway,
    'TXN-' || LPAD(o.order_id::TEXT, 10, '0') AS transaction_reference,
    CASE 
        WHEN o.order_status = 'cancelled' THEN 'reversed'
        WHEN o.order_id % 50 = 0 THEN 'failed'
        ELSE 'success'
    END AS status
FROM orders o;

-- =====================================================
-- 6. INVENTORY_MOVEMENTS TABLE
-- =====================================================
DROP TABLE IF EXISTS inventory_movements CASCADE;

CREATE TABLE inventory_movements (
    movement_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    movement_type VARCHAR(50),  -- inbound, outbound, adjustment
    quantity INTEGER,
    reference_id INTEGER,  -- order_id for outbound, purchase_order_id for inbound
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert inventory movements based on orders
INSERT INTO inventory_movements (product_id, movement_type, quantity, reference_id, movement_date)
SELECT 
    oi.product_id,
    'outbound',
    -oi.quantity,
    oi.order_id,
    o.order_date
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
WHERE o.order_status IN ('shipped', 'delivered');

-- Add some inbound movements (stock replenishment)
INSERT INTO inventory_movements (product_id, movement_type, quantity, reference_id, movement_date)
SELECT 
    product_id,
    'inbound',
    (gs % 100) + 50,
    gs,
    CURRENT_TIMESTAMP - INTERVAL '1 day' * (gs % 180)
FROM products
CROSS JOIN generate_series(1, 3) AS gs;

-- =====================================================
-- Create Indexes for Performance
-- =====================================================
CREATE INDEX idx_customers_status ON customers(status);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_order_status ON orders(order_status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_transactions_order_id ON transactions(order_id);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_inventory_movements_product_id ON inventory_movements(product_id);

-- =====================================================
-- Summary Statistics
-- =====================================================
SELECT 'Data Load Summary' AS info;
SELECT 'Customers' AS table_name, COUNT(*) AS record_count FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items
UNION ALL
SELECT 'Transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'Inventory Movements', COUNT(*) FROM inventory_movements;

-- =====================================================
-- Sample Queries to Verify Data
-- =====================================================
-- Active customers with orders
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.status = 'active' AND c.is_deleted = FALSE
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
HAVING COUNT(o.order_id) > 0
ORDER BY total_spent DESC
LIMIT 10;

-- Top selling products
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(oi.quantity) AS total_sold,
    SUM(oi.line_total) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

VACUUM ANALYZE;
