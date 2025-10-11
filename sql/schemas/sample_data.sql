-- Sample data generation for realistic testing and demos
-- This creates a comprehensive dataset showcasing complex SQL scenarios

-- Insert sample customers with realistic business data
INSERT INTO customers (customer_code, company_name, industry, region, country, customer_tier, metadata_json) VALUES
('CUST-001', 'TechCorp Industries', 'Technology', 'North America', 'USA', 'Premium', '{"revenue_range": "10M-50M", "employees": 500}'),
('CUST-002', 'Global Manufacturing Inc', 'Manufacturing', 'Europe', 'Germany', 'Premium', '{"revenue_range": "50M+", "employees": 2000}'),
('CUST-003', 'Healthcare Solutions Ltd', 'Healthcare', 'North America', 'Canada', 'Standard', '{"revenue_range": "1M-10M", "employees": 100}'),
('CUST-004', 'Financial Services Co', 'Finance', 'Asia Pacific', 'Singapore', 'Premium', '{"revenue_range": "100M+", "employees": 5000}'),
('CUST-005', 'Retail Chain Corp', 'Retail', 'North America', 'USA', 'Standard', '{"revenue_range": "25M-100M", "employees": 1200}'),
('CUST-006', 'Energy Systems Ltd', 'Energy', 'Europe', 'UK', 'Premium', '{"revenue_range": "75M+", "employees": 800}'),
('CUST-007', 'Educational Institute', 'Education', 'North America', 'USA', 'Basic', '{"revenue_range": "5M-25M", "employees": 300}'),
('CUST-008', 'Automotive Parts Inc', 'Automotive', 'Asia Pacific', 'Japan', 'Standard', '{"revenue_range": "15M-50M", "employees": 600}'),
('CUST-009', 'Construction Group', 'Construction', 'Europe', 'France', 'Standard', '{"revenue_range": "20M-75M", "employees": 400}'),
('CUST-010', 'Logistics Solutions', 'Transportation', 'North America', 'USA', 'Premium', '{"revenue_range": "40M-100M", "employees": 900}');

-- Insert sample products with hierarchical categories
INSERT INTO products (sku, product_name, category, subcategory, brand, unit_cost, unit_price, attributes) VALUES
('PRD-SW-001', 'AutoCAD Professional 2024', 'Software', 'CAD Software', 'Autodesk', 800.00, 1200.00, '{"license_type": "annual", "platform": "Windows/Mac"}'),
('PRD-SW-002', 'Maya Animation Suite', 'Software', '3D Animation', 'Autodesk', 600.00, 950.00, '{"license_type": "annual", "platform": "Windows/Mac/Linux"}'),
('PRD-SW-003', 'Inventor Professional', 'Software', 'CAD Software', 'Autodesk', 700.00, 1100.00, '{"license_type": "annual", "platform": "Windows"}'),
('PRD-HW-001', 'CAD Workstation Pro', 'Hardware', 'Workstations', 'Dell', 2500.00, 3500.00, '{"cpu": "Intel i9", "ram": "32GB", "gpu": "RTX 4080"}'),
('PRD-HW-002', 'Graphics Tablet XL', 'Hardware', 'Input Devices', 'Wacom', 800.00, 1200.00, '{"size": "24 inch", "pressure_levels": 8192}'),
('PRD-SRV-001', 'Cloud Rendering Service', 'Services', 'Cloud Computing', 'Autodesk', 50.00, 75.00, '{"billing": "per_hour", "gpu_type": "Tesla V100"}'),
('PRD-TRN-001', 'AutoCAD Training Course', 'Training', 'Software Training', 'Autodesk', 200.00, 500.00, '{"duration": "40 hours", "format": "online"}'),
('PRD-SUP-001', 'Premium Support Plan', 'Support', 'Technical Support', 'Autodesk', 100.00, 300.00, '{"response_time": "4 hours", "availability": "24/7"}'),
('PRD-SW-004', '3ds Max Professional', 'Software', '3D Modeling', 'Autodesk', 650.00, 1000.00, '{"license_type": "annual", "platform": "Windows"}'),
('PRD-SW-005', 'Fusion 360 Enterprise', 'Software', 'CAD Software', 'Autodesk', 400.00, 600.00, '{"license_type": "annual", "platform": "Cloud"}');

-- Generate realistic orders with complex patterns
WITH order_data AS (
    SELECT 
        'ORD-' || LPAD((ROW_NUMBER() OVER())::text, 6, '0') as order_number,
        c.customer_id,
        -- Generate realistic order dates with seasonal patterns
        (CURRENT_DATE - (RANDOM() * 365)::int * INTERVAL '1 day' + 
         CASE 
             WHEN EXTRACT(month from CURRENT_DATE) IN (11, 12) THEN INTERVAL '30 days' * RANDOM() -- Holiday boost
             WHEN EXTRACT(month from CURRENT_DATE) IN (6, 7, 8) THEN -INTERVAL '15 days' * RANDOM() -- Summer dip
             ELSE INTERVAL '0 days'
         END)::date as order_date,
        CASE 
            WHEN RANDOM() < 0.8 THEN 'COMPLETED'
            WHEN RANDOM() < 0.95 THEN 'SHIPPED'
            ELSE 'CANCELLED'
        END as order_status,
        -- Order source distribution
        CASE 
            WHEN RANDOM() < 0.6 THEN 'Online'
            WHEN RANDOM() < 0.85 THEN 'Phone'
            ELSE 'Store'
        END as order_source,
        'REP-' || LPAD((1 + RANDOM() * 20)::int::text, 3, '0') as sales_rep_id
    FROM customers c, generate_series(1, 15) -- 15 orders per customer on average
    WHERE RANDOM() < 0.9 -- Some customers have fewer orders
)
INSERT INTO orders (order_number, customer_id, order_date, order_status, order_source, sales_rep_id, total_amount, discount_amount, tax_amount, shipping_cost, ship_date)
SELECT 
    order_number,
    customer_id,
    order_date,
    order_status,
    order_source,
    sales_rep_id,
    0, -- Will be calculated after order items
    0,
    0,
    CASE 
        WHEN order_source = 'Store' THEN 0
        ELSE 25 + RANDOM() * 75
    END,
    CASE 
        WHEN order_status IN ('SHIPPED', 'COMPLETED') THEN order_date + (1 + RANDOM() * 5)::int * INTERVAL '1 day'
        ELSE NULL
    END
FROM order_data;

-- Generate order items with realistic product mix
WITH order_items_data AS (
    SELECT 
        o.order_id,
        p.product_id,
        p.unit_price,
        -- Quantity based on product type
        CASE 
            WHEN p.category = 'Software' THEN 1 + (RANDOM() * 10)::int
            WHEN p.category = 'Hardware' THEN 1 + (RANDOM() * 3)::int
            WHEN p.category = 'Services' THEN (10 + RANDOM() * 100)::int -- Hours
            ELSE 1 + (RANDOM() * 5)::int
        END as quantity,
        -- Dynamic discount based on customer tier and quantity
        CASE 
            WHEN c.customer_tier = 'Premium' THEN 10 + RANDOM() * 15
            WHEN c.customer_tier = 'Standard' THEN 5 + RANDOM() * 10
            ELSE RANDOM() * 5
        END as discount_percent
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    CROSS JOIN products p
    WHERE RANDOM() < 0.3 -- Not every customer orders every product
    AND o.order_status != 'CANCELLED'
)
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent, line_total)
SELECT 
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_percent,
    ROUND((quantity * unit_price * (100 - discount_percent) / 100)::numeric, 2)
FROM order_items_data;

-- Update order totals based on line items
UPDATE orders 
SET 
    total_amount = (
        SELECT COALESCE(SUM(line_total), 0)
        FROM order_items oi 
        WHERE oi.order_id = orders.order_id
    ),
    discount_amount = (
        SELECT COALESCE(SUM(quantity * unit_price * discount_percent / 100), 0)
        FROM order_items oi 
        WHERE oi.order_id = orders.order_id
    );

-- Calculate tax (8.5% rate)
UPDATE orders 
SET tax_amount = ROUND((total_amount * 0.085)::numeric, 2)
WHERE total_amount > 0;

-- Generate inventory levels
INSERT INTO inventory_levels (product_id, warehouse_location, quantity_on_hand, quantity_reserved, reorder_point, max_stock_level)
SELECT 
    p.product_id,
    location,
    -- Stock levels based on product popularity and type
    CASE 
        WHEN p.category = 'Software' THEN 1000 + (RANDOM() * 5000)::int -- Digital products
        WHEN p.category = 'Hardware' THEN 50 + (RANDOM() * 200)::int
        WHEN p.category = 'Services' THEN 999999 -- Unlimited
        ELSE 100 + (RANDOM() * 500)::int
    END as quantity_on_hand,
    (RANDOM() * 50)::int as quantity_reserved,
    CASE 
        WHEN p.category = 'Software' THEN 100
        WHEN p.category = 'Hardware' THEN 10 + (RANDOM() * 20)::int
        WHEN p.category = 'Services' THEN 0
        ELSE 25 + (RANDOM() * 50)::int
    END as reorder_point,
    CASE 
        WHEN p.category = 'Software' THEN 10000
        WHEN p.category = 'Hardware' THEN 500 + (RANDOM() * 1000)::int
        WHEN p.category = 'Services' THEN 999999
        ELSE 1000 + (RANDOM() * 2000)::int
    END as max_stock_level
FROM products p
CROSS JOIN (
    VALUES ('New York'), ('Los Angeles'), ('Chicago'), ('Dallas'), ('London'), ('Frankfurt'), ('Tokyo'), ('Singapore')
) AS locations(location);

-- Generate some query performance logs for AI analysis
INSERT INTO query_performance_log (query_hash, query_text, execution_time_ms, rows_returned, query_cost, execution_plan, user_id, database_name)
VALUES 
('a1b2c3d4', 'SELECT * FROM customers WHERE industry = ''Technology''', 45.2, 150, 4.5, '{"Plan": {"Node Type": "Index Scan"}}', 'user001', 'autosql_warehouse'),
('e5f6g7h8', 'SELECT o.*, c.company_name FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_date >= ''2024-01-01''', 1250.8, 2500, 45.2, '{"Plan": {"Node Type": "Hash Join"}}', 'user002', 'autosql_warehouse'),
('i9j0k1l2', 'SELECT product_name, SUM(oi.quantity) FROM products p JOIN order_items oi ON p.product_id = oi.product_id GROUP BY product_name', 890.3, 10, 32.1, '{"Plan": {"Node Type": "HashAggregate"}}', 'user003', 'autosql_warehouse'),
('m3n4o5p6', 'SELECT COUNT(*) FROM orders WHERE order_status = ''COMPLETED''', 15.1, 1, 2.1, '{"Plan": {"Node Type": "Index Scan"}}', 'user001', 'autosql_warehouse'),
('q7r8s9t0', 'UPDATE inventory_levels SET quantity_on_hand = quantity_on_hand - 1 WHERE product_id = $1', 2150.5, 0, 78.9, '{"Plan": {"Node Type": "Update"}}', 'system', 'autosql_warehouse');

-- Generate AI optimization suggestions based on the slow queries
INSERT INTO ai_optimization_suggestions (suggestion_type, target_table, target_query_hash, suggestion_text, estimated_improvement, implementation_sql, confidence_score, status)
VALUES 
('INDEX', 'orders', 'e5f6g7h8', 'Create composite index on (customer_id, order_date) to optimize join performance', 65.5, 'CREATE INDEX CONCURRENTLY idx_orders_customer_date ON orders (customer_id, order_date);', 0.85, 'PENDING'),
('QUERY_REWRITE', 'products', 'i9j0k1l2', 'Consider using window functions instead of GROUP BY for better performance', 25.3, 'SELECT product_name, SUM(quantity) OVER (PARTITION BY product_name) FROM products p JOIN order_items oi ON p.product_id = oi.product_id', 0.72, 'PENDING'),
('PARTITION', 'orders', 'e5f6g7h8', 'Partition orders table by month to improve query performance on date ranges', 45.0, 'CREATE TABLE orders_partitioned (LIKE orders) PARTITION BY RANGE (order_date);', 0.78, 'PENDING'),
('INDEX', 'inventory_levels', 'q7r8s9t0', 'Add partial index for active inventory updates', 35.2, 'CREATE INDEX CONCURRENTLY idx_inventory_product_updates ON inventory_levels (product_id) WHERE quantity_on_hand > 0;', 0.68, 'PENDING');

-- Refresh analytics to populate materialized views
SELECT analytics.refresh_all_analytics();
