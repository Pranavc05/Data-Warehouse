-- Advanced database schema with enterprise-grade features
-- This creates a complex data warehouse with multiple business domains
-- Showcasing advanced SQL concepts for Autodesk interview

-- Enable required PostgreSQL extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas for different business domains
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Advanced partitioning example: Orders by date
-- This demonstrates enterprise-level data management
CREATE TABLE sales.orders_2024 (
    LIKE orders INCLUDING ALL
) PARTITION BY RANGE (order_date);

-- Create monthly partitions (showing advanced partitioning strategy)
CREATE TABLE sales.orders_2024_01 PARTITION OF sales.orders_2024
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE sales.orders_2024_02 PARTITION OF sales.orders_2024
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE sales.orders_2024_03 PARTITION OF sales.orders_2024
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
-- Continue for all months...

-- Advanced indexing strategies for query optimization
CREATE INDEX CONCURRENTLY idx_customers_advanced_lookup 
ON customers USING btree (industry, region, customer_tier) 
WHERE is_active = true;

CREATE INDEX CONCURRENTLY idx_orders_time_series 
ON orders USING brin (order_date, created_at);

CREATE INDEX CONCURRENTLY idx_products_text_search 
ON products USING gin (to_tsvector('english', product_name || ' ' || COALESCE(brand, '')));

-- Materialized views for complex analytics (pre-computed for performance)
CREATE MATERIALIZED VIEW analytics.monthly_sales_summary AS
SELECT 
    DATE_TRUNC('month', o.order_date) as month,
    c.industry,
    c.region,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.total_amount) as median_order_value,
    SUM(CASE WHEN o.order_status = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_orders,
    SUM(CASE WHEN o.order_status = 'CANCELLED' THEN 1 ELSE 0 END)::float / COUNT(*) as cancellation_rate
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '24 months'
GROUP BY DATE_TRUNC('month', o.order_date), c.industry, c.region;

CREATE UNIQUE INDEX ON analytics.monthly_sales_summary (month, industry, region);

-- Advanced window function examples for analytics
CREATE MATERIALIZED VIEW analytics.customer_cohort_analysis AS
WITH customer_first_order AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date,
        DATE_TRUNC('month', MIN(order_date)) as cohort_month
    FROM orders
    GROUP BY customer_id
),
customer_orders AS (
    SELECT 
        o.customer_id,
        o.order_date,
        o.total_amount,
        cfo.cohort_month,
        DATE_PART('month', AGE(o.order_date, cfo.first_order_date)) as months_since_first_order
    FROM orders o
    JOIN customer_first_order cfo ON o.customer_id = cfo.customer_id
)
SELECT 
    cohort_month,
    months_since_first_order,
    COUNT(DISTINCT customer_id) as customers,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value,
    -- Advanced analytics: Customer retention calculation
    COUNT(DISTINCT customer_id)::float / 
        FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month 
            ORDER BY months_since_first_order 
            ROWS UNBOUNDED PRECEDING
        ) as retention_rate
FROM customer_orders
GROUP BY cohort_month, months_since_first_order
ORDER BY cohort_month, months_since_first_order;

-- Complex CTE example: Customer Lifetime Value calculation
CREATE MATERIALIZED VIEW analytics.customer_lifetime_value AS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_code,
        c.industry,
        c.customer_tier,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as total_revenue,
        AVG(o.total_amount) as avg_order_value,
        -- Advanced calculation: Average days between orders
        CASE 
            WHEN COUNT(o.order_id) > 1 THEN
                EXTRACT(DAY FROM (MAX(o.order_date) - MIN(o.order_date))) / (COUNT(o.order_id) - 1)
            ELSE NULL 
        END as avg_days_between_orders
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.is_active = true
    GROUP BY c.customer_id, c.customer_code, c.industry, c.customer_tier
),
customer_predictions AS (
    SELECT 
        *,
        -- Predictive analytics: Estimated future orders
        CASE 
            WHEN avg_days_between_orders IS NOT NULL THEN
                GREATEST(1, ROUND(365.0 / avg_days_between_orders))
            ELSE 1
        END as predicted_annual_orders,
        -- Customer health score (0-100)
        CASE 
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 100
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 75
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 50
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '365 days' THEN 25
            ELSE 0
        END as health_score
    FROM customer_metrics
)
SELECT 
    *,
    -- Final CLV calculation combining historical and predictive data
    (avg_order_value * predicted_annual_orders * 
     CASE customer_tier 
         WHEN 'Premium' THEN 3.0 
         WHEN 'Standard' THEN 2.0 
         ELSE 1.5 
     END * (health_score / 100.0)) as estimated_lifetime_value
FROM customer_predictions;

-- Stored procedure for complex data processing
CREATE OR REPLACE FUNCTION analytics.refresh_all_analytics()
RETURNS void AS $$
BEGIN
    -- Refresh materialized views with dependency handling
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.monthly_sales_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.customer_cohort_analysis;
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.customer_lifetime_value;
    
    -- Update customer analytics table with latest calculations
    INSERT INTO customer_analytics (
        customer_id, analysis_date, total_orders, total_revenue, 
        avg_order_value, customer_lifetime_value, churn_probability
    )
    SELECT 
        clv.customer_id,
        CURRENT_DATE,
        clv.total_orders,
        clv.total_revenue,
        clv.avg_order_value,
        clv.estimated_lifetime_value,
        -- Simple churn prediction based on recency
        CASE 
            WHEN clv.health_score > 75 THEN 0.1
            WHEN clv.health_score > 50 THEN 0.3
            WHEN clv.health_score > 25 THEN 0.6
            ELSE 0.9
        END
    FROM analytics.customer_lifetime_value clv
    ON CONFLICT (customer_id, analysis_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        avg_order_value = EXCLUDED.avg_order_value,
        customer_lifetime_value = EXCLUDED.customer_lifetime_value,
        churn_probability = EXCLUDED.churn_probability;
        
    RAISE NOTICE 'Analytics refresh completed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Advanced trigger for real-time analytics updates
CREATE OR REPLACE FUNCTION update_customer_analytics_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Update customer analytics in real-time when orders change
    UPDATE customer_analytics 
    SET 
        total_orders = (
            SELECT COUNT(*) 
            FROM orders 
            WHERE customer_id = NEW.customer_id
        ),
        total_revenue = (
            SELECT COALESCE(SUM(total_amount), 0) 
            FROM orders 
            WHERE customer_id = NEW.customer_id
        ),
        avg_order_value = (
            SELECT COALESCE(AVG(total_amount), 0) 
            FROM orders 
            WHERE customer_id = NEW.customer_id
        )
    WHERE customer_id = NEW.customer_id 
    AND analysis_date = CURRENT_DATE;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for real-time updates
CREATE TRIGGER orders_analytics_update 
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_customer_analytics_trigger();

-- Performance monitoring views
CREATE VIEW monitoring.slow_queries AS
SELECT 
    query_hash,
    query_text,
    AVG(execution_time_ms) as avg_execution_time,
    COUNT(*) as execution_count,
    MAX(execution_time_ms) as max_execution_time,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen
FROM query_performance_log
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY query_hash, query_text
HAVING AVG(execution_time_ms) > 1000
ORDER BY avg_execution_time DESC;

-- Table statistics for optimization recommendations
CREATE VIEW monitoring.table_statistics AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
