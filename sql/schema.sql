-- Create dimension tables first
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_district (
    district_id VARCHAR(50) PRIMARY KEY,
    district_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_upazila (
    upazila_id VARCHAR(50) PRIMARY KEY,
    upazila_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_union (
    union_id VARCHAR(50) PRIMARY KEY,
    union_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_item (
    item_code VARCHAR(50) PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fact tables
CREATE TABLE IF NOT EXISTS fact_inventory (
    id BIGSERIAL PRIMARY KEY,
    year VARCHAR(4) NOT NULL,
    month VARCHAR(2) NOT NULL,
    warehouse_id VARCHAR(50) REFERENCES dim_warehouse(warehouse_id),
    district_id VARCHAR(50) REFERENCES dim_district(district_id),
    upazila_id VARCHAR(50) REFERENCES dim_upazila(upazila_id),
    union_id VARCHAR(50) REFERENCES dim_union(union_id),
    item_code VARCHAR(50) REFERENCES dim_item(item_code),
    facility_name VARCHAR(255) NOT NULL,
    opening_balance NUMERIC(20, 2),
    received NUMERIC(20, 2),
    total NUMERIC(20, 2),
    adj_plus NUMERIC(20, 2),
    adj_minus NUMERIC(20, 2),
    grand_total NUMERIC(20, 2),
    distribution NUMERIC(20, 2),
    closing_balance NUMERIC(20, 2),
    stock_out_reason TEXT,
    stock_out_days INTEGER,
    eligible BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, warehouse_id, district_id, upazila_id, union_id, item_code, facility_name)
);

-- Create scraping progress tracking table
CREATE TABLE IF NOT EXISTS scraping_progress (
    id BIGSERIAL PRIMARY KEY,
    year VARCHAR(4) NOT NULL,
    month VARCHAR(2) NOT NULL,
    warehouse_id VARCHAR(50) NOT NULL,
    district_id VARCHAR(50) NOT NULL,
    upazila_id VARCHAR(50) NOT NULL,
    union_id VARCHAR(50) NOT NULL,
    item_code VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, warehouse_id, district_id, upazila_id, union_id, item_code)
);

-- Create data validation errors table
CREATE TABLE IF NOT EXISTS validation_errors (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(255) NOT NULL,
    error_type VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_fact_inventory_updated_at
    BEFORE UPDATE ON fact_inventory
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_scraping_progress_updated_at
    BEFORE UPDATE ON scraping_progress
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_inventory_date ON fact_inventory(year, month);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_location ON fact_inventory(warehouse_id, district_id, upazila_id, union_id);
CREATE INDEX IF NOT EXISTS idx_scraping_progress_status ON scraping_progress(status);
CREATE INDEX IF NOT EXISTS idx_validation_errors_type ON validation_errors(error_type);