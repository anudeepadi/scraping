import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
        self.conn = None
        self.setup_database()
        
    def _load_config(self, config_path: str) -> dict:
        try:
            import yaml
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return {
                'database': {
                    'host': 'localhost',
                    'port': 5432,
                    'dbname': 'lmis_data',
                    'user': 'postgres',
                    'password': 'postgres'
                }
            }
    
    def get_connection(self):
        """Get database connection with automatic reconnection"""
        try:
            if self.conn is None or self.conn.closed:
                self.conn = psycopg2.connect(
                    host=self.config['database']['host'],
                    port=self.config['database']['port'],
                    dbname=self.config['database']['dbname'],
                    user=self.config['database']['user'],
                    password=self.config['database']['password']
                )
            return self.conn
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def close_connection(self):
        """Close database connection"""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def setup_database(self):
        """Setup database schema"""
        try:
            schema_path = Path('sql/schema.sql')
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
                
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            conn.commit()
            logger.info("Database schema setup completed")
            
        except Exception as e:
            logger.error(f"Database setup error: {str(e)}")
            raise

    def initialize_dimensions(self):
        """Initialize dimension tables from metadata"""
        try:
            with open('metadata/location_hierarchy.json', 'r') as f:
                hierarchy = json.load(f)
            
            with open('metadata/item_mappings.json', 'r') as f:
                items = json.load(f)
            
            conn = self.get_connection()
            with conn.cursor() as cur:
                # Insert warehouses
                warehouses = list(set([
                    (loc['warehouse_id'], loc['warehouse_name'])
                    for loc in hierarchy['locations']
                ]))
                
                execute_values(cur,
                    "INSERT INTO dim_warehouse (warehouse_id, warehouse_name) VALUES %s ON CONFLICT DO NOTHING",
                    warehouses
                )
                
                # Insert districts
                districts = list(set([
                    (loc['district_id'], loc['district_name'])
                    for loc in hierarchy['locations']
                ]))
                
                execute_values(cur,
                    "INSERT INTO dim_district (district_id, district_name) VALUES %s ON CONFLICT DO NOTHING",
                    districts
                )
                
                # Insert upazilas
                upazilas = list(set([
                    (loc['upazila_id'], loc['upazila_name'])
                    for loc in hierarchy['locations']
                ]))
                
                execute_values(cur,
                    "INSERT INTO dim_upazila (upazila_id, upazila_name) VALUES %s ON CONFLICT DO NOTHING",
                    upazilas
                )
                
                # Insert unions
                unions = list(set([
                    (loc['union_id'], loc['union_name'])
                    for loc in hierarchy['locations']
                ]))
                
                execute_values(cur,
                    "INSERT INTO dim_union (union_id, union_name) VALUES %s ON CONFLICT DO NOTHING",
                    unions
                )
                
                # Insert items
                item_data = [
                    (item['code'], item['name'])
                    for item in items['items']
                ]
                
                execute_values(cur,
                    "INSERT INTO dim_item (item_code, item_name) VALUES %s ON CONFLICT DO NOTHING",
                    item_data
                )
                
            conn.commit()
            logger.info("Dimension tables initialized")
            
        except Exception as e:
            logger.error(f"Error initializing dimensions: {str(e)}")
            raise

    def initialize_scraping_progress(self):
        """Initialize scraping progress table from metadata"""
        try:
            df = pd.read_csv('metadata/all_combinations.csv')
            
            progress_data = []
            for _, row in df.iterrows():
                progress_data.append((
                    row['year'],
                    row['month'],
                    row['warehouse_id'],
                    row['district_id'],
                    row['upazila_id'],
                    row['union_id'],
                    row['item_code']
                ))
            
            conn = self.get_connection()
            with conn.cursor() as cur:
                execute_values(cur,
                    """
                    INSERT INTO scraping_progress 
                    (year, month, warehouse_id, district_id, upazila_id, union_id, item_code)
                    VALUES %s
                    ON CONFLICT (year, month, warehouse_id, district_id, upazila_id, union_id, item_code) 
                    DO NOTHING
                    """,
                    progress_data
                )
            
            conn.commit()
            logger.info(f"Initialized {len(progress_data)} scraping tasks")
            
        except Exception as e:
            logger.error(f"Error initializing scraping progress: {str(e)}")
            raise

    def get_pending_tasks(self, batch_size: int = 100) -> List[Tuple]:
        """Get pending scraping tasks"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT year, month, warehouse_id, district_id, upazila_id, union_id, item_code
                    FROM scraping_progress
                    WHERE status = 'pending'
                    AND (retry_count < 3)
                    AND (last_attempt IS NULL OR last_attempt < NOW() - INTERVAL '5 minutes')
                    ORDER BY last_attempt ASC NULLS FIRST
                    LIMIT %s
                """, (batch_size,))
                
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Error getting pending tasks: {str(e)}")
            return []

    def save_inventory_data(self, task: Tuple, data: List[Dict], status: str = 'completed'):
        """Save inventory data to database"""
        try:
            year, month, wh_id, dist_id, upz_id, union_id, item_code = task
            
            conn = self.get_connection()
            with conn.cursor() as cur:
                # Update scraping progress
                cur.execute("""
                    UPDATE scraping_progress
                    SET status = %s,
                        retry_count = retry_count + 1,
                        last_attempt = NOW(),
                        updated_at = NOW()
                    WHERE year = %s
                        AND month = %s
                        AND warehouse_id = %s
                        AND district_id = %s
                        AND upazila_id = %s
                        AND union_id = %s
                        AND item_code = %s
                """, (status, year, month, wh_id, dist_id, upz_id, union_id, item_code))
                
                # Save inventory data if successful
                if status == 'completed' and data:
                    inventory_data = []
                    for item in data:
                        inventory_data.append((
                            year, month, wh_id, dist_id, upz_id, union_id, item_code,
                            item['facility'],
                            item['opening_balance'],
                            item['received'],
                            item['total'],
                            item['adj_plus'],
                            item['adj_minus'],
                            item['grand_total'],
                            item['distribution'],
                            item['closing_balance'],
                            item['stock_out_reason'],
                            item['stock_out_days'],
                            item['eligible']
                        ))
                    
                    execute_values(cur,
                        """
                        INSERT INTO fact_inventory (
                            year, month, warehouse_id, district_id, upazila_id, union_id, item_code,
                            facility_name, opening_balance, received, total, adj_plus, adj_minus,
                            grand_total, distribution, closing_balance, stock_out_reason,
                            stock_out_days, eligible
                        ) VALUES %s
                        ON CONFLICT (year, month, warehouse_id, district_id, upazila_id, union_id, 
                                   item_code, facility_name)
                        DO UPDATE SET
                            opening_balance = EXCLUDED.opening_balance,
                            received = EXCLUDED.received,
                            total = EXCLUDED.total,
                            adj_plus = EXCLUDED.adj_plus,
                            adj_minus = EXCLUDED.adj_minus,
                            grand_total = EXCLUDED.grand_total,
                            distribution = EXCLUDED.distribution,
                            closing_balance = EXCLUDED.closing_balance,
                            stock_out_reason = EXCLUDED.stock_out_reason,
                            stock_out_days = EXCLUDED.stock_out_days,
                            eligible = EXCLUDED.eligible,
                            updated_at = NOW()
                        """,
                        inventory_data
                    )
                
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving inventory data: {str(e)}")
            self.log_error('fact_inventory', str(task), 'data_save_error', str(e))
            raise

    def log_error(self, table_name: str, record_id: str, error_type: str, error_message: str):
        """Log validation or processing errors"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO validation_errors 
                    (table_name, record_id, error_type, error_message)
                    VALUES (%s, %s, %s, %s)
                """, (table_name, record_id, error_type, error_message))
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error logging validation error: {str(e)}")

    def validate_data(self, task: Tuple, data: List[Dict]) -> bool:
        """Validate inventory data"""
        year, month, wh_id, dist_id, upz_id, union_id, item_code = task
        record_id = f"{year}_{month}_{wh_id}_{dist_id}_{upz_id}_{union_id}_{item_code}"
        
        try:
            # Basic validation
            if not data:
                self.log_error('fact_inventory', record_id, 'empty_data', 'No data returned')
                return False
            
            for row in data:
                # Check required fields
                required_fields = ['facility', 'opening_balance', 'received', 'total']
                for field in required_fields:
                    if field not in row or row[field] is None:
                        self.log_error('fact_inventory', record_id, 'missing_field', 
                                     f"Missing required field: {field}")
                        return False
                
                # Validate numeric fields
                numeric_fields = ['opening_balance', 'received', 'total', 'adj_plus', 
                                'adj_minus', 'grand_total', 'distribution', 'closing_balance']
                for field in numeric_fields:
                    if field in row and not isinstance(row[field], (int, float)):
                        self.log_error('fact_inventory', record_id, 'invalid_numeric', 
                                     f"Invalid numeric value in {field}: {row[field]}")
                        return False
                
                # Check totals
                calculated_total = row['opening_balance'] + row['received']
                if abs(calculated_total - row['total']) > 0.01:  # Allow small float differences
                    self.log_error('fact_inventory', record_id, 'total_mismatch',
                                 f"Total mismatch: {calculated_total} != {row['total']}")
                    return False
            
            return True
            
        except Exception as e:
            self.log_error('fact_inventory', record_id, 'validation_error', str(e))
            return False

    def get_progress_summary(self) -> Dict:
        """Get scraping progress summary"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        status,
                        COUNT(*) as count,
                        MIN(last_attempt) as oldest_attempt,
                        MAX(last_attempt) as latest_attempt,
                        AVG(retry_count) as avg_retries
                    FROM scraping_progress
                    GROUP BY status
                """)
                
                results = cur.fetchall()
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'status_counts': {},
                    'total_tasks': 0,
                    'completion_percentage': 0
                }
                
                for row in results:
                    status, count, oldest, latest, avg_retries = row
                    summary['status_counts'][status] = {
                        'count': count,
                        'oldest_attempt': oldest.isoformat() if oldest else None,
                        'latest_attempt': latest.isoformat() if latest else None,
                        'avg_retries': float(avg_retries) if avg_retries else 0
                    }
                    summary['total_tasks'] += count
                
                if summary['total_tasks'] > 0:
                    completed = summary['status_counts'].get('completed', {}).get('count', 0)
                    summary['completion_percentage'] = (completed / summary['total_tasks']) * 100
                
                return summary
                
        except Exception as e:
            logger.error(f"Error getting progress summary: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

if __name__ == "__main__":
    # Test database setup and initialization
    db = DatabaseManager()
    db.initialize_dimensions()
    db.initialize_scraping_progress()
    print(db.get_progress_summary())