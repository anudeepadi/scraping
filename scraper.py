import asyncio
import aiohttp
import json
import logging
from datetime import datetime
from pathlib import Path
import time
from typing import Dict, List, Optional
import random
from dataclasses import dataclass
import yaml
from tqdm import tqdm
import os
import pandas as pd

# Create necessary directories first
for dir_name in ['data', 'logs', 'metadata']:
    os.makedirs(dir_name, exist_ok=True)

# Configure logging after directories are created
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingTask:
    year: str
    month: str
    upazila_id: str
    item_code: str
    retry_count: int = 0
    
    def get_filename(self) -> str:
        """Generate filename for saving data"""
        return f"data/{self.year}_{self.month}_{self.upazila_id}_{self.item_code}.json"
    
    def __str__(self):
        return f"{self.year}-{self.month} {self.upazila_id} {self.item_code}"

class ProgressTracker:
    def __init__(self, progress_file="metadata/progress.json"):
        self.progress_file = progress_file
        self.completed_tasks = self._load_progress()
        self.errors = {}

    def _load_progress(self) -> set:
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                return set(data.get('completed_tasks', []))
            return set()
        except Exception as e:
            logger.error(f"Error loading progress: {str(e)}")
            return set()

    def save_progress(self):
        try:
            with open(self.progress_file, 'w') as f:
                json.dump({
                    'completed_tasks': list(self.completed_tasks),
                    'errors': self.errors,
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {str(e)}")

    def is_completed(self, task: ScrapingTask) -> bool:
        task_key = f"{task.year}_{task.month}_{task.upazila_id}_{task.item_code}"
        return task_key in self.completed_tasks

    def mark_completed(self, task: ScrapingTask):
        task_key = f"{task.year}_{task.month}_{task.upazila_id}_{task.item_code}"
        self.completed_tasks.add(task_key)
        if task_key in self.errors:
            del self.errors[task_key]
        self.save_progress()

    def mark_error(self, task: ScrapingTask, error: str):
        task_key = f"{task.year}_{task.month}_{task.upazila_id}_{task.item_code}"
        self.errors[task_key] = {
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
        self.save_progress()

class LMISScraper:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
        
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        self.session = None
        self.progress_tracker = ProgressTracker()
        self.error_count = 0
        self.success_count = 0
    
    def _load_config(self, config_path: str) -> dict:
        default_config = {
            'api': {
                'base_url': "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
            },
            'scraping': {
                'batch_size': 1,  # Process one at a time
                'requests_per_second': 0.2,  # 5 seconds between requests
                'retry_delay': 10,
                'max_retries': 3
            }
        }
        
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Could not load config file: {str(e)}. Using defaults.")
            return default_config

    async def try_parse_json(self, text: str) -> Optional[dict]:
        """Try to parse JSON from text response"""
        try:
            # Clean the text - remove any leading/trailing whitespace
            text = text.strip()
            if text.startswith('{') and text.endswith('}'):
                return json.loads(text)
            return None
        except json.JSONDecodeError:
            return None
    
    async def setup_session(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers,
                cookie_jar=aiohttp.CookieJar()
            )
    
    async def fetch_data(self, task: ScrapingTask) -> dict:
        url = f"{self.config['api']['base_url']}/form2_view_datasource.php"
        data = {
            "sEcho": "2",
            "iColumns": "13",
            "sColumns": "",
            "iDisplayStart": "0",
            "iDisplayLength": "-1",
            "operation": "getItemlist",
            "Year": task.year,
            "Month": task.month,
            "Item": task.item_code,
            "UPNameList": task.upazila_id,
            "UnionList": "1",
            "WHListAll": "All",
            "DistrictList": "All",
            "baseURL": "https://scmpbd.org/scip/"
        }
        
        for i in range(13):
            data[f"mDataProp_{i}"] = str(i)
        
        max_retries = self.config['scraping'].get('max_retries', 3)
        retry_delay = self.config['scraping'].get('retry_delay', 10)
        
        for retry in range(max_retries):
            try:
                async with self.session.post(url, data=data) as response:
                    if response.status == 200:
                        text = await response.text()
                        json_data = await self.try_parse_json(text)
                        
                        if json_data and 'aaData' in json_data:
                            return json_data
                        else:
                            raise ValueError(f"Invalid response format: {text[:200]}...")
                    else:
                        raise aiohttp.ClientError(f"HTTP {response.status}")
                        
            except Exception as e:
                if retry < max_retries - 1:
                    delay = retry_delay * (retry + 1)
                    logger.warning(f"Retry {retry + 1}/{max_retries} for task {task} after {delay}s. Error: {str(e)}")
                    await asyncio.sleep(delay)
                else:
                    raise
    
    def process_data(self, raw_data: dict) -> List[Dict]:
        if not isinstance(raw_data, dict) or 'aaData' not in raw_data:
            return []
            
        processed_data = []
        for row in raw_data['aaData'][:-1]:  # Skip last row (total)
            try:
                processed_row = {
                    "facility": row[1],
                    "opening_balance": self.parse_number(row[2]),
                    "received": self.parse_number(row[3]),
                    "total": self.parse_number(row[4]),
                    "adj_plus": self.parse_number(row[5]),
                    "adj_minus": self.parse_number(row[6]),
                    "grand_total": self.parse_number(row[7]),
                    "distribution": self.parse_number(row[8]),
                    "closing_balance": self.parse_number(row[9]),
                    "stock_out_reason": row[10],
                    "stock_out_days": self.parse_number(row[11]),
                    "eligible": True if "tick" in str(row[12]) else False
                }
                processed_data.append(processed_row)
            except Exception as e:
                logger.error(f"Error processing row {row}: {str(e)}")
                continue
                
        return processed_data
    
    @staticmethod
    def parse_number(value: str) -> float:
        try:
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value).replace(',', '')) if value else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    async def process_task(self, task: ScrapingTask):
        if self.progress_tracker.is_completed(task):
            return True

        try:
            raw_data = await self.fetch_data(task)
            processed_data = self.process_data(raw_data)
            
            if processed_data:  # Only save if we have data
                output_path = task.get_filename()
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        "metadata": {
                            "year": task.year,
                            "month": task.month,
                            "upazila_id": task.upazila_id,
                            "item_code": task.item_code,
                            "scraped_at": datetime.now().isoformat()
                        },
                        "data": processed_data
                    }, f, indent=2, ensure_ascii=False)
                
                self.progress_tracker.mark_completed(task)
                self.success_count += 1
                if self.success_count % 10 == 0:
                    logger.info(f"Successfully completed {self.success_count} tasks")
                return True
            else:
                raise ValueError("No data processed")
            
        except Exception as e:
            self.error_count += 1
            error_msg = f"Error processing task {task}: {str(e)}"
            logger.error(error_msg)
            self.progress_tracker.mark_error(task, error_msg)
            return False
    
    async def run(self):
        await self.setup_session()
        
        try:
            if not os.path.exists("metadata/all_combinations.csv"):
                logger.error("metadata/all_combinations.csv not found! Run metadata_collector.py first.")
                return
                
            tasks_df = pd.read_csv("metadata/all_combinations.csv")
            total_tasks = len(tasks_df)
            
            tasks = []
            for _, row in tasks_df.iterrows():
                task = ScrapingTask(
                    year=str(row['year']),
                    month=str(row['month']).zfill(2),
                    upazila_id=row['upazila_id'],
                    item_code=row['item_code']
                )
                
                if not self.progress_tracker.is_completed(task):
                    tasks.append(task)
            
            remaining_tasks = len(tasks)
            logger.info(f"Total tasks: {total_tasks}, Remaining tasks: {remaining_tasks}")
            
            if remaining_tasks == 0:
                logger.info("All tasks completed!")
                return
            
            # Process tasks one at a time with fixed delay
            delay = 1.0 / self.config['scraping'].get('requests_per_second', 0.2)
            
            with tqdm(total=remaining_tasks, desc="Processing tasks") as pbar:
                for task in tasks:
                    try:
                        success = await self.process_task(task)
                        if success:
                            pbar.update(1)
                        
                        # Fixed delay between requests
                        await asyncio.sleep(delay)
                        
                    except asyncio.CancelledError:
                        logger.info("Scraping interrupted. Progress saved.")
                        break
                    except Exception as e:
                        logger.error(f"Error processing task {task}: {str(e)}")
                        continue
            
            logger.info(f"Scraping completed. Successes: {self.success_count}, Errors: {self.error_count}")
            
        except Exception as e:
            logger.error(f"Error in main scraping loop: {str(e)}")
            raise
        finally:
            if self.session:
                await self.session.close()

async def main():
    try:
        scraper = LMISScraper()
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user. Progress has been saved.")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())