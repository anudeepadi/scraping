import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Dict, List, Optional, Set
import random
from dataclasses import dataclass
import yaml
from tqdm import tqdm
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import multiprocessing
from queue import Queue
import threading

# Create necessary directories first
for dir_name in ['data', 'logs', 'metadata', 'checkpoints']:
    os.makedirs(dir_name, exist_ok=True)

# Configure logging
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
        return f"data/{self.year}_{self.month}_{self.upazila_id}_{self.item_code}.json"
    
    def __str__(self):
        return f"{self.year}-{self.month} {self.upazila_id} {self.item_code}"

class CheckpointManager:
    def __init__(self, checkpoint_dir="checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        self.last_checkpoint = datetime.now()
        self.checkpoint_interval = timedelta(hours=1)
        
    def should_checkpoint(self) -> bool:
        return datetime.now() - self.last_checkpoint >= self.checkpoint_interval
        
    def save_checkpoint(self, completed_tasks: Set[str], current_task: Optional[ScrapingTask] = None):
        try:
            checkpoint_file = f"{self.checkpoint_dir}/checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(checkpoint_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'completed_tasks': list(completed_tasks),
                    'current_task': str(current_task) if current_task else None
                }, f, indent=2)
            
            # Keep only last 5 checkpoints
            checkpoints = sorted(Path(self.checkpoint_dir).glob('checkpoint_*.json'))
            if len(checkpoints) > 5:
                for old_checkpoint in checkpoints[:-5]:
                    old_checkpoint.unlink()
                    
            self.last_checkpoint = datetime.now()
            logger.info(f"Checkpoint saved: {checkpoint_file}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")

    def load_latest_checkpoint(self) -> Set[str]:
        try:
            checkpoints = sorted(Path(self.checkpoint_dir).glob('checkpoint_*.json'))
            if not checkpoints:
                return set()
                
            latest_checkpoint = checkpoints[-1]
            with open(latest_checkpoint, 'r') as f:
                data = json.load(f)
                return set(data.get('completed_tasks', []))
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            return set()

class ParallelScraper:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        # Initialize managers
        self.checkpoint_manager = CheckpointManager()
        self.completed_tasks = self.checkpoint_manager.load_latest_checkpoint()
        
        # Performance settings
        self.num_workers = min(32, multiprocessing.cpu_count() * 2)  # Optimized for i9
        self.batch_size = 100
        self.request_delay = 0.1  # 100ms between requests
        
        # Statistics
        self.success_count = 0
        self.error_count = 0
        self.start_time = None
        
        # Progress tracking
        self.progress_queue = Queue()
        self.progress_lock = threading.Lock()
        
    def _load_config(self, config_path: str) -> dict:
        default_config = {
            'api': {
                'base_url': "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
            },
            'scraping': {
                'batch_size': 100,
                'num_workers': 32,
                'request_delay': 0.1
            }
        }
        
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Using default config: {str(e)}")
            return default_config

    async def process_task(self, session: aiohttp.ClientSession, task: ScrapingTask) -> bool:
        if str(task) in self.completed_tasks:
            return True

        try:
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

            async with session.post(url, data=data) as response:
                if response.status == 200:
                    text = await response.text()
                    try:
                        raw_data = json.loads(text)
                        processed_data = []
                        
                        if 'aaData' in raw_data:
                            for row in raw_data['aaData'][:-1]:
                                processed_row = {
                                    "facility": row[1],
                                    "opening_balance": self._parse_number(row[2]),
                                    "received": self._parse_number(row[3]),
                                    "total": self._parse_number(row[4]),
                                    "adj_plus": self._parse_number(row[5]),
                                    "adj_minus": self._parse_number(row[6]),
                                    "grand_total": self._parse_number(row[7]),
                                    "distribution": self._parse_number(row[8]),
                                    "closing_balance": self._parse_number(row[9]),
                                    "stock_out_reason": row[10],
                                    "stock_out_days": self._parse_number(row[11]),
                                    "eligible": True if "tick" in str(row[12]) else False
                                }
                                processed_data.append(processed_row)
                        
                        # Save data even if empty (valid state for some periods)
                        output_path = task.get_filename()
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        
                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump({
                                "metadata": {
                                    "year": task.year,
                                    "month": task.month,
                                    "upazila_id": task.upazila_id,
                                    "item_code": task.item_code,
                                    "scraped_at": datetime.now().isoformat(),
                                    "has_data": len(processed_data) > 0
                                },
                                "data": processed_data
                            }, f, indent=2, ensure_ascii=False)
                        
                        with self.progress_lock:
                            self.completed_tasks.add(str(task))
                            self.success_count += 1
                            self.progress_queue.put(1)
                        
                        return True
                        
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON for task {task}: {text[:200]}...")
                        return False
                else:
                    logger.error(f"HTTP {response.status} for task {task}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error processing task {task}: {str(e)}")
            return False

    @staticmethod
    def _parse_number(value: str) -> float:
        try:
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value).replace(',', '')) if value else 0.0
        except (ValueError, TypeError):
            return 0.0

    async def process_batch(self, session: aiohttp.ClientSession, tasks: List[ScrapingTask]):
        for task in tasks:
            await asyncio.sleep(self.request_delay)
            await self.process_task(session, task)

    async def worker(self, worker_id: int, task_queue: asyncio.Queue):
        """Worker process to handle tasks"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            while True:
                try:
                    batch = await task_queue.get()
                    if batch is None:
                        break
                    
                    await self.process_batch(session, batch)
                    task_queue.task_done()
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {str(e)}")
                    continue

    def update_progress(self, total_tasks: int, pbar: tqdm):
        """Update progress bar and save checkpoints"""
        last_count = 0
        
        while True:
            try:
                count = self.progress_queue.get()
                if count is None:
                    break
                    
                pbar.update(count)
                last_count = count
                
                # Save checkpoint every hour
                if self.checkpoint_manager.should_checkpoint():
                    self.checkpoint_manager.save_checkpoint(self.completed_tasks)
                    
            except Exception as e:
                logger.error(f"Progress update error: {str(e)}")
                continue

    async def run(self):
        """Main scraping loop with parallel processing"""
        self.start_time = datetime.now()
        
        # Load tasks
        if not os.path.exists("metadata/all_combinations.csv"):
            logger.error("Run metadata_collector.py first!")
            return
            
        tasks_df = pd.read_csv("metadata/all_combinations.csv")
        all_tasks = []
        
        for _, row in tasks_df.iterrows():
            task = ScrapingTask(
                year=str(row['year']),
                month=str(row['month']).zfill(2),
                upazila_id=row['upazila_id'],
                item_code=row['item_code']
            )
            if str(task) not in self.completed_tasks:
                all_tasks.append(task)

        total_tasks = len(all_tasks)
        remaining_tasks = len(all_tasks)
        logger.info(f"Total tasks: {total_tasks}, Remaining: {remaining_tasks}")
        
        if remaining_tasks == 0:
            logger.info("All tasks completed!")
            return

        # Create task queue and workers
        task_queue = asyncio.Queue()
        workers = []
        
        # Split tasks into batches
        batches = [all_tasks[i:i + self.batch_size] 
                  for i in range(0, len(all_tasks), self.batch_size)]
        
        # Add batches to queue
        for batch in batches:
            await task_queue.put(batch)
            
        # Add worker termination signals
        for _ in range(self.num_workers):
            await task_queue.put(None)

        # Start progress tracking
        pbar = tqdm(total=remaining_tasks, desc="Processing tasks")
        progress_thread = threading.Thread(
            target=self.update_progress,
            args=(total_tasks, pbar)
        )
        progress_thread.start()

        # Start workers
        workers = [
            asyncio.create_task(self.worker(i, task_queue))
            for i in range(self.num_workers)
        ]
        
        try:
            # Wait for all tasks to complete
            await asyncio.gather(*workers)
            
            # Stop progress tracking
            self.progress_queue.put(None)
            progress_thread.join()
            
            # Final checkpoint
            self.checkpoint_manager.save_checkpoint(self.completed_tasks)
            
            # Show summary
            duration = datetime.now() - self.start_time
            logger.info(
                f"\nScraping completed in {duration}.\n"
                f"Successful: {self.success_count}\n"
                f"Failed: {self.error_count}\n"
                f"Average rate: {self.success_count / duration.total_seconds():.2f} tasks/second"
            )
            
        except KeyboardInterrupt:
            logger.info("\nScraping interrupted. Saving progress...")
            self.checkpoint_manager.save_checkpoint(self.completed_tasks)
            
        finally:
            pbar.close()

async def main():
    try:
        scraper = ParallelScraper()
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user. Progress saved.")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())