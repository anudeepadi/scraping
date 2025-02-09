import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple
import random
from dataclasses import dataclass
import yaml
from tqdm import tqdm
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
import psutil
import gzip
import shutil
from database_manager import DatabaseManager

# Setup directories
for dir_name in ['data', 'logs', 'metadata', 'checkpoints', 'archive']:
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

[Previous code remains the same until async def run()]

    async def run(self):
        """Main scraping loop with parallel processing"""
        self.start_time = datetime.now()
        self.running = True
        
        try:
            await self.setup_session()
            
            # Initialize progress tracking
            pending_tasks = self.db.get_pending_tasks()
            total_tasks = len(pending_tasks)
            
            if total_tasks == 0:
                logger.info("No pending tasks found. All done!")
                return
                
            logger.info(f"Starting scraping with {total_tasks} pending tasks")
            
            # Create task queue
            task_queue = asyncio.Queue()
            
            # Create progress bar
            self.progress_bar = tqdm(
                total=total_tasks,
                desc="Scraping Progress",
                unit="tasks"
            )
            
            # Create workers
            workers = []
            num_workers = min(
                self.config['scraping']['num_workers'],
                total_tasks
            )
            
            # Start workers
            for i in range(num_workers):
                worker = asyncio.create_task(self.worker(i, task_queue))
                workers.append(worker)
            
            # Add tasks to queue
            for task_data in pending_tasks:
                task = ScrapingTask(*task_data)
                await task_queue.put(task)
            
            # Add termination signals
            for _ in range(num_workers):
                await task_queue.put(None)
            
            # Monitor progress and resources
            while self.running:
                if task_queue.empty() and self.progress_bar.n >= total_tasks:
                    break
                
                # Update progress
                completed = self.success_count + self.error_count
                self.progress_bar.n = completed
                self.progress_bar.refresh()
                
                # Save checkpoint if needed
                if self.should_checkpoint():
                    self.save_checkpoint()
                
                # Resource monitoring
                if not self.monitor.check_resources():
                    logger.warning("Low resources detected. Pausing...")
                    await asyncio.sleep(60)
                    continue
                
                await asyncio.sleep(1)
            
            # Wait for all workers to complete
            await asyncio.gather(*workers)
            
            # Final checkpoint
            self.save_checkpoint()
            
            # Show summary
            duration = datetime.now() - self.start_time
            logger.info(
                f"\nScraping completed in {duration}:\n"
                f"- Successfully processed: {self.success_count}\n"
                f"- Errors: {self.error_count}\n"
                f"- Average rate: {self.success_count / duration.total_seconds():.2f} tasks/second\n"
                f"- Resource usage peak: {psutil.Process().memory_info().rss / 1024 / 1024:.1f} MB"
            )
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            raise
            
        finally:
            # Cleanup
            if self.session:
                await self.session.close()
            if self.progress_bar:
                self.progress_bar.close()
            self.db.close_connection()

    def archive_completed_data(self):
        """Archive completed data files older than retention period"""
        try:
            retention_days = self.config['storage']['retention_days']
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            data_dir = Path('data')
            archive_dir = Path('archive')
            archive_dir.mkdir(exist_ok=True)
            
            for file_path in data_dir.glob('*.json'):
                if file_path.stat().st_mtime < cutoff_date.timestamp():
                    archive_path = archive_dir / f"{file_path.stem}_{datetime.now().strftime('%Y%m%d')}.json.gz"
                    
                    with open(file_path, 'rb') as f_in:
                        with gzip.open(archive_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    file_path.unlink()  # Remove original file
                    logger.debug(f"Archived {file_path} to {archive_path}")
            
        except Exception as e:
            logger.error(f"Error archiving data: {str(e)}")

    def cleanup_old_checkpoints(self):
        """Clean up old checkpoint files"""
        try:
            checkpoint_dir = Path('checkpoints')
            checkpoints = sorted(checkpoint_dir.glob('checkpoint_*.json'))
            
            # Keep only last 5 checkpoints
            if len(checkpoints) > 5:
                for old_checkpoint in checkpoints[:-5]:
                    old_checkpoint.unlink()
                    logger.debug(f"Removed old checkpoint: {old_checkpoint}")
                    
        except Exception as e:
            logger.error(f"Error cleaning checkpoints: {str(e)}")

async def main():
    try:
        # Initialize database and metadata
        db = DatabaseManager()
        db.initialize_dimensions()
        db.initialize_scraping_progress()
        
        # Start scraping
        scraper = LMISParallelScraper()
        await scraper.run()
        
        # Cleanup
        scraper.archive_completed_data()
        scraper.cleanup_old_checkpoints()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise
    finally:
        logger.info("Scraping process completed")

if __name__ == "__main__":
    asyncio.run(main())