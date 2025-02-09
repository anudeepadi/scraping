import asyncio
import aiohttp
import json
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import time
from typing import Dict, List
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LMISMetadataCollector:
    def __init__(self):
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://elmis.dgfp.gov.bd",
            "Referer": "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php"
        }
        
        self.items = {
            "CON008": "Shukhi",
            "CON010": "Shukhi (3rd Gen)",
            "CON008+CON010": "Oral Pill (Total)",
            "CON009": "Oral Pill Apon",
            "CON002": "Condom",
            "CON006": "Injectables (Vials)",
            "CON001": "AD Syringe (1ML)",
            "CON003": "ECP",
            "MCH021": "Tab. Misoprostol (Dose)",
            "MCH051": "7.1% CHLOROHEXIDINE",
            "MCH012": "MNP(SUSSET)",
            "MCH018": "Iron-Folic Acid (NOS)"
        }
        
        self.hierarchy_cache = {}

    async def initialize_session(self, session: aiohttp.ClientSession):
        """Initialize session by visiting the main page first"""
        try:
            form_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer/form2_view.php"
            async with session.get(form_url) as response:
                await response.text()
                await asyncio.sleep(1)  # Small delay after initialization
        except Exception as e:
            logger.error(f"Error initializing session: {str(e)}")
            raise

    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str, data: Dict, 
                             max_retries: int = 3) -> Dict:
        """Fetch data with retry logic"""
        for retry in range(max_retries):
            try:
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        text = await response.text()
                        if 'html' in response.headers.get('Content-Type', ''):
                            # Try to extract JSON from HTML if possible
                            if text.strip().startswith('{') and text.strip().endswith('}'):
                                return json.loads(text)
                            else:
                                logger.error(f"Received HTML instead of JSON: {text[:200]}...")
                                raise ValueError("Invalid response format")
                        return json.loads(text)
                    await asyncio.sleep(2 ** retry)
            except Exception as e:
                logger.error(f"Error on attempt {retry + 1}: {str(e)}")
                if retry == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** retry)
        return None

    async def get_warehouses(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch list of all warehouses"""
        cache_key = 'warehouses'
        if cache_key in self.hierarchy_cache:
            return self.hierarchy_cache[cache_key]
            
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getWHList",
            "Year": "2025",
            "Month": "01"
        }
        
        result = await self.fetch_with_retry(session, url, data)
        if result:
            self.hierarchy_cache[cache_key] = result
            return result
        return []

    async def save_metadata(self, output_dir="metadata"):
        """Save all metadata required for full scraping"""
        Path(output_dir).mkdir(exist_ok=True)
        
        logger.info("Collecting location hierarchy...")
        hierarchy_data = await self.collect_hierarchy()
        
        if not hierarchy_data:
            raise ValueError("Failed to collect hierarchy data")
        
        # Save item mappings
        with open(f"{output_dir}/item_mappings.json", 'w', encoding='utf-8') as f:
            json.dump({
                "items": [
                    {"code": code, "name": name} 
                    for code, name in self.items.items()
                ]
            }, f, indent=2, ensure_ascii=False)
        logger.info("Saved item mappings")
        
        logger.info("Metadata collection completed!")

async def main():
    try:
        collector = LMISMetadataCollector()
        await collector.save_metadata()
    except Exception as e:
        logger.error(f"Metadata collection failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
