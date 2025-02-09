# LMIS Data Scraper

This project scrapes data from the LMIS system, handling rate limiting and storing data efficiently.

## Setup

1. Create a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install requirements:
```bash
pip install -r requirements.txt
```

3. Update config.yaml with your settings:
- Adjust rate limiting parameters
- Set database credentials if using SQL Server
- Modify batch sizes if needed

## Usage

1. First, collect metadata:
```bash
python metadata_collector.py
```
This will create:
- List of all upazilas
- Item mappings
- Date ranges
- Summary of combinations to scrape

2. Run the main scraper:
```bash
python scraper.py
```

The scraper will:
- Create necessary directories
- Process tasks in batches
- Handle rate limiting
- Save data as JSON files
- Log progress

## Directory Structure

```
lmis_scraper/
├── config.yaml         # Configuration settings
├── metadata/          # Metadata files
├── data/             # Scraped data files
├── logs/             # Log files
├── metadata_collector.py
├── scraper.py
└── requirements.txt
```

## Features

- Async/await for efficient processing
- Rate limiting to avoid server overload
- Robust error handling
- Progress tracking
- Detailed logging
- Data validation and cleaning
- Resume capability
- Random delays between requests

## Data Format

Each scraped file contains:
```json
{
    "metadata": {
        "year": "2024",
        "month": "01",
        "upazila_id": "T116",
        "item_code": "CON008",
        "scraped_at": "2024-02-08T10:00:00"
    },
    "data": [
        {
            "facility": "Example Facility",
            "opening_balance": 100,
            "received": 50,
            "total": 150,
            ...
        }
    ]
}
```