# iCasa.ch Real Estate Scraper
# Web scraper for Swiss real estate listings from icasa.ch

## Requirements
- Python 3.10+
- requests
- beautifulsoup4
- lxml

## Installation
```
pip install requests beautifulsoup4 lxml
```

## Usage
```
python scraper_icasa.py
```

## Output
- `output/Objekte.csv` - Property listings
- `output/Kontakte.csv` - Contact information
- `output/scraper.log` - Detailed log file

## Configuration
Edit `config.py` to change settings like:
- Delay between requests
- Minimum rent threshold
- Agency keywords for filtering
- Output directory
