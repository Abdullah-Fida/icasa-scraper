"""
Configuration for iCasa.ch Real Estate Scraper
"""

# ============================================================
# PORTAL SETTINGS
# ============================================================
PORTAL_NAME = "icasa.ch"
PORTAL_ID = "7"
VENDOR_ID = "12"

# Set to True to bypass agency filter for demo/testing purposes
START_ID = 200000
# Enabled temporarily for demo run
DEMO_MODE = False

# ID Persistence (shared across all scripts)
ID_PERSISTENCE_FILE = "last_id.txt"
ID_RANGE_START = 200000
ID_RANGE_END = 300000

BASE_URL = "https://www.icasa.ch"

# Listing pages
BUY_URL = f"{BASE_URL}/kaufangebote"
RENT_URL = f"{BASE_URL}/mietangebote"

# Pagination
# Production mode: scrape all pages
MAX_PAGES = 5000

# ============================================================
# REQUEST SETTINGS
# ============================================================
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'de-CH,de;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Connection': 'keep-alive',
}

REQUEST_TIMEOUT = 20  # seconds
# Reduced delays for demo/testing (speed up runs)
DELAY_BETWEEN_REQUESTS = 0.1  # seconds
DELAY_BETWEEN_PAGES = 0.1  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ============================================================
# FILTERING RULES
# ============================================================

# Minimum monthly rent in CHF to include rental listings
MIN_RENT_CHF = 3000

# Keywords that indicate a listing is from an AGENCY (not private person)
# If the advertiser name contains ANY of these -> SKIP the listing
AGENCY_KEYWORDS = [
    # German company forms
    'gmbh', 'ag', 'sa', 'sarl', 's.a.', 's.à r.l.', 'sàrl',
    'gesellschaft', 'holding', 'treuhand', 'verwaltung',
    # Real estate industry terms
    'immobilien', 'immobilier', 'immobiliare', 'real estate',
    'makler', 'broker', 'courtier', 'agentur', 'agency', 'agence',
    'vermittlung', 'beratung', 'consulting',
    'treuhand', 'verwaltung', 'management',
    'immo', 'homes', 'wincasa', 'dom', 'invest', 'properties', 'realestate', 'bau', 'architektur',
    # Company/branding indicators  
    'group', 'gruppe', 'partner', 'associates',
    'corp', 'inc', 'ltd', 'limited',
    # Swiss real estate specific
    'casaone', 'casasoft', 'casatour',
    'neubau', 'projekt',
    'büro', 'office', 'bureau',
    'bauherr', 'promoteur', 'promotore',
    'régie', 'regieimmobilien',
]

# Keywords that indicate a PRIVATE listing
PRIVATE_KEYWORDS = [
    'privat', 'private', 'particulier', 'privato',
    'eigentümer', 'propriétaire', 'proprietario',
    'owner', 'besitzer',
]

# ============================================================
# OUTPUT SETTINGS
# ============================================================
OUTPUT_DIR = "output"
OBJEKTE_FILENAME = "Objekte.csv"
KONTAKTE_FILENAME = "Kontakte.csv"
LOG_FILENAME = "scraper.log"

# CSV encoding
CSV_ENCODING = "utf-8-sig"  # UTF-8 with BOM for Excel compatibility
CSV_DELIMITER = ","

# ============================================================
# OBJEKTE.CSV COLUMNS (Property Listings)
# ============================================================
OBJEKTE_COLUMNS = [
    'external_id',
    'contact_external_id',
    'portal_id',
    'vendor_id',
    'type_id',
    'detail_url',
    'title',
    'description',
    'street',
    'house_number',
    'zip_code',
    'city',
    'latitude',
    'longitude',
    'price',
    'living_space_area',
    'land_area',
    'rs_category_id',
    'price_value',
    'advertiser_id',
]

# ============================================================
# KONTAKTE.CSV COLUMNS (Contacts)
# ============================================================
KONTAKTE_COLUMNS = [
    'external_id',
    'first_name',
    'last_name',
    'organization_name',
    'email',
    'phone',
    'street',
    'house_number',
    'zip_code',
    'city',
    'normalized_phone',
    'portal_id',
    'vendor_id',
]
