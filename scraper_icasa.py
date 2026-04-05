"""
iCasa.ch Real Estate Scraper
=============================
Scrapes real estate listings from icasa.ch
- Filters for PRIVATE individuals only (no agencies/brokers)
- Includes all sales, rentals only >= 3000 CHF/month
- Deduplicates by address + contact
- Outputs Objekte.csv and Kontakte.csv

Author: POS Scraper Pipeline
Portal: https://www.icasa.ch
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import os
import time
import logging
from datetime import datetime
from urllib.parse import urljoin
import uuid
import sys
import threading

from config import (
    BASE_URL, BUY_URL, RENT_URL, MAX_PAGES,
    REQUEST_HEADERS, REQUEST_TIMEOUT, DELAY_BETWEEN_REQUESTS,
    DELAY_BETWEEN_PAGES, MAX_RETRIES, RETRY_DELAY,
    MIN_RENT_CHF, AGENCY_KEYWORDS, PRIVATE_KEYWORDS,
    OUTPUT_DIR, OBJEKTE_FILENAME, KONTAKTE_FILENAME, LOG_FILENAME,
    CSV_ENCODING, CSV_DELIMITER,
    OBJEKTE_COLUMNS, KONTAKTE_COLUMNS,
    OBJEKTE_COLUMNS, KONTAKTE_COLUMNS,
    PORTAL_NAME, PORTAL_ID, VENDOR_ID, DEMO_MODE,
    ID_PERSISTENCE_FILE, ID_RANGE_START, ID_RANGE_END,
)


# ============================================================
# LOGGING SETUP
# ============================================================
def setup_logging():
    """Configure logging to both file and console."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, LOG_FILENAME)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(),
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# ============================================================
# HTTP SESSION
# ============================================================
class ScraperSession:
    """HTTP session with retry logic and rate limiting."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self.last_request_time = 0
    
    def get(self, url, delay=None):
        """Make a GET request with rate limiting and retry logic."""
        if delay is None:
            delay = DELAY_BETWEEN_REQUESTS
        
        # Rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < delay:
            time.sleep(delay - elapsed)
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug(f"GET {url} (attempt {attempt})")
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                self.last_request_time = time.time()
                
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited (429). Waiting {RETRY_DELAY * attempt}s...")
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        
            except requests.RequestException as e:
                logger.error(f"Request error for {url}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        
        logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
        return None


# ============================================================
# PHONE NUMBER NORMALIZATION
# ============================================================
def normalize_phone(phone_raw):
    """
    Normalize a Swiss phone number to +41 format.
    Examples:
        076 543 21 00 -> +41765432100
        +41 76 543 21 00 -> +41765432100
        0041 (0)27 455 82 82 -> +41274558282
    """
    if not phone_raw:
        return ''
    
    # Remove all non-digit characters except leading +
    cleaned = phone_raw.strip()
    
    # Replace (0) pattern common in Swiss numbers
    cleaned = re.sub(r'\(0\)', '', cleaned)
    
    # Keep only digits and leading +
    if cleaned.startswith('+'):
        digits = '+' + re.sub(r'[^\d]', '', cleaned[1:])
    else:
        digits = re.sub(r'[^\d]', '', cleaned)
    
    # Normalize to +41 format
    if digits.startswith('0041'):
        digits = '+41' + digits[4:]
    elif digits.startswith('041') and len(digits) > 10:
        digits = '+41' + digits[3:]
    elif digits.startswith('0') and len(digits) >= 10:
        digits = '+41' + digits[1:]
    elif digits.startswith('+41'):
        pass  # Already correct
    elif digits.startswith('41') and len(digits) >= 11:
        digits = '+' + digits
    
    # Validate minimum length
    if len(digits) < 10:
        return ''
    
    return digits


# ============================================================
# DATA EXTRACTION
# ============================================================
def extract_json_ld(soup):
    """Extract and parse JSON-LD data from the page."""
    results = {
        'product': None,
        'residence': None,
        'agent': None,
    }
    
    for script in soup.find_all('script', type='application/ld+json'):
        raw = script.string
        if not raw:
            continue
        
        # Clean up the JSON-LD (icasa wraps in HTML comments)
        raw = raw.strip()
        raw = re.sub(r'^//\s*<!\-\-\s*', '', raw)
        raw = re.sub(r'\s*//\s*\-\->\s*$', '', raw)
        
        # Fix escaped slashes
        raw = raw.replace('\\\\/', '/')
        
        try:
            data = json.loads(raw)
            schema_type = data.get('@type', '')
            
            if schema_type == 'Product':
                results['product'] = data
            elif schema_type == 'Residence':
                results['residence'] = data
            elif schema_type == 'RealEstateAgent':
                results['agent'] = data
                
        except json.JSONDecodeError as e:
            logger.debug(f"JSON-LD parse error: {e}")
    
    return results


def extract_phone_numbers(soup, json_ld=None):
    """Extract all phone numbers from the page."""
    phones = []
    
    # === PRIORITY 0: Extract from JSON-LD (if provided) ===
    if json_ld:
        # Check standard Schema.org paths
        for key in ['telephone', 'phone', 'contactPoint']:
            val = json_ld.get(key)
            if isinstance(val, str) and val.strip():
                phones.append(val)
            elif isinstance(val, dict):
                p = val.get('telephone') or val.get('phone')
                if p: phones.append(p)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, str): phones.append(item)
                    elif isinstance(item, dict):
                        p = item.get('telephone') or item.get('phone')
                        if p: phones.append(p)

    # === PRIORITY 1: Extract from behind-sticker spans (icasa-specific) ===
    for span in soup.find_all('span', class_='behind-sticker'):
        phone = span.get_text(strip=True)
        if phone and re.search(r'\d{2,}', phone):
            phones.append(phone)
    
    # === PRIORITY 2: Extract from tel: links and data attributes ===
    for a in soup.find_all(['a', 'button', 'div', 'span']):
        # tel: links
        if a.name == 'a' and a.get('href', '').startswith('tel:'):
            phones.append(a['href'].replace('tel:', '').strip())
        
        # data attributes (common in modern SPAs)
        for attr in ['data-phone', 'data-telephone', 'data-tel', 'data-call']:
            val = a.get(attr)
            if val and len(val) > 5:
                phones.append(val)

    # === PRIORITY 3: Extract from description text ===
    desc_section = soup.find(id='singleDescription')
    if desc_section:
        desc_text = desc_section.get_text()
        patterns = [
            r'\+41\s?\d{2}\s?\d{3}\s?\d{2}\s?\d{2}',
            r'0041\s?\(0\)\d{2}\s?\d{3}\s?\d{2}\s?\d{2}',
            r'0\d{2}\s?\d{3}\s?\d{2}\s?\d{2}',
            r'\+41\s?\d{9,10}',
            r'0\d{9,10}',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, desc_text)
            phones.extend(matches)
    
    # === PRIORITY 4: Aggressive regex on provider box ===
    provider_box = soup.find(class_='single__providerbox')
    if provider_box:
        provider_text = provider_box.get_text(' ', strip=True)
        # More flexible regex for provider box numbers
        patterns = [
            r'\+?\d{2,3}[\s\-()]?\d{2,3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}', # +41 79 123 45 67
            r'0\d{2}[\s\-()]?\d{3}[\s\-()]?\d{2}[\s\-()]?\d{2}',                     # 079 123 45 67
            r'\+?\d{9,12}',                                                         # 41791234567
        ]
        for pattern in patterns:
            matches = re.findall(pattern, provider_text)
            phones.extend(matches)
    
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for p in phones:
        normalized = normalize_phone(p)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(p)
    
    return unique


def extract_email_addresses(soup):
    """Extract email addresses from the page."""
    emails = []
    
    # From mailto links
    for a in soup.find_all('a', href=re.compile(r'^mailto:')):
        email = a['href'].replace('mailto:', '').split('?')[0].strip()
        if email:
            emails.append(email)
    
    # From page text (but exclude common false positives)
    text = str(soup)
    found = re.findall(r'[\w.+-]+@[\w-]+\.[\w.]+', text)
    for email in found:
        if email not in emails and not any(x in email for x in ['casasoft', 'icasa.ch', 'example']):
            emails.append(email)
    
    return list(set(emails))


def parse_price(price_text):
    """
    Parse price from text like:
    - "CHF 1'120'000.-"
    - "CHF 1'900.- / Monat"
    - "Auf Anfrage"
    Returns (price_float, price_unit) or (None, None)
    """
    if not price_text or 'anfrage' in price_text.lower():
        return None, None
    
    # Remove Swiss thousand separators (both apostrophe types) and trailing dashes
    cleaned = price_text.replace("\u2019", "").replace("'", "").replace(",", "").replace(".-", "")
    # Extract the first integer/decimal number
    match = re.search(r'(\d+(?:\.\d+)?)', cleaned)
    
    if not match:
        return None, None
    
    price = float(match.group(1))
    # Sanity check: a price of 3.5 is clearly rooms, not money
    if price < 100:
        return None, None
    
    # Determine unit
    if 'monat' in price_text.lower() or '/ m' in price_text.lower():
        unit = 'CHF/month'
    elif 'm2' in price_text.lower() or 'm\u00b2' in price_text.lower():
        unit = 'CHF/m2/month'
    else:
        unit = 'CHF'
    
    return price, unit


def is_agency(advertiser_name):
    """
    Determine if the advertiser is an agency or company.
    Returns True if it's an agency (should be filtered out).
    """
    if not advertiser_name:
        return True  # No name = cannot verify, skip
    
    name_lower = advertiser_name.lower().strip()
    
    # Check for private keywords
    for kw in PRIVATE_KEYWORDS:
        if kw in name_lower:
            return False  # Definitely private
    
    # Check for agency keywords
    for kw in AGENCY_KEYWORDS:
        if kw in name_lower:
            return True  # It's an agency
    
    # If name looks like a person's name (First Last format), consider it private
    parts = advertiser_name.strip().split()
    if len(parts) == 2 and all(p[0].isupper() and p[1:].islower() for p in parts if len(p) > 1):
        return False  # Looks like a person's name
    
    # Default: on icasa.ch, most listings are from professionals
    # If no clear indicator, assume it's an agency to be safe
    return True

def clean_text(text):
    """Clean encoding issues and normalize whitespace.
    
    IMPORTANT: Do NOT re-encode text (latin1→utf8) as this corrupts
    proper Unicode characters like apostrophes (') and fractions (½).
    BeautifulSoup already handles encoding correctly, but some pages
    inject Windows-1252 characters (0x80-0x9F range) that need mapping.
    """
    if not text:
        return ''
    # Fix Windows-1252 characters that sneak through (0x80-0x9F range)
    # These are the most common encoding issues with European websites
    cp1252_map = {
        '\x80': '€', '\x82': '‚', '\x83': 'ƒ', '\x84': '„',
        '\x85': '…', '\x86': '†', '\x87': '‡', '\x88': 'ˆ',
        '\x89': '‰', '\x8a': 'Š', '\x8b': '‹', '\x8c': 'Œ',
        '\x8e': 'Ž',
        '\x91': "'", '\x92': "'", '\x93': '"', '\x94': '"',
        '\x95': '•', '\x96': '–', '\x97': '—', '\x98': '˜',
        '\x99': '™', '\x9a': 'š', '\x9b': '›', '\x9c': 'œ',
        '\x9e': 'ž', '\x9f': 'Ÿ',
    }
    for bad_char, good_char in cp1252_map.items():
        text = text.replace(bad_char, good_char)
    
    # Normalize common Unicode variants to ASCII-safe alternatives
    text = text.replace('\u2019', "'")   # right single quotation mark → apostrophe
    text = text.replace('\u2018', "'")   # left single quotation mark → apostrophe  
    text = text.replace('\u201c', '"').replace('\u201d', '"')  # smart double quotes
    text = text.replace('\u2013', '-')   # en-dash → hyphen
    text = text.replace('\u2014', '-')   # em-dash → hyphen
    text = text.replace('\u2026', '...') # ellipsis
    text = text.replace('\u00bd', '½')   # ensure half fraction is preserved
    text = text.replace('\u00ad', '')     # soft hyphen → remove
    # Remove stray combining characters from double-encoding
    text = text.replace('\u00c2', '').replace('\u00c3', '')
    # Normalize whitespace but preserve paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)      # collapse inline spaces
    text = re.sub(r'\n{3,}', '\n\n', text)   # max 2 newlines
    return text.strip()


# rs_category_id mapping: icasa.ch property types -> numeric CRM category IDs
# 1=apartment, 2=house, 3=multi-family, 4=land, 5=commercial, 6=parking
RS_CATEGORY_MAP = {
    'wohnung': 1, 'etagenwohnung': 1, 'dachwohnung': 1, 'attikawohnung': 1,
    'studio': 1, 'loft': 1, 'duplex': 1, 'maisonette': 1, 'gartenwohnung': 1,
    'haus': 2, 'einfamilienhaus': 2, 'doppeleinfamilienhaus': 2,
    'reihenhaus': 2, 'villa': 2, 'chalet': 2, 'bauernhaus': 2,
    'mehrfamilienhaus': 3,
    'bauland': 4, 'grundstück': 4,
    'büro': 5, 'gewerbe': 5, 'laden': 5, 'einzelhandel': 5,
    'garage': 6, 'parkplatz': 6,
}


def get_rs_category_id(title, property_type):
    """Map listing type to rs_category_id."""
    text = (title + ' ' + property_type).lower()
    for key, cat_id in RS_CATEGORY_MAP.items():
        if key in text:
            return cat_id
    return ''


def extract_property_details(soup, json_ld):
    """Extract property details from the detail page.
    
    icasa.ch uses a data table (.single__datatable) with <th>/<td> pairs for:
    - Adresse, Referenz-Nr., Kategorien, Verfügbar ab, Etage, Zimmer, Badezimmer, Baujahr
    - Verkaufspreis / Mietpreis
    """
    details = {
        'property_type': '',
        'title': '',
        'description': '',
        'rooms': '',
        'living_area_m2': '',
        'land_area_m2': '',
        'floor': '',
        'year_built': '',
        'features': [],
    }
    
    # === TITLE: extract from h1 with span.f1 + subtitle ===
    h1 = soup.find('h1')
    if h1:
        f1_span = h1.find('span', class_='f1')
        if f1_span:
            main_title = clean_text(f1_span.get_text(strip=True))
            # Fix common typo where 312 Zimmer is typed instead of 3½ Zimmer
            main_title = re.sub(r'(\d+)12\s+Zimmer', r'\1½ Zimmer', main_title)
            
            # Get the subtitle (text directly in h1 after the span)
            subtitle_parts = []
            for child in h1.children:
                if hasattr(child, 'name') and child.name == 'span' and 'f1' in (child.get('class') or []):
                    continue
                text = child.get_text(strip=True) if hasattr(child, 'get_text') else str(child).strip()
                if text:
                    subtitle_parts.append(text)
            subtitle = clean_text(' '.join(subtitle_parts))
            
            # Fix same typo in subtitle
            subtitle = re.sub(r'(\d+)12\s+Zimmer', r'\1½ Zimmer', subtitle)
            
            if subtitle:
                details['title'] = f"{main_title} - {subtitle}"
            else:
                details['title'] = main_title
        else:
            raw_title = clean_text(h1.get_text(strip=True))
            raw_title = re.sub(r'(\d+)12\s+Zimmer', r'\1½ Zimmer', raw_title)
            
            # Fix missing spaces between lowercase and uppercase (merged titles)
            fixed_title = re.sub(r'([a-z\u00e0-\u00ff])([A-Z\u00C0-\u00DC])', r'\1 - \2', raw_title)
            parts = fixed_title.split(' - ', 1)
            if len(parts) == 2 and parts[1].isupper():
                parts[1] = parts[1][0].upper() + parts[1][1:].lower()
                fixed_title = f"{parts[0]} - {parts[1]}"
            details['title'] = fixed_title
    
    # === DESCRIPTION: extract from #singleDescription div (primary icasa structure) ===
    desc_div = soup.find(id='singleDescription')
    if desc_div:
        # The singleDescription div contains two copies (read-more-default and read-more-target)
        # Use the read-more-target (expanded version with itemprop) or read-more-default
        target_span = desc_div.find('span', class_='read-more-target')
        if not target_span:
            target_span = desc_div.find('span', class_='read-more-default')
        if target_span:
            # Remove the h3 heading FIRST (before modifying the tree)
            h3 = target_span.find('h3')
            if h3:
                h3.decompose()  # Remove it from the tree entirely
            
            # Replace <br> with newlines and add spacing around <p> tags
            for br in target_span.find_all('br'):
                br.replace_with('\n')
            for p in target_span.find_all('p'):
                p.insert_before('\n')
                p.insert_after('\n')
            
            raw_desc = target_span.get_text()
            details['description'] = clean_text(raw_desc)[:2000]
    
    # Fallback: try h3 parent approach if singleDescription not found
    if not details['description']:
        desc_sections = soup.find_all('h3')
        for h3 in desc_sections:
            if 'beschreibung' in h3.get_text(strip=True).lower() or 'description' in h3.get_text(strip=True).lower():
                parent = h3.parent
                if parent:
                    text = parent.get_text('\n\n', strip=True)
                    if len(text) > 50:
                        h3_text = h3.get_text(strip=True)
                        if text.startswith(h3_text):
                            text = text[len(h3_text):].strip()
                        details['description'] = clean_text(text)[:2000]
                        break
    
    # === FIX 3 & 4: Extract from JSON-LD Residence ===
    residence = json_ld.get('residence')
    if residence:
        floor_size = residence.get('floorSize', {})
        if isinstance(floor_size, dict):
            area_val = floor_size.get('value', '')
            if area_val:
                try:
                    if float(area_val) > 10:
                        details['living_area_m2'] = str(area_val)
                except ValueError:
                    pass
        land_size = residence.get('lotSize', {})
        if isinstance(land_size, dict):
            land_val = land_size.get('value', '')
            if land_val:
                details['land_area_m2'] = str(land_val)
    
    # === Extract from single__datatable (icasa detail page table with th/td) ===
    datatable = soup.find(class_='single__datatable')
    if datatable:
        for tr in datatable.find_all('tr'):
            th = tr.find('th')
            td = tr.find('td')
            if th and td:
                label = th.get_text(strip=True).lower()
                value = td.get_text(strip=True)
                
                if 'zimmer' in label and not details['rooms']:
                    rooms_match = re.search(r'([\d.]+)', value)
                    if rooms_match:
                        details['rooms'] = rooms_match.group(1)
                
                elif 'wohnfläche' in label or ('fläche' in label and 'wohn' in label):
                    # Only extract if value explicitly contains m² or m2 unit
                    area_match = re.search(r'([\d]+[.,]?[\d]*)\s*(?:m²|m2)', value)
                    if area_match and not details['living_area_m2']:
                        details['living_area_m2'] = area_match.group(1).replace(',', '.')
                
                elif 'grundstück' in label or 'grundfläche' in label or 'nutzfläche' in label:
                    # Only extract if value explicitly contains m² or m2 unit
                    area_match = re.search(r'([\d]+[.,]?[\d]*)\s*(?:m²|m2)', value)
                    if area_match and not details['land_area_m2']:
                        details['land_area_m2'] = area_match.group(1).replace(',', '.')
                
                elif 'etage' in label and not details['floor']:
                    floor_match = re.search(r'([\d]+)', value)
                    if floor_match:
                        details['floor'] = floor_match.group(1)
                
                elif 'baujahr' in label and not details['year_built']:
                    year_match = re.search(r'(\d{4})', value)
                    if year_match:
                        details['year_built'] = year_match.group(1)
                
                elif 'kategorie' in label and not details['property_type']:
                    details['property_type'] = value
    
    # === Fallback: extract from propertycard__infos (used on listing cards) ===
    if not details['rooms'] or not details['property_type']:
        for info_div in soup.find_all(class_='propertycard__infos'):
            items = info_div.find_all(['div', 'span', 'li'])
            for item in items:
                item_text = item.get_text(strip=True)
                
                if 'Zimmer' in item_text and not details['rooms']:
                    rooms_match = re.search(r'([\d.]+)', item_text)
                    if rooms_match:
                        details['rooms'] = rooms_match.group(1)
                
                elif ('m²' in item_text or 'm2' in item_text) and 'Wohn' in item_text:
                    area_match = re.search(r'([\d]+[.,]?[\d]*)\s*(?:m²|m2)', item_text)
                    if area_match and not details['living_area_m2']:
                        details['living_area_m2'] = area_match.group(1).replace(',', '.')
                
                elif ('Grundst' in item_text or 'Grundfl' in item_text) and ('m²' in item_text or 'm2' in item_text):
                    area_match = re.search(r'([\d]+[.,]?[\d]*)\s*(?:m²|m2)', item_text)
                    if area_match and not details['land_area_m2']:
                        details['land_area_m2'] = area_match.group(1).replace(',', '.')
                
                elif 'Etage' in item_text and not details['floor']:
                    floor_match = re.search(r'([\d]+)', item_text)
                    if floor_match:
                        details['floor'] = floor_match.group(1)
                
                elif 'Baujahr' in item_text and not details['year_built']:
                    year_match = re.search(r'(\d{4})', item_text)
                    if year_match:
                        details['year_built'] = year_match.group(1)
            
            # Property type from card
            text = info_div.get_text(' ', strip=True).lower()
            if not details['property_type']:
                known_types = ['wohnung', 'haus', 'villa', 'maisonette', 'einfamilienhaus',
                              'doppeleinfamilienhaus', 'reihenhaus', 'dachwohnung', 'duplex',
                              'etagenwohnung', 'bauland', 'büro', 'gewerbe', 'garage',
                              'parkplatz', 'studio', 'chalet', 'loft', 'attikawohnung']
                for pt in known_types:
                    if pt in text:
                        details['property_type'] = pt.capitalize()
                        break
    
    # Fallback: parse area from description text (e.g. "92,85 m²" or "92.85 m²")
    if not details['living_area_m2'] and details['description']:
        m2_match = re.search(r'([\d]{2,}[.,][\d]+)\s*m²', details['description'])
        if m2_match:
            details['living_area_m2'] = m2_match.group(1).replace(',', '.')
    
    # Rooms fallback from title
    if not details['rooms'] and details['title']:
        rooms_match = re.search(r'([\d.]+)\s*[Zz]immer', details['title'])
        if rooms_match:
            details['rooms'] = rooms_match.group(1)
    
    # Features
    features_list = soup.find(class_='single__features__list')
    if features_list:
        for li in features_list.find_all('li'):
            feat = li.get_text(strip=True)
            if feat:
                details['features'].append(feat)
    
    return details


def extract_contact_from_page(soup, json_ld):
    """Extract contact/advertiser information.
    
    On icasa.ch, the provider (Anbieter) section contains:
    - Person name in <strong> tag
    - Company name in <div class="company"> tag  
    - Phone numbers hidden behind sticker overlays
    - Company address in provider box sub-blocks
    """
    contact = {
        'name': '',
        'first_name': '',
        'last_name': '',
        'organization_name': '',
        'email': '',
        'phone': '',
        'street': '',
        'house_number': '',
        'zip_code': '',
        'city': '',
    }
    
    # === From JSON-LD (most reliable) ===
    agent_data = json_ld.get('agent')
    if agent_data:
        brand = agent_data.get('brand', {})
        contact['organization_name'] = brand.get('name', '')
        
        address = agent_data.get('address', {})
        street_full = address.get('streetAddress', '').strip()
        
        # Split street and number
        if street_full:
            street_match = re.match(r'^(.+?)\s+(\d+\w?)$', street_full)
            if street_match:
                contact['street'] = street_match.group(1).strip()
                contact['house_number'] = street_match.group(2).strip()
            else:
                contact['street'] = street_full
        
        contact['zip_code'] = address.get('postalCode', '')
        contact['city'] = address.get('addressLocality', '')
    
    # === From Anbieter (Provider) section ===
    provider_box = soup.find(class_='single__providerbox')
    if provider_box:
        # Extract person name from <strong> inside the provider box
        strong_name = provider_box.find('strong')
        if strong_name:
            person_name = strong_name.get_text(strip=True)
            if person_name and not any(kw in person_name.lower() for kw in ['telefon', 'anzeigen', 'kontakt']):
                contact['name'] = person_name
                name_parts = person_name.strip().split()
                if len(name_parts) >= 2:
                    contact['first_name'] = name_parts[0]
                    contact['last_name'] = ' '.join(name_parts[1:])
        
        # Extract company name from .company div
        company_div = provider_box.find(class_='company')
        if company_div:
            company_name = company_div.get_text(strip=True)
            if company_name and not contact['organization_name']:
                contact['organization_name'] = company_name
        
        # Extract company address from provider box blocks
        if not contact['street']:
            addr_div = provider_box.find(class_='single__providerbox__company-address')
            if addr_div:
                addr_lines = []
                for line_div in addr_div.find_all(class_='single__providerbox__company-address__line'):
                    text = line_div.get_text(strip=True)
                    if text:
                        addr_lines.append(text)
                
                for line in addr_lines:
                    # Check for street + number pattern
                    street_match = re.match(r'^(.+?)\s+(\d+\w?)$', line)
                    if street_match and not contact['street']:
                        contact['street'] = street_match.group(1).strip()
                        contact['house_number'] = street_match.group(2).strip()
                    # Check for zip + city pattern
                    elif re.match(r'^\d{4}\s+.+', line) and not contact['zip_code']:
                        zip_city = re.match(r'(\d{4})\s+(.+)', line)
                        if zip_city:
                            contact['zip_code'] = zip_city.group(1)
                            contact['city'] = zip_city.group(2).strip()
    
    # === Fallback: From Kontakt section ===
    if not contact['organization_name']:
        kontakt_h2 = soup.find('h2', string=re.compile('Kontakt', re.I))
        if kontakt_h2:
            kontakt_section = kontakt_h2.parent
            if kontakt_section:
                text = kontakt_section.get_text('\n', strip=True)
                lines = text.split('\n')
                after_absenden = False
                for line in lines:
                    line = line.strip()
                    if 'Absenden' in line:
                        after_absenden = True
                        continue
                    if after_absenden and line and line not in ['Alle Objekte ansehen', 'Webseite anzeigen']:
                        if not line.startswith('+') and not line.startswith('0') and '@' not in line:
                            if not contact['organization_name']:
                                contact['organization_name'] = line
                            elif not contact['street'] and not line.startswith('Schweiz'):
                                contact['street'] = line
    
    # === Extract phone numbers (using improved extractor) ===
    phones = extract_phone_numbers(soup)
    if phones:
        contact['phone'] = phones[0]
    
    # === Extract email addresses ===
    emails = extract_email_addresses(soup)
    if emails:
        contact['email'] = emails[0]
    
    # === Try to extract personal name from organization (if no person found yet) ===
    if not contact['first_name'] and not contact['last_name']:
        org = contact['organization_name']
        if org and not is_agency(org):
            parts = org.strip().split()
            if len(parts) >= 2:
                contact['first_name'] = parts[0]
                contact['last_name'] = ' '.join(parts[1:])
    
    return contact


def extract_address_from_page(soup, json_ld, listing_url):
    """Extract property address.
    
    icasa.ch detail pages use a <table> inside .single__datatable with <th>/<td> pairs:
        <th>Adresse</th>
        <td>Strasse auf Anfrage<br> 1271 Givrins</td>
    OR:
        <th>Adresse</th>
        <td>Route de Genolier 5<br> 1271 Givrins</td>
    
    The address may also come from the schema.org microdata (itemprop) on similar properties.
    """
    address = {
        'street': '',
        'house_number': '',
        'zip_code': '',
        'city': '',
        'canton': '',
        'country': 'Schweiz',
    }
    
    # === From JSON-LD Residence ===
    residence = json_ld.get('residence')
    if residence:
        addr = residence.get('address', {})
        street_full = addr.get('streetAddress', '').strip()
        
        # Filter out "auf Anfrage" (on request) addresses
        if street_full and 'auf anfrage' not in street_full.lower():
            # Split street and number: handle "Route de Genolier 5", "Piazzetta San Carlo 2"
            street_match = re.match(r'^(.+?)\s+(\d+\w?)$', street_full)
            if street_match:
                address['street'] = street_match.group(1).strip()
                address['house_number'] = street_match.group(2).strip()
            else:
                address['street'] = street_full
        
        address['zip_code'] = addr.get('postalCode', '')
        address['city'] = addr.get('addressLocality', '')
        address['canton'] = addr.get('addressRegion', '')
    
    # === From the detail page data table (single__datatable) ===
    # This is the primary DOM source on icasa.ch detail pages
    if not address['street'] or not address['zip_code']:
        for table in soup.find_all('table'):
            for tr in table.find_all('tr'):
                th = tr.find('th')
                td = tr.find('td')
                if th and td:
                    label = th.get_text(strip=True).lower()
                    if 'adresse' in label or 'strasse' in label or 'standort' in label:
                        # The address cell may contain street + br + zip city
                        # e.g. "Route de Genolier 5\n 1271 Givrins"
                        # or   "Strasse auf Anfrage\n 1271 Givrins"
                        addr_lines = []
                        for part in td.stripped_strings:
                            addr_lines.append(part.strip())
                        
                        if addr_lines:
                            # First line = street (or "Strasse auf Anfrage")
                            first_line = addr_lines[0].strip()
                            if 'auf anfrage' not in first_line.lower() and not address['street']:
                                street_match = re.match(r'^(.+?)\s+(\d+\w?)$', first_line)
                                if street_match:
                                    address['street'] = street_match.group(1).strip()
                                    address['house_number'] = street_match.group(2).strip()
                                else:
                                    address['street'] = first_line
                            
                            # Second line (or remaining) = zip + city (e.g. "1271 Givrins")
                            for line in addr_lines[1:]:
                                zip_city_match = re.match(r'(\d{4})\s+(.+)', line.strip())
                                if zip_city_match:
                                    if not address['zip_code']:
                                        address['zip_code'] = zip_city_match.group(1)
                                    if not address['city']:
                                        address['city'] = zip_city_match.group(2).strip()
                        break  # Found the address row
    
    # === Try finding street via dl/dt/dd (other page styles) ===
    if not address['street']:
        for dl in soup.find_all('dl'):
            items = list(dl.find_all(['dt', 'dd']))
            for i in range(len(items) - 1):
                if items[i].name == 'dt' and items[i+1].name == 'dd':
                    label = items[i].get_text(strip=True).lower()
                    if 'adresse' in label or 'strasse' in label:
                        street_full = items[i+1].get_text(strip=True)
                        if 'auf anfrage' not in street_full.lower():
                            street_match = re.match(r'^(.+?)\s+(\d+\w?)$', street_full)
                            if street_match:
                                address['street'] = street_match.group(1).strip()
                                address['house_number'] = street_match.group(2).strip()
                            else:
                                address['street'] = street_full
                        break
    
    # === From schema.org microdata (itemprop) ===
    if not address['street']:
        street_el = soup.find('span', itemprop='streetAddress')
        if street_el:
            street_full = street_el.get_text(strip=True)
            if street_full and 'auf anfrage' not in street_full.lower():
                street_match = re.match(r'^(.+?)\s+(\d+\w?)$', street_full)
                if street_match:
                    address['street'] = street_match.group(1).strip()
                    address['house_number'] = street_match.group(2).strip()
                else:
                    address['street'] = street_full
    
    if not address['zip_code']:
        zip_el = soup.find('span', itemprop='postalCode')
        if zip_el:
            address['zip_code'] = zip_el.get_text(strip=True)
    
    if not address['city']:
        city_el = soup.find('span', itemprop='addressLocality')
        if city_el:
            address['city'] = city_el.get_text(strip=True)
    
    if not address['canton']:
        region_el = soup.find('span', itemprop='addressRegion')
        if region_el:
            address['canton'] = region_el.get_text(strip=True)
    
    # === From URL as fallback ===
    if not address['zip_code'] or not address['city']:
        # URL pattern: /X.X-zimmer-type-zu-kaufen-canton-region-city-ZIP-city-ID.html
        url_path = listing_url.split('/')[-1].replace('.html', '')
        parts = url_path.split('-')
        
        # Find 4-digit zip code in URL parts
        for i, part in enumerate(parts):
            if re.match(r'^\d{4}$', part):
                if not address['zip_code']:
                    address['zip_code'] = part
                # City name is typically the part(s) after the zip
                if not address['city'] and i + 1 < len(parts):
                    remaining = parts[i+1:]
                    if remaining:
                        city_parts = remaining[:-1] if remaining[-1].isdigit() else remaining
                        address['city'] = ' '.join(city_parts).title()
                break
    
    return address


# Global lock for ID persistence file
ID_LOCK = threading.Lock()

def get_next_persistent_id():
    """Read the next available ID from last_id.txt and increment it."""
    with ID_LOCK:
        if not os.path.exists(ID_PERSISTENCE_FILE):
            with open(ID_PERSISTENCE_FILE, 'w') as f:
                f.write(str(ID_RANGE_START))
            return str(ID_RANGE_START)
            
        with open(ID_PERSISTENCE_FILE, 'r') as f:
            try:
                val = int(f.read().strip())
            except (ValueError, TypeError):
                val = ID_RANGE_START
                
        if val >= ID_RANGE_END:
            logger.error(f"FATAL ERROR: ID range limit reached ({ID_RANGE_END}). No more IDs can be assigned!")
            sys.exit(1)
            
        next_val = val + 1
        with open(ID_PERSISTENCE_FILE, 'w') as f:
            f.write(str(next_val))
            
        return str(next_val)

# ============================================================
# MAIN SCRAPER
# ============================================================
class ICasaScraper:
    """Main scraper class for icasa.ch."""
    
    def __init__(self):
        self.session = ScraperSession()
        self.properties = []  # List of property dicts
        self.contacts = {}    # contact_key -> contact dict
        self.rejected = []    # List of rejected property dicts
        self.seen_addresses = set()  # For deduplication
        # No more id_counter here - we pull fresh IDs from last_id.txt as needed
        self.stats = {
            'pages_scraped': 0,
            'listings_found': 0,
            'listings_scraped': 0,
            'private_found': 0,
            'agency_filtered': 0,
            'no_phone_filtered': 0,
            'rent_below_threshold': 0,
            'duplicates_filtered': 0,
            'errors': 0,
        }
        self.next_property_id = 1001
        self.next_contact_id = 5001
    
    def check_url_exists(self, url):
        """Check if URL already exists via API."""
        try:
            resp = self.session.session.post(
                "https://api.we-net.ch/api/listings/check-url",
                json={"detail_url": url},
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get('exists', False)
        except Exception as e:
            logger.error(f"Error checking API for url: {e}")
        return False

    def check_contact(self, contact_data):
        """Check if contact is blocked or exists."""
        try:
            resp = self.session.session.post(
                "https://api.we-net.ch/api/advertisers/check",
                json=contact_data,
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Error checking contact API: {e}")
        return {"found": False, "blocked": False}
    
    def get_listing_links(self, category_url, offer_type, apply_price_filter=True, max_pages=None, stream_file=None):
        """Scrape all listing URLs from paginated listing pages.

        Parameters:
        - apply_price_filter: when False, do not pre-filter rental cards by price (useful for urls-only mode)
        - max_pages: override config MAX_PAGES when set (int)
        """
        links = []
        page = 1
        
        seen_page_links = set()
        max_pages = MAX_PAGES if max_pages is None else max_pages
        
        while page <= max_pages:
            if page == 1:
                url = f"{category_url}?sort="
            else:
                url = f"{category_url}?page={page}&sort="
            
            logger.info(f"Scraping listing page {page}: {url}")
            resp = self.session.get(url, delay=DELAY_BETWEEN_PAGES)
            
            if not resp:
                break
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Find property card links
            cards = soup.find_all('a', class_='propertycard__header', href=True)
            
            if not cards:
                logger.info(f"No more listings on page {page}. Stopping.")
                break
            
            page_links = []
            page_hrefs = []
            for card in cards:
                href = card['href']
                full_url = urljoin(BASE_URL, href)
                page_hrefs.append(full_url)
                
                # For rentals, optionally check price from card before visiting detail page
                if offer_type == 'rent' and apply_price_filter:
                    parent_card = card.find_parent(class_='propertycard')
                    if parent_card:
                        price_el = parent_card.find(class_='propertycard__price')
                        if price_el:
                            price_text = price_el.get_text(strip=True)
                            price_val, price_unit = parse_price(price_text)
                            
                            if price_val is not None and price_unit in ['CHF/month', 'CHF/m2/month']:
                                if price_val < MIN_RENT_CHF:
                                    self.stats['rent_below_threshold'] += 1
                                    self.rejected.append({'url': full_url, 'reason': f'Phase 2: Rent too low ({price_val} < {MIN_RENT_CHF})', 'advertiser': 'N/A (Card level)'})
                                    logger.info(f"    SKIPPED (Phase 2): Rent below threshold: {price_val} - {full_url}")
                                    continue
                
                page_links.append((full_url, offer_type))

                # If a stream file is provided, write the URL immediately so
                # callers (or users tailing the file) can see progress live.
                if stream_file is not None:
                    try:
                        stream_file.write(full_url + '\n')
                        stream_file.flush()
                    except Exception as e:
                        logger.debug(f"Failed to write URL to stream file: {e}")
            
            # Check for duplicate pages (icasa sometimes loops to page 1)
            links_tuple = tuple(page_hrefs)
            if links_tuple in seen_page_links:
                logger.info("Duplicate page detected (same listings as before). Stopping pagination.")
                break
            seen_page_links.add(links_tuple)
            
            links.extend(page_links)
            self.stats['pages_scraped'] += 1
            logger.info(f"  Found {len(page_links)} listings on page {page}")
            
            # Check if there's a next page
            next_page = soup.find('a', href=re.compile(f'page={page+1}'))
            if not next_page:
                logger.info(f"No next page link found. Last page was {page}.")
                break
            
            page += 1
        
        self.stats['listings_found'] += len(links)
        return links

    def collect_listing_urls_only(self, max_pages=None):
        """Collect all property detail URLs from BUY and RENT listings and write to files.

        This method does not visit detail pages or apply any detail-level filters.
        """
        logger.info("--- URLS-ONLY: Collecting BUY and RENT listing URLs ---")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        buy_path = os.path.join(OUTPUT_DIR, 'buy_urls.txt')
        rent_path = os.path.join(OUTPUT_DIR, 'rent_urls.txt')

        # Open both files and stream-write URLs as they are discovered
        try:
            with open(buy_path, 'w', encoding='utf-8') as bf, open(rent_path, 'w', encoding='utf-8') as rf:
                buy_links = self.get_listing_links(BUY_URL, 'buy', apply_price_filter=False, max_pages=max_pages, stream_file=bf)
                rent_links = self.get_listing_links(RENT_URL, 'rent', apply_price_filter=False, max_pages=max_pages, stream_file=rf)

            buy_urls = [u for u, _ in buy_links]
            rent_urls = [u for u, _ in rent_links]

            logger.info(f"Saved {len(buy_urls)} BUY URLs to {buy_path}")
            logger.info(f"Saved {len(rent_urls)} RENT URLs to {rent_path}")

        except Exception as e:
            logger.error(f"Failed to write URL files: {e}")

        logger.info("--- URLS-ONLY: Completed. Exiting (no detail scraping). ---")
    
    def scrape_detail_page(self, url, offer_type):
        """Scrape a single listing detail page."""
        logger.info(f"  Scraping detail: {url}")
        
        # === API CHECK: Check if URL was already scraped ===
        if not DEMO_MODE and self.check_url_exists(url):
            self.stats['duplicates_filtered'] += 1
            logger.info("    SKIPPED: URL already exists in database (checked via API)")
            return None

        resp = self.session.get(url)
        if not resp:
            self.stats['errors'] += 1
            return None
        
        soup = BeautifulSoup(resp.text, 'lxml')
        
        # === Extract JSON-LD ===
        json_ld = extract_json_ld(soup)
        
        # === Extract contact information ===
        contact = extract_contact_from_page(soup, json_ld)
        
        # === CHECK: Phone number required ===
        if not contact['phone']:
            self.stats['no_phone_filtered'] += 1
            self.rejected.append({'url': url, 'reason': 'No phone number', 'advertiser': contact['organization_name']})
            logger.info(f"    SKIPPED: No phone number found")
            return None
        
        # === CHECK: Filter out agencies (skip in DEMO_MODE) ===
        advertiser_name = contact['organization_name']
        if not DEMO_MODE and is_agency(advertiser_name):
            self.stats['agency_filtered'] += 1
            self.rejected.append({'url': url, 'reason': 'Agency detected', 'advertiser': advertiser_name})
            logger.info(f"    SKIPPED: Agency detected: '{advertiser_name}'")
            return None
        
        if DEMO_MODE:
            self.stats['private_found'] += 1
            logger.info(f"    [DEMO] Accepting listing from: '{advertiser_name}'")
        else:
            self.stats['private_found'] += 1
            logger.info(f"    [OK] PRIVATE listing found! Advertiser: '{advertiser_name}'")
        
        # === Extract property address ===
        address = extract_address_from_page(soup, json_ld, url)
        
        # === CHECK: Deduplication by URL (primary) and full address (secondary) ===
        if url in self.seen_addresses:
            self.stats['duplicates_filtered'] += 1
            self.rejected.append({'url': url, 'reason': 'Duplicate URL', 'advertiser': contact['organization_name']})
            logger.info(f"    SKIPPED: Duplicate URL")
            return None
        # Only deduplicate by address when we have a complete address (zip + city + street)
        addr_key = f"{address['zip_code']}|{address['city']}|{address['street']}|{address['house_number']}".lower()
        if address['street'] and address['zip_code'] and addr_key in self.seen_addresses:
            self.stats['duplicates_filtered'] += 1
            self.rejected.append({'url': url, 'reason': 'Duplicate address', 'advertiser': contact['organization_name']})
            logger.info(f"    SKIPPED: Duplicate address: {addr_key}")
            return None
        self.seen_addresses.add(url)
        if address['street'] and address['zip_code']:
            self.seen_addresses.add(addr_key)
        
        # === Extract property details ===
        details = extract_property_details(soup, json_ld)
        
        # === Extract latitude and longitude ===
        latitude = ''
        longitude = ''
        
        # Priority 1: From JSON-LD
        residence = json_ld.get('residence')
        if residence and residence.get('geo'):
            latitude = residence['geo'].get('latitude', '')
            longitude = residence['geo'].get('longitude', '')
        elif product_data := json_ld.get('product'):
            if product_data.get('geo'):
                latitude = product_data['geo'].get('latitude', '')
                longitude = product_data['geo'].get('longitude', '')
        
        # Priority 2: From #map div data-marker attribute
        # icasa.ch stores coords as: data-marker='{"lat":"46.4331079","lng":"6.1934267","id":1825641}'
        if not latitude or not longitude:
            map_div = soup.find('div', id='map')
            if map_div and map_div.get('data-marker'):
                try:
                    marker_data = json.loads(map_div['data-marker'])
                    if marker_data.get('lat'):
                        latitude = str(marker_data['lat'])
                    if marker_data.get('lng'):
                        longitude = str(marker_data['lng'])
                    logger.debug(f"    Got coords from data-marker: {latitude}, {longitude}")
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"    Failed to parse data-marker: {e}")
        
        # Priority 3: From schema.org microdata (itemprop) on the page
        if not latitude or not longitude:
            # Important: exclude .similar-properties block which holds meta tags for OTHER listings
            safe_soup_area = soup.find('div', class_='single__content') or soup
            lat_meta = safe_soup_area.find('meta', itemprop='latitude')
            lng_meta = safe_soup_area.find('meta', itemprop='longitude')
            if lat_meta and lat_meta.get('content'):
                latitude = lat_meta['content']
            if lng_meta and lng_meta.get('content'):
                longitude = lng_meta['content']
        
        # Priority 4: Regex fallback for various JSON patterns in HTML
        if not latitude or not longitude:
            # Try "lat":"value" pattern (icasa uses quoted values)
            lat_match = re.search(r'"lat"\s*:\s*"?([0-9.]+)"?', str(soup))
            lon_match = re.search(r'"lng"\s*:\s*"?([0-9.]+)"?', str(soup))
            if lat_match and lon_match:
                latitude = lat_match.group(1)
                longitude = lon_match.group(1)
            else:
                # Try "latitude":value pattern
                lat_match = re.search(r'"latitude"\s*:\s*"?([0-9.]+)"?', str(soup))
                lon_match = re.search(r'"longitude"\s*:\s*"?([0-9.]+)"?', str(soup))
                if lat_match and lon_match:
                    latitude = lat_match.group(1)
                    longitude = lon_match.group(1)
        
        # === Extract PRICE ===
        # Priority 1: From single__datatable (Verkaufspreis / Mietpreis row)
        # Priority 2: From propertycard__price div 
        # Priority 3: From JSON-LD offers.price
        price_val = None
        price_text = ''
        price_unit = 'CHF'
        
        # Priority 1: Parse from the detail page data table
        datatable = soup.find(class_='single__datatable')
        if datatable:
            for tr in datatable.find_all('tr'):
                th = tr.find('th')
                td = tr.find('td')
                if th and td:
                    label = th.get_text(strip=True).lower()
                    if 'preis' in label or 'miete' in label:
                        price_text = td.get_text(strip=True)
                        price_val, price_unit = parse_price(price_text)
                        if price_val:
                            break
        
        # Priority 2: From propertycard__price div
        if price_val is None:
            price_divs = soup.find_all(class_='propertycard__price')
            if price_divs:
                price_text = price_divs[0].get_text(strip=True).replace('\n', ' ').strip()
                price_val, price_unit = parse_price(price_text)
        
        # Priority 3: From JSON-LD
        if price_val is None:
            product_data = json_ld.get('product')
            if product_data:
                offers = product_data.get('offers', {})
                jld_price = offers.get('price')
                if jld_price:
                    try:
                        jld_val = float(jld_price)
                        if jld_val >= 100:
                            price_val = jld_val
                            price_unit = offers.get('priceCurrency', 'CHF')
                    except (ValueError, TypeError):
                        pass
        
        if offer_type == 'rent' and price_unit == 'CHF':
            price_unit = 'CHF/month'
            
        # Standardize price string (keep raw text as requested by user)
        clean_price_string = price_text if price_text else (str(int(price_val)) if price_val else "")
        
        # === Rental price check ===
        if offer_type == 'rent' and price_val:
            if price_val < MIN_RENT_CHF:
                self.stats['rent_below_threshold'] += 1
                self.rejected.append({'url': url, 'reason': f'Price too low ({price_val} < {MIN_RENT_CHF})', 'advertiser': contact.get('organization_name', 'N/A')})
                logger.info(f"    SKIPPED: Rent {price_val} < {MIN_RENT_CHF} CHF")
                return None
                
        # === API CHECK: Check contact (skip in DEMO_MODE) ===
        advertiser_id = ''
        if not DEMO_MODE:
            api_contact_data = {
                "first_name": contact.get('first_name', ''),
                "last_name": contact.get('last_name', ''),
                "organization_name": contact.get('organization_name', ''),
                "phone": contact.get('phone', ''),
                "email": contact.get('email', '')
            }
            api_contact_data = {k: v for k, v in api_contact_data.items() if v}
            
            contact_check = self.check_contact(api_contact_data)
            
            if contact_check.get('blocked', False):
                self.stats['agency_filtered'] += 1
                self.rejected.append({'url': url, 'reason': 'Blocked by CRM API', 'advertiser': contact.get('organization_name', 'N/A')})
                logger.info("    SKIPPED: Contact is blocked by API")
                return None
                
            if contact_check.get('found', False):
                advertiser_id = contact_check.get('id', '')
        
        # === Build contact record ===
        normalized = normalize_phone(contact['phone'])
        contact_key = f"{normalized}|{contact.get('first_name','')}|{contact.get('last_name','')}".lower()
        
        if contact_key not in self.contacts:
            contact_id = get_next_persistent_id()
            
            self.contacts[contact_key] = {
                'external_id': contact_id,
                'first_name': contact.get('first_name', ''),
                'last_name': contact.get('last_name', ''),
                'organization_name': contact.get('organization_name', ''),
                'email': contact.get('email', ''),
                'phone': contact.get('phone', ''),
                'street': contact.get('street', ''),
                'house_number': contact.get('house_number', ''),
                'zip_code': contact.get('zip_code', ''),
                'city': contact.get('city', ''),
                'normalized_phone': normalized,
                'portal_id': PORTAL_ID,
                'vendor_id': VENDOR_ID,
            }
        
        contact_external_id = self.contacts[contact_key]['external_id']
        
        # === FIX 5: rs_category_id mapping ===
        rs_cat = get_rs_category_id(details.get('title', ''), details.get('property_type', ''))
        
        # === Build property record ===
        property_external_id = get_next_persistent_id()
        
        prop = {
            'external_id': property_external_id,
            'contact_external_id': contact_external_id,
            'portal_id': PORTAL_ID,
            'vendor_id': VENDOR_ID,
            'type_id': 1 if offer_type == 'buy' else 2,
            'detail_url': url,
            'title': details.get('title', ''),
            'description': details.get('description', '')[:2000],
            'street': address.get('street', ''),
            'house_number': address.get('house_number', ''),
            'zip_code': address.get('zip_code', ''),
            'city': address.get('city', ''),
            'latitude': latitude,
            'longitude': longitude,
            # price = clean string like "1050000" or standardized
            'price': clean_price_string,
            'living_space_area': details.get('living_area_m2', ''),
            'land_area': details.get('land_area_m2', ''),
            'rs_category_id': rs_cat,
            # price_value = clean integer matching the price text
            'price_value': int(price_val) if price_val else '',
            'advertiser_id': '',  # Set to empty as requested by user
        }
        
        self.properties.append(prop)
        self.stats['listings_scraped'] += 1
        
        return prop
    
    def export_csv(self):
        """Export results to Objekte.csv and Kontakte.csv."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # === Objekte.csv ===
        objekte_path = os.path.join(OUTPUT_DIR, OBJEKTE_FILENAME)
        try:
            with open(objekte_path, 'w', newline='', encoding=CSV_ENCODING) as f:
                writer = csv.DictWriter(f, fieldnames=OBJEKTE_COLUMNS, delimiter=CSV_DELIMITER,
                                       extrasaction='ignore')
                writer.writeheader()
                for prop in self.properties:
                    writer.writerow(prop)
            logger.info(f"Exported {len(self.properties)} properties to {objekte_path}")
        except PermissionError:
            logger.error(f"❌ PERMISSION DENIED: Please close '{objekte_path}' so the scraper can save data!")
        
        # === Kontakte.csv ===
        kontakte_path = os.path.join(OUTPUT_DIR, KONTAKTE_FILENAME)
        try:
            with open(kontakte_path, 'w', newline='', encoding=CSV_ENCODING) as f:
                writer = csv.DictWriter(f, fieldnames=KONTAKTE_COLUMNS, delimiter=CSV_DELIMITER,
                                       extrasaction='ignore')
                writer.writeheader()
                for contact in self.contacts.values():
                    writer.writerow(contact)
            logger.info(f"Exported {len(self.contacts)} contacts to {kontakte_path}")
        except PermissionError:
            logger.error(f"❌ PERMISSION DENIED: Please close '{kontakte_path}' so the scraper can save data!")
    
    def print_report(self):
        """Print final scraping report."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("SCRAPING REPORT")
        logger.info("=" * 60)
        logger.info(f"Portal:                {PORTAL_NAME}")
        logger.info(f"Date:                  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        logger.info(f"Pages scraped:         {self.stats['pages_scraped']}")
        logger.info(f"Listings found:        {self.stats['listings_found']}")
        logger.info(f"Detail pages scraped:  {self.stats['listings_scraped'] + self.stats['agency_filtered'] + self.stats['no_phone_filtered'] + self.stats['duplicates_filtered']}")
        logger.info(f"")
        logger.info(f"--- FILTERING ---")
        logger.info(f"Agency/company:        {self.stats['agency_filtered']} filtered out")
        logger.info(f"No phone number:       {self.stats['no_phone_filtered']} filtered out")
        logger.info(f"Rent below threshold:  {self.stats['rent_below_threshold']} filtered out")
        logger.info(f"Duplicates:            {self.stats['duplicates_filtered']} filtered out")
        logger.info(f"Errors:                {self.stats['errors']}")
        logger.info(f"")
        logger.info(f"--- RESULTS ---")
        logger.info(f"Private listings:      {self.stats['private_found']}")
        logger.info(f"Properties exported:   {len(self.properties)}")
        logger.info(f"Contacts exported:     {len(self.contacts)}")
        logger.info("=" * 60)
    
    def export_rejected(self):
        """Export rejected listings to a CSV file for transparency."""
        if not self.rejected:
            logger.info("No rejected listings to export.")
            return
            
        path = os.path.join(OUTPUT_DIR, 'Phase3_Rejected.csv')
        try:
            with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['url', 'reason', 'advertiser'])
                writer.writeheader()
                writer.writerows(self.rejected)
            logger.info(f"Exported {len(self.rejected)} rejected listings to {path}")
        except Exception as e:
            logger.error(f"Failed to export rejected listings: {e}")

    def run(self):
        """Run the full scraping pipeline."""
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info(f"iCasa.ch Scraper - Starting")
        logger.info(f"Date: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        logger.info("=" * 60)
        
        # === Step 1: Collect all listing URLs ===
        logger.info("\n--- PHASE 1: Collecting listing URLs ---")
        
        # Buy listings (all)
        logger.info("\nCollecting BUY listings...")
        buy_links = self.get_listing_links(BUY_URL, 'buy')
        logger.info(f"Total buy listings: {len(buy_links)}")
        
        # Rent listings (>= 3000 CHF)
        logger.info("\nCollecting RENT listings...")
        rent_links = self.get_listing_links(RENT_URL, 'rent')
        logger.info(f"Total rent listings (after price filter): {len(rent_links)}")
        
        all_links = buy_links + rent_links
        logger.info(f"\nTotal listings to scrape: {len(all_links)}")
        
        # === Step 2: Scrape each detail page ===
        logger.info("\n--- PHASE 2: Scraping detail pages ---")
        
        # Collect listings until we find the required number of valid private listings
        logger.info(f"Scanning up to {len(all_links)} links to find 100 valid private listings.")
        
        for i, (url, offer_type) in enumerate(all_links, 1):
            logger.info(f"\n[{i}/{len(all_links)}] ({offer_type.upper()}) | Found so far: {len(self.properties)}")
            try:
                self.scrape_detail_page(url, offer_type)
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                self.stats['errors'] += 1
            
            # Auto-save immediately so data is populated in output live
            if i % 1 == 0:
                self.export_csv()
        
        # === Step 3: Export CSV ===
        logger.info("\n--- PHASE 3: Exporting CSV files ---")
        self.export_csv()
        self.export_rejected()
        
        # === Step 4: Print report ===
        elapsed = time.time() - start_time
        logger.info(f"\nTotal time: {elapsed:.1f} seconds")
        self.print_report()
        
        return self.stats


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == '__main__':
    scraper = ICasaScraper()

    # Simple CLI parsing for urls-only mode and optional max-pages override
    max_pages_arg = None
    for arg in sys.argv[1:]:
        if arg.startswith('--max-pages='):
            try:
                max_pages_arg = int(arg.split('=', 1)[1])
            except Exception:
                logger.warning(f"Invalid --max-pages value: {arg}")

    if '--urls-only' in sys.argv:
        scraper.collect_listing_urls_only(max_pages=max_pages_arg)
        print(f"[DONE] URL lists saved in ./{OUTPUT_DIR}/ (buy_urls.txt, rent_urls.txt)")
    else:
        stats = scraper.run()
        # Exit summary
        print(f"\n[DONE] Results in ./{OUTPUT_DIR}/")
        print(f"  - {OBJEKTE_FILENAME}: {len(scraper.properties)} properties")
        print(f"  - {KONTAKTE_FILENAME}: {len(scraper.contacts)} contacts")
        
        if len(scraper.properties) == 0:
            print(f"\n[WARNING] No private listings found on {PORTAL_NAME}.")
            print(f"  This portal is primarily used by real estate agencies.")
            print(f"  {scraper.stats['agency_filtered']} listings were from agencies/companies.")
            print(f"  Consider trying portals with more private sellers (e.g., tutti.ch, anibis.ch).")
