#!/usr/bin/env python3
"""
Phase 3 scraper: fetch detail pages for entries marked NOT FOUND and extract contact/property data.
Writes qualified contacts to output/Kontakte.csv and properties to output/Objekte.csv.
Usage example:
    python phase3_scrape.py --workers 10 --limit 200 --resume
"""
import argparse
import csv
import json
import os
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

import requests
from bs4 import BeautifulSoup
import html
from requests.adapters import HTTPAdapter

import config

LOG = logging.getLogger('phase3')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
DELAY = 0.0
# runtime flags (set from CLI args in main)
STORE_NO_PRICE = False
ALLOW_BELOW_MIN = False

PHONE_RE = re.compile(r'(\+?\d[\d\-\s\(\)]{6,}\d)')
POSTAL_CITY_RE = re.compile(r"\b(\d{4})\s+([A-Za-zÀ-ÖØ-öø-ÿ\- ]{2,60})")


def normalize_phone(raw):
    if not raw:
        return ''
    s = str(raw).strip()
    # normalize whitespace and common unicode spaces
    s = s.replace('\u00a0', ' ').replace('\xa0', ' ').replace('\u2009', ' ')
    # remove common leading labels
    s = re.sub(r'(?i)^\s*(tel:|telefon:|phone:)\s*', '', s)
    # convert international 00 prefix to +
    s = re.sub(r'^\s*00', '+', s)
    # remove common extension markers at end (e.g. ext 123)
    s = re.sub(r'(?i)(?:ext|x|durchwahl|dw)\s*[:\.-]?\s*\d+$', '', s)
    # remove the common '(0)' trunk indicator used in international formats
    s = re.sub(r'\(\s*0\s*\)', '', s)
    # remove remaining parentheses but keep digits inside
    s = s.replace('(', '').replace(')', '')
    # keep only digits and optional leading +
    s = re.sub(r'[^+\d]', '', s)
    if not s:
        return ''
    digits = re.sub(r'\D', '', s)
    # Accept only plausible phone numbers (reject short fragments)
    # Require at least 9 digits (covers Swiss national numbers and most international numbers)
    if s.startswith('+'):
        if len(digits) >= 9:
            return '+' + digits
        return ''
    # local Swiss format starting with 0 -> convert to +41
    if s.startswith('0'):
        if len(digits) >= 9:
            return '+41' + digits[1:]
        return ''
    # numbers that look like they already include country code without + (e.g. 4144...)
    if digits.startswith('41') and len(digits) >= 11:
        return '+' + digits
    # fallback: accept long digit-only sequences
    if len(digits) >= 9:
        return digits
    return ''


def normalize_name(name):
    if not name:
        return ''
    return ' '.join(name.split()).lower()


def normalize_addr(street, house, zip_code, city):
    parts = [street or '', house or '', zip_code or '', city or '']
    joined = ' '.join(p.strip() for p in parts if p)
    joined = re.sub(r"[^\w\d ]", '', joined.lower())
    joined = re.sub(r'\s+', ' ', joined).strip()
    return joined


def load_existing_contacts(path):
    contacts = {}  # (phone_norm, name_norm) -> external_id
    phones = set()
    if not os.path.exists(path):
        return contacts, phones
    with open(path, 'r', encoding=config.CSV_ENCODING, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ext = r.get('external_id') or r.get('externalId')
            if not ext or not ext.strip() or not ext.strip().isdigit():
                continue
            phone = r.get('phone', '')
            nphone = normalize_phone(r.get('normalized_phone') or phone)
            first = r.get('first_name', '')
            last = r.get('last_name', '')
            name_norm = normalize_name((first + ' ' + last).strip())
            contacts[(nphone, name_norm)] = ext
            if nphone:
                phones.add(nphone)
    return contacts, phones


def load_existing_properties(path):
    props = set()
    if not os.path.exists(path):
        return props
    with open(path, 'r', encoding=config.CSV_ENCODING, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            street = r.get('street', '')
            hn = r.get('house_number', '')
            zipc = r.get('zip_code', '')
            city = r.get('city', '')
            norm = normalize_addr(street, hn, zipc, city)
            if norm:
                props.add(norm)
    return props


def parse_input_results(path):
    """Parses input file. Supports .jsonl (line-delimited) or .json (standard array)."""
    items = []
    if not os.path.exists(path):
        LOG.error('Input file not found: %s', path)
        return items
        
    ext = os.path.splitext(path)[1].lower()
    if ext == '.json':
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for d in data:
                        err = (d.get('error') or '').upper()
                        status = d.get('status')
                        if err == 'NOT FOUND' or status == 404:
                            items.append({'url': d.get('url'), 'type': d.get('type')})
                else:
                    LOG.error('JSON file is not a list: %s', path)
        except Exception as e:
            LOG.error('Error parsing JSON array: %s', e)
    else:
        # assume .jsonl
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    err = (d.get('error') or '').upper()
                    status = d.get('status')
                    if err == 'NOT FOUND' or status == 404:
                        items.append({'url': d.get('url'), 'type': d.get('type')})
                except Exception:
                    continue
    return items


def extract_phone(soup):
    # 1) tel: links (prefer explicit tel: anchors)
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if href.lower().startswith('tel:'):
            # strip parameters after ; (e.g. tel:+411234567;ext=123)
            phone_raw = href.split(':', 1)[1].split(';', 1)[0].strip()
            # prefer visible anchor text when it contains digits
            text = a.get_text(strip=True)
            candidate_raw = text if (text and re.search(r'\d', text)) else phone_raw
            n = normalize_phone(candidate_raw)
            if n and len(re.sub(r'\D', '', n)) >= 9:
                return n

    # 1b) look for elements/classes that commonly contain phone labels
    for el in soup.find_all(class_=re.compile('phone|tel|kontakt|telefon', re.I)):
        txt = el.get_text(' ', strip=True)
        n = normalize_phone(txt)
        if n and len(re.sub(r'\D', '', n)) >= 9:
            return n

    # 2) text regex fallback: scan entire page for candidate phone-like strings
    txt = soup.get_text(separator=' ')
    for m in PHONE_RE.finditer(txt):
        candidate_raw = m.group(1).strip()
        n = normalize_phone(candidate_raw)
        if n and len(re.sub(r'\D', '', n)) >= 9:
            return n

    return ''


def extract_address(soup):
    street = house = zip_code = city = ''
    # microdata
    s = soup.select_one('[itemprop=streetAddress]')
    if s:
        street = s.get_text(strip=True)
    hn = soup.select_one('[itemprop=streetNumber]') or soup.select_one('[itemprop=houseNumber]')
    if hn:
        house = hn.get_text(strip=True)
    pc = soup.select_one('[itemprop=postalCode]')
    if pc:
        zip_code = pc.get_text(strip=True)
    loc = soup.select_one('[itemprop=addressLocality]') or soup.select_one('[itemprop=addressRegion]')
    if loc:
        city = loc.get_text(strip=True)
    # address tag
    if not (zip_code and city):
        addr_tag = soup.find('address')
        if addr_tag:
            text = addr_tag.get_text(separator='\n')
            m = POSTAL_CITY_RE.search(text)
            if m:
                zip_code = zip_code or m.group(1)
                city = city or m.group(2).strip()
            # attempt to get street line
            for line in text.splitlines():
                if line.strip() and not re.search(r'(Tel|Telefon|E-Mail|www|http|CHF|Fr\.)', line, re.I):
                    sm = re.search(r'(.+?)\s+(\d+\w*)$', line.strip())
                    if sm:
                        street = street or sm.group(1).strip()
                        house = house or sm.group(2).strip()
                        break
                    else:
                        street = street or line.strip()
    # fallback: search page text for postal code and nearby street
    if not (zip_code and city):
        txt = soup.get_text(separator='\n')
        m = POSTAL_CITY_RE.search(txt)
        if m:
            zip_code = zip_code or m.group(1)
            city = city or m.group(2).strip()
            # find preceding non-empty line for street
            lines = txt.splitlines()
            for i, line in enumerate(lines):
                if m.group(1) in line:
                    for j in range(i - 1, max(-1, i - 5), -1):
                        cand = lines[j].strip()
                        if cand and not re.search(r'(Tel|Telefon|E-Mail|www|http|CHF|Fr\.)', cand, re.I):
                            sm = re.search(r'(.+?)\s+(\d+\w*)$', cand)
                            if sm:
                                street = street or sm.group(1).strip()
                                house = house or sm.group(2).strip()
                            else:
                                street = street or cand
                            break
                    break
    return street, house, zip_code, city


def extract_price(soup):
    def _parse_num(s):
        if not s:
            return None, None
        raw = s
        s = s.strip()
        # normalize common unicode characters
        s = s.replace('\u2019', "'").replace('\u2018', "'")
        s = s.replace('\u00a0', ' ').replace('\xa0', ' ')
        # flags for context
        had_currency = bool(re.search(r'\b(CHF|Fr\.?|frs|fr)\b', s, re.I))
        had_label = bool(re.search(r'(?i)(?:preis|verkaufspreis|mietpreis|kaufpreis|prix|prezzo)', s))
        # remove common leading tokens and trailing descriptors
        s = re.sub(r'(?i)^(?:preis|verkaufspreis|mietpreis|kaufpreis|chf|fr\.?|auf anfrage)[:\s]*', '', s)
        s = s.split('/')[0]
        # if it looks like a decimal room count (e.g. 3.5, 2,5) and no explicit currency, ignore
        if not had_currency and re.search(r"\d+[\.,]\d+", s):
            return None, None
        # extract digits only
        digits = re.sub(r'[^0-9]', '', s)
        if not digits:
            return None, None
        try:
            v = int(digits)
        except Exception:
            return None, None
        # reject implausibly small numbers unless currency or explicit price label present
        if not had_currency and not had_label and v < 1000:
            return None, None
        return v, raw.strip()

    # 1) Look for explicit labeled rows (e.g. <th>Verkaufspreis</th> <td>CHF 785'000.–</td>)
    for th in soup.find_all(['th', 'strong', 'b']):
        text = th.get_text(' ', strip=True)
        if re.search(r'verkaufspreis|mietpreis|kaufpreis|preis', text, re.I):
            # try to find the corresponding td in the same row
            tr = th.find_parent('tr')
            td_text = None
            if tr:
                td = tr.find('td')
                if td:
                    td_text = td.get_text(' ', strip=True)
            if not td_text:
                sib = th.find_next_sibling('td')
                if sib:
                    td_text = sib.get_text(' ', strip=True)
            v, s_raw = _parse_num(td_text or '')
            if v is not None:
                return v, s_raw

    # 1b) Directly check <td> elements that start with a currency token (e.g. <td>CHF\u00a01\u2019490\u2019000.–</td>)
    for td in soup.find_all('td'):
        td_txt = td.get_text(' ', strip=True)
        if re.search(r'^(?:CHF|Fr\.?|verkaufspreis)\s*[0-9\u2019\'"\.,\s\u00a0\xa0]+', td_txt, re.I):
            v, s_raw = _parse_num(td_txt)
            if v is not None:
                return v, s_raw

    # 2) Look for tables with a header 'Preis'
    for table in soup.find_all('table'):
        thead = table.find('thead')
        if thead and re.search(r'preis', thead.get_text(' ', strip=True), re.I):
            for td in table.find_all('td'):
                v, s_raw = _parse_num(td.get_text(' ', strip=True))
                if v is not None:
                    return v, s_raw

    # 3) Try elements whose class contains 'price' or 'preis' but skip room-count labels
    price_elems = soup.find_all(class_=re.compile('price|preis', re.I))
    for el in price_elems:
        txt = el.get_text(' ', strip=True)
        # skip obvious room-counts like '3.5 Zimmer', '4.5-Zimmer', '3.5 pces'
        if re.search(r"\b\d+[\.,]?\d*\s*(?:zimmer|zi\b|pces|loc\b|pieces|rooms|pi[eè]ces)\b", txt, re.I):
            continue
        v, s_raw = _parse_num(txt)
        if v is not None:
            return v, s_raw

    # 4) Fallback: search for explicit CHF/Fr. mentions anywhere
    full_text = soup.get_text(' ', strip=True)
    m = re.search(r'(?:CHF|Fr\.?)([0-9\.\'"\u2019\, ]+)', full_text, flags=re.I)
    if m:
        v, s_raw = _parse_num(m.group(1))
        if v is not None:
            # We reconstruct the matched string for a nicer presentation
            found_str = m.group(0).strip() 
            return v, found_str

    return None, None


def extract_title_description(soup):
    title = ''
    desc = ''
    t = soup.find('meta', property='og:title') or soup.find('meta', attrs={'name': 'title'})
    if t and t.get('content'):
        title = t['content'].strip()
    if not title:
        h1 = soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)

    # Prefer the expanded description (read-more-target) when available
    read_more = soup.select_one('#singleDescription .read-more-target') or soup.select_one('.read-more-target')
    if read_more:
        desc = read_more.get_text(separator=' ').strip()
        return title, desc

    d = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', property='og:description')
    if d and d.get('content'):
        desc = d['content'].strip()
        return title, desc

    # fallback to first paragraph
    p = soup.find('p')
    if p:
        desc = p.get_text(strip=True)
    return title, desc


def extract_org_from_html(soup):
    """Extract organization name using microdata/schema logic."""
    # Look for [itemprop="name"] inside an [itemprop="organization"] parent
    org_node = soup.select_one('[itemprop="organization"]') or soup.select_one('[itemprop="brand"]')
    if org_node:
        name_node = org_node.select_one('[itemprop="name"]')
        if name_node:
            t = name_node.get_text(strip=True)
            if t: return t
    # Fallback: specific classes usually used for companies or agencies
    for cls in ['organization-name', 'advertiser-name', 'provider-name', 'agency-name']:
        node = soup.find(class_=re.compile(cls, re.I))
        if node:
            t = node.get_text(strip=True)
            if t: return t
    return ''


def extract_org_from_email(email):
    """Fallback: Extract potentially professional domain from email."""
    if not email or '@' not in email:
        return ''
    domain = email.split('@')[-1].lower()
    # Skip common generic providers
    generic = ['gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'bluewin.ch', 'gmx.ch', 'mail.ch', 'sunrise.ch']
    if domain in generic or '.' not in domain:
        return ''
    # Remove TLD (e.g. .ch, .com)
    org = domain.split('.')[0].capitalize()
    return org


def extract_contact_name_near_phone(soup):
    # find phone tag and then try to pick name from parent text
    for a in soup.find_all('a', href=True):
        if a['href'].lower().startswith('tel:'):
            parent = a.parent
            if parent:
                txt = parent.get_text(separator='|').strip()
                txt = re.sub(r'\+?\d[\d\-\s\(\)]{6,}\d', '', txt)
                txt = re.sub(r'\s+', ' ', txt).strip()
                # split and choose candidate pieces
                parts = [p.strip() for p in txt.split('|') if p.strip()]
                for p in parts:
                    if len(p.split()) <= 4 and len(p) > 2 and not re.search(r'@|www|http', p):
                        return p
    # find label Kontakt or Contact
    node = soup.find(string=re.compile(r'Kontakt|Contact|Ansprechpartner', re.I))
    if node:
        parent = node.parent
        if parent:
            txt = parent.get_text(separator='|')
            txt = re.sub(r'\+?\d[\d\-\s\(\)]{6,}\d', '', txt)
            parts = [p.strip() for p in txt.split('|') if p.strip()]
            for p in parts:
                if len(p.split()) <= 4 and len(p) > 2 and not re.search(r'@|www|http|CHF|Fr\.', p):
                    return p
    return ''


def extract_lat_lng(soup, page_text=None):
    """Try multiple strategies to extract latitude/longitude from a detail page."""
    # 1) data-marker attribute on #map (encoded JSON)
    marker_id = ''
    map_div = soup.find(id='map')
    if map_div:
        dm = map_div.get('data-marker') or map_div.get('dataMarker') or map_div.attrs.get('data-marker')
        if dm:
            try:
                s = html.unescape(dm)
                j = json.loads(s)
                lat = j.get('lat') or j.get('latitude')
                lng = j.get('lng') or j.get('lon') or j.get('longitude')
                marker_id = j.get('id') or j.get('markerId') or j.get('propertyId') or ''
                return (str(lat) if lat else '', str(lng) if lng else '', str(marker_id) if marker_id else '')
            except Exception:
                pass

    # 2) meta itemprop tags
    lat_meta = soup.find('meta', attrs={'itemprop': 'latitude'})
    lng_meta = soup.find('meta', attrs={'itemprop': 'longitude'})
    if lat_meta and lng_meta:
        lat = lat_meta.get('content') or lat_meta.get_text(strip=True)
        lng = lng_meta.get('content') or lng_meta.get_text(strip=True)
        return str(lat), str(lng), ''

    # 3) search in page text for JSON-like lat/lng
    txt = page_text or str(soup)
    m = re.search(r'["\']lat(?:itude)?["\']\s*:\s*["\']?([0-9\.\-]+)', txt)
    n = re.search(r'["\'](?:lng|lon|longitude)["\']\s*:\s*["\']?([0-9\.\-]+)', txt)
    if m and n:
        return m.group(1), n.group(1), ''

    return '', '', ''


def extract_vendor_id(soup, page_text=None):
    """Try to infer a vendor/provider id from the page (e.g. provider-XXXX paths)."""
    txt = page_text or str(soup)
    m = re.search(r'provider-(\d+)', txt)
    if m:
        return m.group(1)
    m = re.search(r'"provider"\s*:\s*(\d+)', txt)
    if m:
        return m.group(1)
    return None


def fetch_with_retries(session, url):
    """Fetch URL with retries (uses config.MAX_RETRIES and config.RETRY_DELAY).
    Raises last exception on failure.
    """
    last_exc = None
    max_retries = max(1, getattr(config, 'MAX_RETRIES', 1))
    retry_delay = getattr(config, 'RETRY_DELAY', 1)
    for attempt in range(1, max_retries + 1):
        try:
            return session.get(url, timeout=config.REQUEST_TIMEOUT, headers=config.REQUEST_HEADERS)
        except Exception as e:
            last_exc = e
            LOG.warning('Fetch error for %s (attempt %d/%d): %s', url, attempt, max_retries, e)
            try:
                time.sleep(retry_delay)
            except Exception:
                pass
    raise last_exc


def extract_areas(soup, page_text=None):
    """Extract living space and land area (in m2) from the page text.
    Returns (living_space, land_area) as normalized numeric strings or '' if not found.
    """
    txt = (page_text or soup.get_text(separator=' '))
    living = ''
    land = ''

    # Try common microdata itemprops
    # floorSize or floorArea or area
    fs = soup.select_one('[itemprop=floorSize]') or soup.select_one('[itemprop=floorArea]') or soup.select_one('[itemprop=area]')
    if fs:
        val = fs.get('content') or fs.get_text(strip=True)
        if val:
            living = val

    # Regex for living area: look for keywords like Wohnfläche, Wfl., living space
    if not living:
        m = re.search(r"(?:Wohnfl(?:a|ä|ae|ä)?che|Wfl\.?|Wohnfl\.?|living[ -]?space|living area)[^\d\n\r]{0,40}([0-9\.\', ]+?)\s*(?:m2|m²|m)\b", txt, re.I)
        if m:
            living = m.group(1).strip()

    # Regex for land area: Grundstück, lot size, land area
    m2 = re.search(r"(?:Grundst(?:ü|ue)ck(?:sfl[aä]che)?|Grundstueck|lot size|land area|surface area)[^\d\n\r]{0,40}([0-9\.\', ]+?)\s*(?:m2|m²|m)\b", txt, re.I)
    if m2:
        land = m2.group(1).strip()

    def _normnum(s):
        if not s:
            return ''
        s = s.replace("'", '').replace(' ', '')
        s = s.replace('\u00a0', '')
        s = s.replace(',', '.')
        return s

    return _normnum(living), _normnum(land)


ID_LOCK = getattr(__import__('threading'), 'Lock')()

def get_next_external_id():
    """Read the next available ID from last_id.txt and increment it."""
    with ID_LOCK:
        path = config.ID_PERSISTENCE_FILE
        if not os.path.exists(path):
            with open(path, 'w') as f:
                f.write(str(config.ID_RANGE_START))
            return str(config.ID_RANGE_START)
            
        with open(path, 'r') as f:
            try:
                content = f.read().strip()
                val = int(content) if content else config.ID_RANGE_START
            except (ValueError, TypeError):
                val = config.ID_RANGE_START
                
        if val >= config.ID_RANGE_END:
            LOG.error(f"FATAL ERROR: ID range limit reached ({config.ID_RANGE_END}). No more IDs can be assigned!")
            raise Exception("ID range limit reached")
            
        next_val = val + 1
        with open(path, 'w') as f:
            f.write(str(next_val))
            
        return str(next_val)


def assign_contact_id(contacts_map, phone_norm, name_norm):
    key = (phone_norm, name_norm)
    if key in contacts_map:
        return contacts_map[key]
    # create new
    new_id = get_next_external_id()
    contacts_map[key] = new_id
    return new_id


def extract_from_js(page_text):
    m = re.search(r'window\.singleJsOffer\s*=\s*(\{.*?\});\s*(?://-->|<\/script>)', page_text, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        LOG.warning("JSON decode error: %s", e)
        return None


def build_obj_row(columns, data):
    # Ensure all columns present
    row = {k: data.get(k, '') for k in columns}
    return row


def process_one(item, session):
    url = item.get('url')
    itype = item.get('type') or ''
    out = {'url': url, 'status': 'error', 'reason': '', 'extracted': {}}
    if not url:
        out['status'] = 'rejected'
        out['reason'] = 'no_url'
        return out
    try:
        r = fetch_with_retries(session, url)
    except Exception as e:
        out['status'] = 'rejected'
        out['reason'] = f'fetch_error:{e}'
        return out
    # per-worker delay throttle (set via --delay)
    try:
        if DELAY and DELAY > 0:
            time.sleep(DELAY)
    except Exception:
        pass
    if not r or getattr(r, 'status_code', None) != 200:
        out['status'] = 'rejected'
        out['reason'] = f'http_{getattr(r, "status_code", "no_response")} '
        return out
    soup = BeautifulSoup(r.text, 'html.parser')
    
    js_data = extract_from_js(r.text)
    js_phone = js_street = js_house = js_zip = js_city = js_price = ''
    js_contact = js_living = js_land = ''
    
    if js_data:
        prop = js_data.get('_embedded', {}).get('property', {})
        addr = prop.get('_embedded', {}).get('address', {})
        js_street = addr.get('street', '') or ''
        js_house = addr.get('street_number', '') or ''
        js_zip = addr.get('postal_code', '') or ''
        js_city = addr.get('locality', '') or ''
        
        org = prop.get('_embedded', {}).get('organization', {}) or {}
        vp = prop.get('_embedded', {}).get('viewPerson', {}) or {}
        ip = prop.get('_embedded', {}).get('inquiryPerson', {}) or {}
            
        js_phone = vp.get('mobile') or vp.get('phone') or ip.get('mobile') or ip.get('phone') or org.get('mobile') or org.get('phone') or ''
        js_contact = vp.get('displayName') or ip.get('displayName') or org.get('displayName') or ''
        js_org_name = org.get('name') or org.get('companyName') or org.get('legalName') or ''
        js_email = vp.get('email') or ip.get('email') or org.get('email') or ''
        
        pr = prop.get('price') or prop.get('gross_price') or prop.get('net_price')
        if pr is not None:
            js_price = pr

        num_vals = prop.get('_embedded', {}).get('numeric_values', [])
        if num_vals:
            js_living = next((str(v.get('value')) for v in num_vals if v.get('key') in ('area_bwf', 'area_sia_nf')), '')
            js_land = next((str(v.get('value')) for v in num_vals if v.get('key') in ('area_sia_gsf', 'area_plot', 'area_land')), '')

    phone = normalize_phone(js_phone) if js_phone else extract_phone(soup)
    street, house, zip_code, city = extract_address(soup)
    if js_zip and js_city:
        street = js_street
        house = js_house
        zip_code = js_zip
        city = js_city

    title, desc = extract_title_description(soup)
    contact_name = js_contact or extract_contact_name_near_phone(soup)
    org_name_html = extract_org_from_html(soup)
    lat, lng, marker_id = extract_lat_lng(soup, r.text)
    vendor = extract_vendor_id(soup, r.text)
    portal_id = marker_id or None

    living_space_area, land_area = extract_areas(soup, r.text)
    if js_living:
        living_space_area = js_living
    if js_land:
        land_area = js_land
        
    v_price, s_price = (None, None)
    if js_price != '':
        v_price = js_price
        s_price = f"CHF {js_price}.–"
    else:
        v_price, s_price = extract_price(soup)

    out['extracted'] = {
        'phone': phone,
        'street': street,
        'house_number': house,
        'zip_code': zip_code,
        'city': city,
        'price': s_price,
        'price_numeric': v_price,
        'title': title,
        'description': desc,
        'contact_name': contact_name,
        'organization_name': js_org_name or org_name_html or extract_org_from_email(js_email),
        'email': js_email if js_data else '',
        'latitude': lat,
        'longitude': lng,
        'vendor_id': vendor,
        'portal_id': portal_id,
        'living_space_area': living_space_area,
        'land_area': land_area,
        'type': itype,
    }

    # Mandatory checks
    if not phone:
        out['status'] = 'rejected'
        out['reason'] = 'missing_phone'
        return out
    if not zip_code:
        out['status'] = 'rejected'
        out['reason'] = 'missing_postal_code'
        return out

    # Determine property type
    ptype = 'unknown'
    if itype:
        ptype = itype
    else:
        lu = url.lower()
        if 'zu-kaufen' in lu or 'kauf' in lu or 'kaufen' in lu:
            ptype = 'buy'
        elif 'miet' in lu or 'mieten' in lu or 'zu-mieten' in lu:
            ptype = 'rent'

    out['extracted']['type'] = ptype

    # Filtering
    if ptype == 'rent':
        if v_price is None:
            if not STORE_NO_PRICE:
                out['status'] = 'rejected'
                out['reason'] = 'rent_no_price'
                return out
        else:
            if v_price < config.MIN_RENT_CHF:
                if not ALLOW_BELOW_MIN:
                    out['status'] = 'rejected'
                    out['reason'] = 'rent_below_min'
                    return out
    elif ptype == 'buy':
        # accept all sales
        pass
    else:
        out['status'] = 'rejected'
        out['reason'] = 'unknown_type'
        return out

    out['status'] = 'accepted'
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', type=str, default='results.jsonl', help='Input results file (results.jsonl or result.json)')
    parser.add_argument('--workers', '-w', type=int, default=10)
    parser.add_argument('--limit', type=int, default=0, help='Limit number of URLs to process (0=all)')
    parser.add_argument('--skip', type=int, default=0, help='Skip the first N items in results file')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--no-dedupe', action='store_true', help='Do not deduplicate by address (useful for testing)')
    parser.add_argument('--delay', type=float, default=0.0, help='Per-worker delay in seconds between requests')
    parser.add_argument('--store-no-price', action='store_true', help='Store rent listings even if price missing')
    parser.add_argument('--allow-below-min', action='store_true', help='Store rent listings even if price is below MIN_RENT_CHF')
    args = parser.parse_args()
    global DELAY, STORE_NO_PRICE, ALLOW_BELOW_MIN
    DELAY = float(args.delay or 0.0)
    STORE_NO_PRICE = bool(args.store_no_price)
    ALLOW_BELOW_MIN = bool(args.allow_below_min)

    root = os.path.dirname(__file__)
    out_dir = os.path.join(root, config.OUTPUT_DIR)
    os.makedirs(out_dir, exist_ok=True)

    results_path = os.path.join(out_dir, args.input)
    if not os.path.exists(results_path):
        # fallback: check if input is full path or relative to root
        if os.path.exists(args.input):
            results_path = args.input

    phase3_jsonl = os.path.join(out_dir, 'phase3_results.jsonl')
    reject_csv = os.path.join(out_dir, 'rejected.csv')
    kontakte_path = os.path.join(out_dir, config.KONTAKTE_FILENAME)
    objekte_path = os.path.join(out_dir, config.OBJEKTE_FILENAME)

    items = parse_input_results(results_path)
    if not items:
        LOG.info('No NOT FOUND entries found in %s', results_path)
        return
    LOG.info('Found %d NOT FOUND entries in %s', len(items), results_path)

    if args.skip and args.skip > 0:
        LOG.info('Skipping first %d items as requested', args.skip)
        items = items[args.skip:]

    if args.limit and args.limit > 0:
        items = items[:args.limit]

    contacts_map, existing_phones = load_existing_contacts(kontakte_path)
    props_set = load_existing_properties(objekte_path)

    # Prepare CSV writers (append mode). If a CSV is locked (PermissionError),
    # fall back to writing to a sibling fallback file so the run can continue.
    used_kontakte_path = kontakte_path
    used_objekte_path = objekte_path
    used_reject_path = reject_csv

    try:
        kontakte_file = open(kontakte_path, 'a', encoding=config.CSV_ENCODING, newline='')
    except PermissionError:
        used_kontakte_path = kontakte_path + '.fallback'
        LOG.warning('Permission denied opening %s, falling back to %s', kontakte_path, used_kontakte_path)
        kontakte_file = open(used_kontakte_path, 'a', encoding=config.CSV_ENCODING, newline='')

    try:
        objekte_file = open(objekte_path, 'a', encoding=config.CSV_ENCODING, newline='')
    except PermissionError:
        used_objekte_path = objekte_path + '.fallback'
        LOG.warning('Permission denied opening %s, falling back to %s', objekte_path, used_objekte_path)
        objekte_file = open(used_objekte_path, 'a', encoding=config.CSV_ENCODING, newline='')

    try:
        reject_file = open(reject_csv, 'a', encoding=config.CSV_ENCODING, newline='')
    except PermissionError:
        used_reject_path = reject_csv + '.fallback'
        LOG.warning('Permission denied opening %s, falling back to %s', reject_csv, used_reject_path)
        reject_file = open(used_reject_path, 'a', encoding=config.CSV_ENCODING, newline='')

    kontakte_writer = csv.DictWriter(kontakte_file, fieldnames=config.KONTAKTE_COLUMNS, delimiter=config.CSV_DELIMITER)
    objekte_writer = csv.DictWriter(objekte_file, fieldnames=config.OBJEKTE_COLUMNS, delimiter=config.CSV_DELIMITER)
    reject_writer = csv.DictWriter(reject_file, fieldnames=['url', 'reason', 'phone', 'street', 'house_number', 'zip_code', 'city', 'type', 'price'], delimiter=config.CSV_DELIMITER)

    # write headers if files were empty
    if os.path.getsize(used_kontakte_path) == 0:
        kontakte_writer.writeheader()
    if os.path.getsize(used_objekte_path) == 0:
        objekte_writer.writeheader()
    if os.path.getsize(used_reject_path) == 0:
        reject_writer.writeheader()

    lock = __import__('threading').Lock()
    # configure HTTP connection pool to support higher concurrency
    session = requests.Session()
    try:
        poolsize = max(10, args.workers * 3)
        adapter = HTTPAdapter(pool_connections=poolsize, pool_maxsize=poolsize)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
    except Exception:
        # best-effort fallback to default session
        pass

    stats = Counter()

    processed_urls = set()

    # resume: skip urls already in phase3_results.jsonl
    if args.resume and os.path.exists(phase3_jsonl):
        with open(phase3_jsonl, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    j = json.loads(line)
                    if j.get('url'):
                        processed_urls.add(j.get('url'))
                except Exception:
                    continue

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {}
        for item in items:
            if item.get('url') in processed_urls:
                stats['skipped_already_processed'] += 1
                continue
            fut = ex.submit(process_one, item, session)
            futures[fut] = item

        for fut in as_completed(futures):
            item = futures[fut]
            url = item.get('url')
            try:
                res = fut.result()
            except Exception as e:
                LOG.exception('Error processing %s: %s', url, e)
                stats['errors'] += 1
                continue
            # write raw result to phase3 jsonl
            with lock:
                with open(phase3_jsonl, 'a', encoding='utf-8') as pj:
                    pj.write(json.dumps(res, ensure_ascii=False) + '\n')

            if res['status'] != 'accepted':
                stats['rejected'] += 1
                stats[res.get('reason', 'unknown_reason')] += 1
                # record rejection
                exd = res.get('extracted', {})
                with lock:
                    reject_writer.writerow({
                        'url': url,
                        'reason': res.get('reason', ''),
                        'phone': exd.get('phone', ''),
                        'street': exd.get('street', ''),
                        'house_number': exd.get('house_number', ''),
                        'zip_code': exd.get('zip_code', ''),
                        'city': exd.get('city', ''),
                        'type': exd.get('type', ''),
                        'price': exd.get('price', ''),
                    })
                continue

            stats['accepted'] += 1
            exd = res['extracted']
            phone_norm = normalize_phone(exd.get('phone'))
            name_raw = exd.get('contact_name') or ''
            name_norm = normalize_name(name_raw)
            street = exd.get('street') or ''
            house = exd.get('house_number') or ''
            zipc = exd.get('zip_code') or ''
            city = exd.get('city') or ''
            addr_norm = normalize_addr(street, house, zipc, city)

            # deduplicate by address (skip when --no-dedupe is set)
            if not args.no_dedupe and addr_norm and addr_norm in props_set:
                stats['dedup_address'] += 1
                continue

            # deduplicate by contact (phone+name)
            contact_key = (phone_norm, name_norm)
            if contact_key in contacts_map:
                contact_id = contacts_map[contact_key]
                stats['reused_contact'] += 1
            else:
                contact_id = assign_contact_id(contacts_map, phone_norm, name_norm)
                # write contact row
                first = ''
                last = ''
                org = ''
                if name_raw:
                    parts = name_raw.split(None, 1)
                    first = parts[0]
                    last = parts[1] if len(parts) > 1 else ''
                
                js_org = exd.get('organization_name') or ''
                js_email = exd.get('email') or ''
                
                final_first = first
                final_last = last
                final_org = js_org or org
                
                # If name looks like a company or URL, move it to organization_name
                full_name_raw = f"{first} {last}".strip()
                if not final_org and (re.search(r'\.|www\b|http\b|\bAG\b|\bGmbH\b', full_name_raw, re.I) or len(full_name_raw.split()) > 3):
                    final_org = full_name_raw
                    final_first = ''
                    final_last = ''

                contact_row = {
                    'external_id': contact_id,
                    'first_name': final_first,
                    'last_name': final_last,
                    'organization_name': final_org,
                    'email': js_email,
                    'phone': exd.get('phone', ''),
                    'street': street,
                    'house_number': house,
                    'zip_code': zipc,
                    'city': city,
                    'normalized_phone': phone_norm,
                    'portal_id': config.PORTAL_ID,
                    'vendor_id': config.VENDOR_ID,
                }
                with lock:
                    kontakte_writer.writerow(contact_row)
                    kontakte_file.flush()
                stats['new_contacts'] += 1

            # write property row
            type_id = '1' if exd.get('type') == 'buy' else '2'
            price_val = exd.get('price') or ''
            obj_row = {
                'contact_external_id': contact_id,
                'portal_id': config.PORTAL_ID,
                'vendor_id': config.VENDOR_ID,
                'type_id': type_id,
                'detail_url': url,
                'title': exd.get('title', ''),
                'description': exd.get('description', ''),
                'street': street,
                'house_number': house,
                'zip_code': zipc,
                'city': city,
                'latitude': exd.get('latitude', ''),
                'longitude': exd.get('longitude', ''),
                'price': price_val if price_val is not None else '',
                'living_space_area': exd.get('living_space_area', ''),
                'land_area': exd.get('land_area', ''),
                'rs_category_id': '1',
                'price_value': price_val if price_val is not None else '',
                'advertiser_id': '',
            }
            with lock:
                objekte_writer.writerow(obj_row)
                objekte_file.flush()
                props_set.add(addr_norm)

    # close files
    kontakte_file.close()
    objekte_file.close()
    reject_file.close()

    # print summary
    LOG.info('Phase 3 scraping complete')
    LOG.info('Stats: %s', dict(stats))


if __name__ == '__main__':
    main()
