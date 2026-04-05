import csv
import os
import re
import config

def clean_phase4():
    # Use config for directory
    out_dir = config.OUTPUT_DIR
    
    input_kontakte = os.path.join(out_dir, 'Kontakte.csv')
    fallback_k = input_kontakte + '.fallback'
    if os.path.exists(fallback_k):
        input_kontakte = fallback_k
            
    input_objekte = os.path.join(out_dir, 'Objekte.csv')
    fallback_o = input_objekte + '.fallback'
    if os.path.exists(fallback_o):
        input_objekte = fallback_o

    # Final outputs
    output_kontakte_final = os.path.join(out_dir, 'Phase4_Kontakte_Final.csv')
    output_objekte_final = os.path.join(out_dir, 'Phase4_Objekte_Final.csv')
    
    # Rejected outputs
    output_kontakte_rejected = os.path.join(out_dir, 'Phase4_Kontakte_Rejected.csv')
    output_objekte_rejected = os.path.join(out_dir, 'Phase4_Objekte_Rejected.csv')

    agency_keywords = [
        r'\bAG\b', r'\bGmbH\b', r'\bSA\b', r'\bS\.A\.\b', r'\bSàrl\b', r'\bSarl\b', r'\bSagl\b',
        r'\bImmo', r'\bimmo', r'Immobilien', r'Immobiliare', r'Immobilier', 
        r'Real\s?Estate', r'Treuhand', r'Courtage', r'Verkauf', r'Secretariat', r'Sekretariat',
        r'Service', r'Agence', r'\bPartner\b', r'Bureau', r'Invest\b', r'Investments?', r'Group', 
        r'Properties', r'\bBau', r'Architektur', r'Homefinders', r'Promotion', r'Maison',
        r'Team\b', r'Abteilung', r'Fiduciaire', r'Verwaltung', r'Management', r'Consulting', r'Generalunternehmung',
        r'\.ch\b', r'\.com\b', r'www\.'
    ]
    agency_pattern = re.compile('|'.join(agency_keywords), re.IGNORECASE)

    rejected_contact_ids = set()
    accepted_contact_ids = set()

    # --- 1. Clean Contacts ---
    with open(input_kontakte, 'r', encoding=config.CSV_ENCODING) as f_in, \
         open(output_kontakte_final, 'w', encoding=config.CSV_ENCODING, newline='') as f_acc, \
         open(output_kontakte_rejected, 'w', encoding=config.CSV_ENCODING, newline='') as f_rej:
        
        reader = csv.DictReader(f_in)
        acc_writer = csv.DictWriter(f_acc, fieldnames=reader.fieldnames)
        acc_writer.writeheader()
        rej_writer = csv.DictWriter(f_rej, fieldnames=reader.fieldnames)
        rej_writer.writeheader()

        for row in reader:
            first = row.get('first_name', '').strip()
            last = row.get('last_name', '').strip()
            org = row.get('organization_name', '').strip()
            
            # 1. Check if organization name matches agency keywords
            is_rejected = False
            reason = ""
            
            if org and agency_pattern.search(org):
                is_rejected = True
                reason = f"Organization matches agency pattern: {org}"

            # 2. Check if First or Last Name fields themselves contain company keywords
            # (Matches user's instruction: "If you saw a company name [in first/last] then skip it")
            if first and agency_pattern.search(first):
                is_rejected = True
                reason = f"First name matches agency pattern: {first}"
            if last and agency_pattern.search(last):
                is_rejected = True
                reason = f"Last name matches agency pattern: {last}"
            
            # 3. Check for Proper Person Name (Override)
            # (Matches user's instruction: "If see a person name that keep it")
            is_proper_person = False
            if first and last:
                full_name = f"{first} {last}".strip()
                # A proper person name shouldn't have agency keywords
                if not agency_pattern.search(full_name):
                    parts = full_name.split()
                    # Real names usually have 2-4 parts and each part has multiple letters
                    if 2 <= len(parts) <= 4:
                        if all(len(p) > 1 for p in parts):
                            is_proper_person = True
            
            if is_proper_person:
                is_rejected = False
                reason = f"Accepted as proper person name: {first} {last}"
            
            # 4. Demo Mode Bypass
            if config.DEMO_MODE:
                is_rejected = False
                if not is_proper_person:
                    reason = "DEMO_MODE: Accepting all for testing"
            
            if is_rejected:
                rejected_contact_ids.add(row['external_id'])
                rej_writer.writerow(row)
            else:
                accepted_contact_ids.add(row['external_id'])
                acc_writer.writerow(row)

    print(f"Phase 4: Kept {len(accepted_contact_ids)} contacts. Rejected {len(rejected_contact_ids)}.")

    # --- 2. Clean Properties ---
    with open(input_objekte, 'r', encoding=config.CSV_ENCODING) as f_in, \
         open(output_objekte_final, 'w', encoding=config.CSV_ENCODING, newline='') as f_acc, \
         open(output_objekte_rejected, 'w', encoding=config.CSV_ENCODING, newline='') as f_rej:
        
        reader = csv.DictReader(f_in)
        acc_writer = csv.DictWriter(f_acc, fieldnames=reader.fieldnames)
        acc_writer.writeheader()
        rej_writer = csv.DictWriter(f_rej, fieldnames=reader.fieldnames)
        rej_writer.writeheader()

        kept_obj = 0
        rej_obj = 0
        for row in reader:
            contact_id = row.get('contact_external_id')
            price = row.get('price', '').strip()
            
            # Reject if contact was rejected
            if contact_id in rejected_contact_ids:
                rej_writer.writerow(row)
                rej_obj += 1
                continue
            
            # Reject if NO price
            if not price:
                rej_writer.writerow(row)
                rej_obj += 1
                continue
                
            acc_writer.writerow(row)
            kept_obj += 1
            
    print(f"Phase 4: Kept {kept_obj} properties. Rejected {rej_obj}.")

if __name__ == '__main__':
    clean_phase4()
