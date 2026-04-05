import csv
import json
import os
import concurrent.futures
import requests
import config

def check_contact(contact):
    payload = {}
    if contact.get('first_name') and contact['first_name'].strip():
        payload['first_name'] = contact['first_name'].strip()
    if contact.get('last_name') and contact['last_name'].strip():
        payload['last_name'] = contact['last_name'].strip()
    if contact.get('organization_name') and contact['organization_name'].strip():
        payload['organization_name'] = contact['organization_name'].strip()
    
    # Use normalized_phone first, fallback to phone
    phone = contact.get('normalized_phone', '').strip() or contact.get('phone', '').strip()
    if phone:
        payload['phone'] = phone
    
    if contact.get('email') and contact['email'].strip():
        payload['email'] = contact['email'].strip()

    ext_id = contact.get('external_id')

    try:
        resp = requests.post('https://api.we-net.ch/api/advertisers/check', json=payload, timeout=15)
        try:
            data = resp.json()
            return ext_id, data
        except Exception:
            if resp.status_code == 200:
                return ext_id, {"error": "Not JSON but 200"}
            else:
                return ext_id, {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return ext_id, {"error": str(e)}

def run_phase5():
    out_dir = config.OUTPUT_DIR
    
    # Phase 5 takes Phase 4's FINAL output as its INPUT
    input_kontakte = os.path.join(out_dir, 'Phase4_Kontakte_Final.csv')
    input_objekte = os.path.join(out_dir, 'Phase4_Objekte_Final.csv')

    output_kontakte_final = os.path.join(out_dir, 'Phase5_Kontakte_Final.csv')
    output_kontakte_rejected = os.path.join(out_dir, 'Phase5_Kontakte_Rejected.csv')
    output_objekte_final = os.path.join(out_dir, 'Phase5_Objekte_Final.csv')
    output_objekte_rejected = os.path.join(out_dir, 'Phase5_Objekte_Rejected.csv')

    if not os.path.exists(input_kontakte):
        print(f"Error: Phase 4 input not found at {input_kontakte}")
        return

    contacts = []
    with open(input_kontakte, 'r', encoding=config.CSV_ENCODING) as f:
        reader = csv.DictReader(f)
        for row in reader:
            contacts.append(row)

    if not contacts:
        print("No contacts found from Phase 4 to validate.")
        return

    print(f"Loaded {len(contacts)} contacts from Phase 4. Starting CRM API checks...")

    contact_status = {}
    success_count = 0
    blocked_count = 0
    found_count = 0
    not_found_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_contact, c): c for c in contacts}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            ext_id, result = future.result()
            
            action = 'keep'
            adv_id = None
            
            if 'error' in result:
                # If API fails, we keep them as a safety measure (don't lose data)
                action = 'keep'
            else:
                success_count += 1
                is_blocked = result.get('blocked') is True
                is_found = result.get('found') is True

                if is_blocked:
                    action = 'drop'
                    blocked_count += 1
                elif is_found:
                    action = 'keep' 
                    adv_id = result.get('id')
                    found_count += 1
                else:
                    action = 'keep'
                    not_found_count += 1
            
            contact_status[ext_id] = {'action': action, 'adv_id': adv_id}

    print(f"CRM Check Complete: {blocked_count} Blocked, {found_count} Duplicates, {not_found_count} New.")

    # Write Contacts
    with open(output_kontakte_final, 'w', encoding=config.CSV_ENCODING, newline='') as f_final, \
         open(output_kontakte_rejected, 'w', encoding=config.CSV_ENCODING, newline='') as f_rej:
        
        writer_final = csv.DictWriter(f_final, fieldnames=contacts[0].keys())
        writer_rej = csv.DictWriter(f_rej, fieldnames=contacts[0].keys())
        writer_final.writeheader()
        writer_rej.writeheader()

        for c in contacts:
            # Use str() for safety to match any ID format
            ext_id = str(c.get('external_id', ''))
            status = contact_status.get(ext_id)
            if status and status['action'] == 'keep':
                writer_final.writerow(c)
            else:
                writer_rej.writerow(c)

    # Write Objects
    with open(input_objekte, 'r', encoding=config.CSV_ENCODING) as f_in, \
         open(output_objekte_final, 'w', encoding=config.CSV_ENCODING, newline='') as f_final, \
         open(output_objekte_rejected, 'w', encoding=config.CSV_ENCODING, newline='') as f_rej:
        
        reader = csv.DictReader(f_in)
        writer_final = csv.DictWriter(f_final, fieldnames=reader.fieldnames)
        writer_rej = csv.DictWriter(f_rej, fieldnames=reader.fieldnames)
        writer_final.writeheader()
        writer_rej.writeheader()

        for row in reader:
            # Use str() for safety to match any ID format
            ext_id = str(row.get('contact_external_id', ''))
            status = contact_status.get(ext_id)
            if status and status['action'] == 'keep':
                # If an advertiser ID was returned by the API, update the row
                if status.get('adv_id'):
                    row['advertiser_id'] = status['adv_id']
                writer_final.writerow(row)
            else:
                writer_rej.writerow(row)

    print(f"Phase 5 Complete. Results saved in {out_dir}")

if __name__ == '__main__':
    run_phase5()
