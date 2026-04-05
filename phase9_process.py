import pandas as pd
import re
import os
import config

def clean_price(val: str) -> str:
    """Extract only digits from the price string."""
    if not val or pd.isna(val):
        return ""
    # Remove everything except digits
    return re.sub(r'\D', '', str(val))

def run_phase9():
    out_dir = config.OUTPUT_DIR
    
    # Input from Phase 8
    input_objekte = os.path.join(out_dir, "Phase8_Objekte_Final.csv")
    input_kontakte = os.path.join(out_dir, "Phase8_Kontakte_Final.csv")

    # Output for Phase 9
    output_objekte = os.path.join(out_dir, "Phase9_Objekte.csv")
    output_kontakte = os.path.join(out_dir, "Phase9_Kontakte.csv")

    if not os.path.exists(input_objekte):
        print(f"Error: Phase 8 input not found at {input_objekte}")
        return

    print("Reading Phase 8 files ...")
    df_obj = pd.read_csv(input_objekte, dtype=str, keep_default_na=False)
    
    # Process Objekte
    print("Processing Objekte: Removing external_id and cleaning price_value ...")
    
    # 1. Remove external_id column if it exists
    if 'external_id' in df_obj.columns:
        df_obj = df_obj.drop(columns=['external_id'])
    
    # 2. Clean price_value column
    if 'price_value' in df_obj.columns:
        df_obj['price_value'] = df_obj['price_value'].apply(clean_price)

    print(f"Saving {output_objekte} ...")
    df_obj.to_csv(output_objekte, index=False, encoding="utf-8-sig")

    # Process Kontakte (just rename/copy essentially)
    if os.path.exists(input_kontakte):
        print("Reading Phase 8 Kontakte ...")
        df_kont = pd.read_csv(input_kontakte, dtype=str, keep_default_na=False)
        print(f"Saving {output_kontakte} ...")
        df_kont.to_csv(output_kontakte, index=False, encoding="utf-8-sig")
    else:
        print("Warning: Phase 8 Kontakte file not found. Skipping.")

    print("Phase 9 complete.")

if __name__ == '__main__':
    run_phase9()
