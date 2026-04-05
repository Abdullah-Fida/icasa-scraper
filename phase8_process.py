import pandas as pd
import re
import os
import config

def run_phase8():
    out_dir = config.OUTPUT_DIR
    
    # Take Phase 5 directly as input
    input_objekte = os.path.join(out_dir, "Phase5_Objekte_Final.csv")
    input_kontakte = os.path.join(out_dir, "Phase5_Kontakte_Final.csv")

    output_objekte = os.path.join(out_dir, "Phase8_Objekte_Final.csv")
    output_kontakte = os.path.join(out_dir, "Phase8_Kontakte_Final.csv")

    if not os.path.exists(input_kontakte):
        print(f"Error: Phase 5 input not found at {input_kontakte}")
        return

    print("Reading Phase 5 files …")
    df_obj = pd.read_csv(input_objekte, dtype=str, keep_default_na=False)
    df_kont = pd.read_csv(input_kontakte, dtype=str, keep_default_na=False)
    print(f"  Objekte  : {len(df_obj):,} rows")
    print(f"  Kontakte : {len(df_kont):,} rows")

    # ───── DELETED: ID Simplification (1, 2, 3...) ─────
    # We are KEEPING the 200,000+ ids from Phase 3 as requested.
    # ───── DELETED ─────

    # Map rs_category_id from detail_url
    RE_SALE = re.compile(r'-zu-(?:kaufen|mieten)-')

    def get_cat(url: str) -> str:
        if not url: return "11"
        path = url.lower().split("icasa.ch/", 1)[-1]
        m = RE_SALE.search(path)
        slug = path[:m.start()] if m else path.split("/")[0]

        # Apartment types (id 1)
        if "penthouse"              in slug: return "4"
        if "maisonette"             in slug: return "3"
        if "attikawohnung"          in slug: return "10"
        if "attika"                 in slug: return "10"
        if "dachwohnung"            in slug: return "1"
        if "dachgeschoss"           in slug: return "1"
        if "loft"                   in slug: return "2"
        if "gartenwohnung"          in slug: return "6"
        if "erdgeschosswohnung"     in slug: return "6"
        if "erdgeschoss-wohnung"    in slug: return "6"
        if "terrassenwohnung"       in slug: return "5"
        if "terrassen-wohnung"      in slug: return "5"
        if "etagenwohnung"          in slug: return "7"

        # House types (id 2)
        if "villa"                  in slug: return "24"
        if "chalet"                 in slug: return "28"
        if "doppeleinfamilienhaus"  in slug: return "23"
        if "reiheneinfamilienhaus"  in slug: return "14"
        if "reihenhaus"             in slug: return "15"
        if "terrassenhaus"          in slug: return "14"
        if "townhouse"              in slug: return "19"
        if "zweifamilienhaus"       in slug: return "13"
        if "mehrfamilienhaus"       in slug: return "18"
        if "einfamilienhaus"        in slug: return "12"
        if "bauernhaus"             in slug: return "22"
        if "rustico"                in slug: return "22"
        if "bungalow"               in slug: return "21"

        if "wohnung"                in slug: return "7"
        if "haus"                   in slug: return "12"

        # Land
        if any(x in slug for x in ["bauland", "grundstueck", "grundstuck", "parzelle", "baugrundst"]): return "128"

        # Commercial
        if any(x in slug for x in ["buero", "buro"]): return "42"
        if "ausstellungsflaeche"    in slug: return "63"
        if "gewerbe"                in slug: return "88"
        if "industrie"              in slug: return "89"
        if "laden"                  in slug: return "81"
        if "verkaufsflaeche"        in slug: return "83"

        # Parking
        if "garage"                 in slug: return "33"
        if "parkplatz"              in slug: return "34"
        if "parking"                in slug: return "37"
        if "hallenplatz"            in slug: return "33"

        # Hospitality
        if "hotel"                  in slug: return "58"
        if "restaurant"             in slug: return "62"

        return "11" # OTHER

    print("Mapping categories …")
    df_obj["rs_category_id"] = df_obj["detail_url"].apply(get_cat)

    print("Saving final results …")
    df_obj.to_csv(output_objekte, index=False, encoding="utf-8-sig")
    df_kont.to_csv(output_kontakte, index=False, encoding="utf-8-sig")
    print(f"Phase 8 complete. Files in {out_dir}")

if __name__ == '__main__':
    run_phase8()
