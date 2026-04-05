import csv
p='output/Objekte.csv.fallback'
try:
    with open(p,'r',encoding='utf-8') as f:
        r=csv.DictReader(f)
        for i,row in enumerate(r):
            print(f"{i+1}\t{row.get('detail_url','')}\tliving={row.get('living_space_area','')}\tland={row.get('land_area','')}")
            if i>=19:
                break
except FileNotFoundError:
    print(p+" not found")
except Exception as e:
    print('error', e)
