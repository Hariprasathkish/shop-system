import sqlite3
import json

conn = sqlite3.connect('shop_system.db')
cur = conn.cursor()
cur.execute("PRAGMA table_info(snacks_menu)")
cols = cur.fetchall()
res = []
for c in cols:
    res.append({
        "cid": c[0],
        "name": c[1],
        "type": c[2],
        "notnull": c[3],
        "dflt_value": c[4],
        "pk": c[5]
    })
print(json.dumps(res, indent=2))
conn.close()
