import ijson
import csv
from datetime import datetime

# --- First Pass: Determine the union of all columns ---

# We want "date" and "object" always, plus all keys from each item’s data (except those containing "Userid")
union_keys = set()

with open('outputs/measurements.json', 'rb') as f:
    parser = ijson.items(f, 'item')
    for reentry in parser:
        results = reentry[2]['results']
        union_keys.add("date")
        union_keys.add("object")
        for item in results:
            for key in item['data'].keys():
                if "Userid" in key:
                    continue
                union_keys.add(key)

other_cols = sorted(union_keys - {"date", "object"})
cols = ["date", "object"] + other_cols

# --- Second Pass: Write rows with aligned columns ---

with open('outputs/measurements.json', 'rb') as f, open('output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=cols)
    writer.writeheader()

    parser = ijson.items(f, 'item')
    for reentry in parser:
        results = reentry[2]['results']
        for item in results:
            row = {col: None for col in cols}
            row["date"] = item['data'][f"{item['protocol'].replace("_", "")}MeasuredAt"]
            row["object"] = reentry[1]
            for key, value in item['data'].items():
                if "Userid" in key:
                    continue
                try:
                    row[key] = float(value)
                except:
                    continue

            try:
                dt = datetime.strptime(row["date"], "%Y-%m-%dT%H:%M:%S")
                row["date"] = dt.isoformat()
            except Exception:
                pass

            writer.writerow(row)
