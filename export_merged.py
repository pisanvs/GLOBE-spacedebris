# Disclaimer: This code is MEGA inefficient in terms of CPU, but it is all in favor of avoiding OOM errors (my 16 GB mahcine crashed multiple times before refactoring it)

import ijson
import csv
import pickle
import os
from datetime import datetime

def is_numeric(value):
    """Return True if value can be cast to float."""
    try:
        float(value)
        return True
    except Exception:
        return False

# ------------------------
# Phase 1: Determine Union of Columns
# ------------------------
union_keys = set()

with open('outputs/measurements.json', 'rb') as f:
    parser = ijson.items(f, 'item')
    for reentry in parser:
        results = reentry[2]['results']
        union_keys.add("date")
        union_keys.add("distance")
        union_keys.add("object")
        for item in results:
            for key in item['data'].keys():
                if "Userid" in key or "Comments" in key or "Measured" in key:
                    continue
                union_keys.add(key)

# Establish a consistent column order: "date" and "object" first, then the rest sorted alphabetically.
other_cols = sorted(union_keys - {"date", "object", "distance"})
cols = ["date", "object", "distance"] + other_cols

# ------------------------
# Phase 2: Chunked Aggregation (Streaming Aggregation)
# ------------------------

# Parameters: adjust CHUNK_SIZE based on available memory.
CHUNK_SIZE = 100000  # maximum number of unique groups in one chunk
temp_files = []       # will hold names of temporary pickle files
aggregator = {}       # key: (object, date) --> value: (merged_row, counts_dict)
# counts_dict stores, for each numeric column, the number of values merged so far.

def flush_aggregator(agg, chunk_index):
    """Flush the current aggregator to a temporary file."""
    filename = f"temp_chunk_{chunk_index}.pkl"
    with open(filename, 'wb') as pf:
        pickle.dump(agg, pf)
    return filename

chunk_index = 0

from math import cos, asin, sqrt, pi
# Haversine formula for geodesic approximation
def distance(lat1, lon1, lat2, lon2):
    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)
    r = 6371 # km
    p = pi / 180

    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 2 * r * asin(sqrt(a))


with open('outputs/measurements.json', 'rb') as f:
    parser = ijson.items(f, 'item')
    for reentry in parser:
        results = reentry[2]['results']
        for item in results:
            # Build a row dictionary for this measurement.
            row = {col: None for col in cols}
            # Construct the date key using protocol – e.g., "foo_bar" becomes "foobarMeasuredAt"
            protocol_key = f"{item['protocol'].replace('_', '')}MeasuredAt"
            row_date = item['data'].get(protocol_key, None)
            row["date"] = row_date
            row["object"] = reentry[1]
            row["distance"] = distance(item["latitude"], item["longitude"], reentry[3][0], reentry[3][1])

            # Fill in other fields from item['data']
            for key, value in item['data'].items():
                if "Userid" in key or "Comments" in key or "Measured" in key:
                    continue
                try:
                    row[key] = float(value)
                except Exception:
                    row[key] = value

            # Optionally, standardize the date format.
            if row["date"]:
                try:
                    dt = datetime.strptime(row["date"], "%Y-%m-%dT%H:%M:%S")
                    row["date"] = dt.isoformat()
                except Exception:
                    pass

            group_key = (row["object"], row["date"], row["distance"])

            # Aggregation logic: if group already exists, merge new row with the existing one.
            if group_key not in aggregator:
                # Initialize with the current row and create counts for numeric fields.
                counts = {}
                for col, val in row.items():
                    if val is not None and is_numeric(val):
                        counts[col] = 1
                aggregator[group_key] = (row, counts)
            else:
                existing_row, counts = aggregator[group_key]
                for col in row:
                    new_val = row[col]
                    if new_val is not None:
                        if existing_row[col] is None:
                            existing_row[col] = new_val
                            if is_numeric(new_val):
                                counts[col] = 1
                        else:
                            # If both values are numeric, update using a running average.
                            if is_numeric(existing_row[col]) and is_numeric(new_val):
                                cnt = counts.get(col, 1)
                                try:
                                    avg = (float(existing_row[col]) * cnt + float(new_val)) / (cnt + 1)
                                    existing_row[col] = avg
                                    counts[col] = cnt + 1
                                except Exception:
                                    pass
                            # For non-numeric fields, we keep the existing value.
                aggregator[group_key] = (existing_row, counts)
        # Check if aggregator has grown too big; flush if necessary.
        if len(aggregator) >= CHUNK_SIZE:
            temp_filename = flush_aggregator(aggregator, chunk_index)
            temp_files.append(temp_filename)
            aggregator = {}  # reset for next chunk
            chunk_index += 1

# Flush any remaining aggregator data.
if aggregator:
    temp_filename = flush_aggregator(aggregator, chunk_index)
    temp_files.append(temp_filename)
    aggregator = {}

# ------------------------
# Phase 3: Merge Temporary Chunks
# ------------------------
final_aggregator = {}

def merge_aggregators(target, source):
    """Merge two aggregator dictionaries (source into target)."""
    for group_key, (row, counts) in source.items():
        if group_key not in target:
            target[group_key] = (row, counts)
        else:
            existing_row, existing_counts = target[group_key]
            for col in row:
                new_val = row[col]
                if new_val is not None:
                    if existing_row[col] is None:
                        existing_row[col] = new_val
                        if is_numeric(new_val):
                            existing_counts[col] = counts.get(col, 1)
                    else:
                        if is_numeric(existing_row[col]) and is_numeric(new_val):
                            cnt = existing_counts.get(col, 1)
                            try:
                                avg = (float(existing_row[col]) * cnt + float(new_val)) / (cnt + 1)
                                existing_row[col] = avg
                                existing_counts[col] = cnt + 1
                            except Exception:
                                pass
            target[group_key] = (existing_row, existing_counts)
    return target

# Iterate over each temporary file and merge it into final_aggregator.
for filename in temp_files:
    with open(filename, 'rb') as pf:
        chunk_data = pickle.load(pf)
    final_aggregator = merge_aggregators(final_aggregator, chunk_data)
    os.remove(filename)  # Remove the temporary file after merging.

# ------------------------
# Phase 4: Remove Columns with Any Non-Numerical Values and Write Final Aggregated Results to CSV
# ------------------------
# Determine which columns have exclusively numeric (or None) values.
columns_to_keep = ["date", "object", "distance"]
for col in cols:
    keep = True
    for (obj, date, dist), (row, counts) in final_aggregator.items():
        val = row.get(col)
        # Treat None as acceptable, but if any non-None value isn't numeric, mark for removal.
        if val is not None and not is_numeric(val):
            keep = False
            break
    if keep:
        columns_to_keep.append(col)

with open('outputs/output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=columns_to_keep)
    writer.writeheader()
    for (obj, date, dist), (row, counts) in final_aggregator.items():
        # Remove keys not in columns_to_keep.
        filtered_row = {col: row[col] for col in columns_to_keep}
        writer.writerow(filtered_row)

print("Aggregation complete. Output written to output.csv")
