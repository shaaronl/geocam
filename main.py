import requests
import json
import os
import csv
import time

API_URL = "https://production.geocam.io/arcgis/rest/services/cell+zj9iod9+156/FeatureServer/0/query"
BASE_URL = "https://production.geocam.io"

os.makedirs("images", exist_ok=True)

csv_file = open("metadata.csv", "w", newline="")
writer = csv.writer(csv_file)
writer.writerow([
    "shot_id",
    "camera",
    "latitude",
    "longitude",
    "yaw",
    "capture",
    "segment",
    "utc_time",
    "image_file"
])

offset = 0
batch_size = 10
total_downloaded = 0
while True:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "resultRecordCount": batch_size,
        "resultOffset": offset,
    }

    resp = requests.get(API_URL, params=params)
    data = resp.json()

    features = data.get("features", [])
    if not features:
        break
    print(f"Fetched {len(features)} shots at offset {offset}")

    for feature in features:
        attrs = feature["attributes"]

        shot_id = attrs["id"]
        lat = attrs["latitude"]
        lon = attrs["longitude"]
        yaw = attrs["yaw"]
        capture = attrs["capture"]
        segment = attrs["segment"]
        utc_time = attrs["utc_time"]

        filenames_str = attrs["filenames"]
        filenames = json.loads(filenames_str)

        for path in filenames:
            parts = path.split("/")
            camera = parts[-3]         
            img_name = parts[-1]
            url = BASE_URL + path
            save_name = f"{shot_id}_cam{camera}.jpg"
            save_path = os.path.join("images", save_name)
            # download image
            r = requests.get(url)
            if r.status_code == 200:
                with open(save_path, "wb") as f:
                    f.write(r.content)
                writer.writerow([
                    shot_id,
                    camera,
                    lat,
                    lon,
                    yaw,
                    capture,
                    segment,
                    utc_time,
                    save_path
                ])
                total_downloaded += 1
                print("Downloaded", save_name)
            else:
                print("Failed", url, r.status_code)
        time.sleep(0.05)
    offset += batch_size
csv_file.close()
print("images downloaded:", total_downloaded)
