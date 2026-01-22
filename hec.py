#!/usr/bin/env python3
import os
import csv
import json
from urllib.request import Request, urlopen

HEC_URL = os.getenv(
    "SPLUNK_HEC_URL",
    "https://192.168.64.1:8088/services/collector/event",
)
HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN", "01af8ecd-ff1b-4c44-a3fc-611bf657fc1b")
HEC_INDEX = os.getenv("SPLUNK_HEC_INDEX", "feed_test")
HEC_SOURCETYPE = os.getenv("SPLUNK_HEC_SOURCETYPE", "feed_test")
CSV_PATH = os.getenv("OTX_CSV_PATH", "otx_iocs.csv")

def send_batch(events):
    payload = "\n".join(json.dumps(e) for e in events).encode("utf-8")
    headers = {
        "Authorization": f"Splunk {HEC_TOKEN}",
        "Content-Type": "application/json",
        "Content-Length": str(len(payload)),
    }
    req = Request(HEC_URL, data=payload, headers=headers, method="POST")
    with urlopen(req, timeout=30) as resp:
        resp.read()

def main():
    events = []
    batch_size = 500

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_body = {
                "indicator": row.get("indicator"),
                "indicator_type": row.get("indicator_type"),
                "otx_type": row.get("otx_type"),
                "pulse_name": row.get("pulse_name"),
                "pulse_id": row.get("pulse_id"),
                "title": row.get("title"),
                "description": row.get("description"),
                "pulse_created": row.get("pulse_created"),
                "pulse_modified": row.get("pulse_modified"),
            }

            wrapper = {
                "index": HEC_INDEX,
                "sourcetype": HEC_SOURCETYPE,
                "event": event_body,
            }

            events.append(wrapper)

            if len(events) >= batch_size:
                send_batch(events)
                events = []

    if events:
        send_batch(events)

if __name__ == "__main__":
    main()

