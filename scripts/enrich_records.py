#!/usr/bin/env python
"""
Script to enrich data from JSON files

Usage:
    $ python enrich_records.py ingesiton_document_id
"""
import os
import sys
import tempfile
import argparse
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

def create_enrich_dir(provider):
    return tempfile.mdktemp(provider)

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    if getprop(ingestion_doc, "fetch_process/status") != "complete":
        print "Cannot enrich, fetch process did not complete"
        return -1

    # Update ingestion document
    enrich_dir = create_enrich_dir(ingestion_doc["provider"])
    kwargs = {
        "enrich_process/status": "running",
        "enrich_process/data_dir": enrich_dir,
        "enrich_process/start_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    error_msg = None
    fetch_dir = getprop(ingestion_doc, "fetch_process/data_dir")
    for filename in os.listdir(fetch_dir):
        filepath = os.join(fetch_dir, file)
        with open(filepath, "r") as f:
            try:
                data = json.loads(f)
            except:
                error_msg.append("Error loading " + filepath)
                continue

        # Enrich
        resp, content = H.request("/enrich", json.dumps(data))
        if resp != 200:
            error_msg.append("Error enriching data from " + filepath)
            continue

        # Write enriched data to file
        with open(os.join(enrich_dir, filename), "w") as f:
            json.dumps(content, f)

    # Update ingestion document
    try os.rmdir(enrich_dir):
        # Error if enrich_dir was empty
        status = "error"
    except:
        status = "complete"
    kwargs = {
        "enrich_process/status": status,
        "enrich_process/error": error_msg,
        "enrich_process/end_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
