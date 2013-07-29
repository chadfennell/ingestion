#!/usr/bin/env python
"""
Script to save data from JSON files to the CouchDB database

Usage:
    $ python save_records.py ingestion_document_id
"""
import os
import sys
import argparse
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

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
    if getprop(ingestion_doc, "enrich_process/status") != "complete":
        print "Cannot save, enrich process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "save_process/status": "running",
        "save_process/start_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    # Back up provider data
    resp = couch.back_up_data_for(ingestion_doc["provider"])
    if resp == -1:
        # Fatal error, do not continue with save process
        kwargs = {
            "save_process/status": "error",
            "save_process/end_time": datetime.now().isoformat(),
            "save_process/error": "Error backing up DPLA records"
        }
        couch._update_ingestion_doc(ingestion_doc, kwargs)
        return -1 

    error_msg = None
    enrich_dir = getprop(ingestion_doc, "enrich_process/data_dir")
    for file in os.listdir(enrich_dir):
        filename = os.join(enrich_dir, file)
        with open(filename, "r") as f:
            try:
                data = json.loads(f)
            except:
                error_msg = "Error loading " + filename
                break

        # Save
        resp = couch.process_and_post_to_dpla(data, ingestion_doc_id)
        if resp == -1:
            error_msg = "Error saving data from " + filename

    if error_msg:
        status = "error"
    else:
        status = "complete"
    kwargs = {
        "save_process/status": status,
        "save_process/error": error_msg,
        "save_process/end_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
