#!/usr/bin/env python
"""
Script to fetch records from a provider.

Usage:
    $ python fetch_records.py ingestion_document_id
"""
import os
import argparse
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

class Fetcher():
    pass

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

    # Update ingestion document
    fetch_dir = create_fetch_dir()
    kwargs = {
        "fetch_process/status": "running",
        "fetch_process/data_dir": fetch_dir,
        "fetch_process/start_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    error_msg = None
    # TODO: instantiate Fetcher

    # Update ingestion document
    if error_msg:
        status = "error"
    else:
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
