#!/usr/bin/env python
"""
Script to remove previous ingestion document after a reingestion

Usage:
    $ python remove_deleted_documents.py ingestion_document_id
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
    if getprop(ingestion_doc, "save_process/status") != "complete":
        print "Error, save process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "delete_process/status": "running",
        "delete_process/start_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    resp = couch._process_deleted_docs(ingestion_doc.id)
    if resp == -1:
        status = "error"
        error_msg = "Error deleting documents"
    else:
        status = "complete"
        error_msg = None

    # Update ingestion document
    kwargs = {
        "delete_process/status": status,
        "delete_process/error": error_msg,
        "delete_process/end_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)