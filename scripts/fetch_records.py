#!/usr/bin/env python
"""
Script to fetch records from a provider.

Usage:
    $ python fetch_records.py ingestion_document_id
"""
import os
import argparse
import tempfile
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.fetcher import create_fetcher
from dplaingestion.selector import getprop

def create_fetch_dir(provider):
    return tempfile.mdktemp(provider)

def create_subresources_json(subresources):
    subresources_json = []
    for subresource in subresources:
        subresources_json.append({"title": subresource})
    return json.dumps(subresources_json)

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
    fetch_dir = create_fetch_dir(ingestion_doc["provider"])
    kwargs = {
        "fetch_process/status": "running",
        "fetch_process/data_dir": fetch_dir,
        "fetch_process/start_time": datetime.now().isoformat()
    }
    couch._update_ingestion_doc(ingestion_doc, kwargs)

    error_msg = []
    fetcher = create_fetcher(ingestion_doc["profile_path"])
    for response in fetcher.request_collections_and_records():
        if response["error"] is not None:
            error_msg.append(response["error"])
        else:
            # Write records to file
            filename = os.path.join(fetch_dir, hash(str(response["records"])))
            with open(filename, "w") as f:
                f.write(json.dumps(response["records"]))

    # Write collections to file
    if fetcher.subresources:
        filename = os.path.join(fetch_dir, hash(str(fetcher.subresources)))
        with open(filename, "w") as f:
            f.write(create_subresources_json())

    # Update ingestion document
    try os.rmdir(fetch_dir):
        # Error if fetch_dir was empty
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
