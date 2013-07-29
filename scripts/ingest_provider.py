#!/usr/bin/env python
import sys
import argparse
import ConfigParser
from dplaingestion import create_ingestion_document
from dplaingestion import fetch_records
from dplaingestion import enrich_records
from dplaingestion import save_records
from dplaingestion import remove_deleted_records

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the profile(s) to be processed",
                        nargs="+")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    config_file = ("akara.ini")
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))
    uri_base = "http://localhost:" + config.get("Akara", "Port")

    for profile_path in args.profile_path:
        print "Creating ingestion document for profile %s" % profile_path
        ingestion_doc_id = create_ingestion_document(uri_base, profile_path)
        if ingestion_doc_id is None:
            print "Error creating ingestion document"
            continue

        print "Fetching records..."
        resp = fetch_records(ingestion_doc_id)
        if not resp == 0:
            print "Error fetching records"
            continue

        print "Enriching records..."
        resp = enrich_records(ingestion_doc_id)
        if not resp == 0:
            print "Error enriching records"
            continue

        print "Saving records..."
        resp = save_records(ingestion_doc_id)
        if not resp == 0:
            print "Error saving records"
            continue

        print "Removing deleted records..."
        resp = remove_deleted_records(ingestion_doc_id) 
        if not resp == 0:
            print "Error saving records..."
            continue

        print "Ingestion complete!"

if __name__ == '__main__':
    main(sys.argv)
