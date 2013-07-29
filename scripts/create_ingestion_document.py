#!/usr/bin/env python
import argparse
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("uri_base",
                        help="The base URI for the server hosting the " +
                             "enrichment pipeline")
    parser.add_argument("profile_path",
                        help="The path to the profile(s) to be processed",
                        nargs="+")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    with open(args.profile_path, "r") as f:
        try:
            profile = json.load(f)
        except:
            # TODO: Handle JSON load exception
            pass
    provider = profile["name"]

    couch = Couch()
    latest_ingestion_doc = couch._get_last_ingestion_doc_for(provider)
    if latest_ingestion_doc is None:
        ingestion_sequence = 1
    elif getprop(latest_ingestion_doc, "delete_process/status") == "complete":
        ingestion_sequence = 1 + \
                             getprop(latest_ingestion_doc, "ingestionSequence")
    else:
        # TODO: Handle case where previous ingestion did not complete
        return

    ingestion_document_id = couch.create_ingestion_document(provider,
                                                            ingestion_sequence,
                                                            uri_base)

    return ingestion_document_id

if __name__ == '__main__':
    main(sys.argv)
