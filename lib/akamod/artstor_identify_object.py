"""
Artstor specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config

from dplaingestion import selector


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/artstor_identify_object',
                'artstor_identify_object', 'application/json')
def artstor_identify_object(body, ctype, download="True"):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_document_key = u"originalRecord"
    original_sources_key = u"handle"
    artstor_preview_prefix = "/size1/"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_sources_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_sources_key, data[u'id'])
        return body

    preview_url = None
    http_re = re.compile("https?://.*$", re.I)
    for s in data[original_document_key][original_sources_key]:
        if artstor_preview_prefix in s:
            match = re.search(http_re, s)
            if match:
                preview_url = match.group(0)
                break

    if not preview_url:
        logger.error("Can't find url with '%s' prefix in [%s] for fetching document preview url for Artstor.", artstor_preview_prefix, data[original_document_key][original_sources_key])
        return body

    data["object"] = preview_url

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)


