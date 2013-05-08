from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from akara import module_config
from amara.lib.iri import is_absolute
from dplaingestion.audit_logger import audit_logger

IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/contentdm_identify_object',
    'contentdm_identify_object', 'application/json')
def contentdm_identify_object(body, ctype, download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """

    try:
        data = json.loads(body)
    except Exception as e:
        audit_logger.error("Bad JSON in %s: %s" % (__name__, e.args[0]))
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    handle_field = "originalRecord/handle"
    if exists(data, handle_field):
        url = None
        handle = getprop(data, handle_field)
        for h in (handle if not isinstance(handle, basestring) else [handle]):
            if is_absolute(h):
                url = h
                break
        if not url:
            audit_logger.error("There is no URL in %s for doc [%s]" % (handle_field, data["_id"]))
            return body
    else:
        audit_logger("Field %s does not exist for doc [%s]" % (handle_field, data["_id"]))
        return body

    p = url.split("u?")

    if len(p) != 2:
        audit_logger.error("Bad URL %s in doc [%s]. It should have just one 'u?' part." % (data["_id"], url))
        return body

    (base_url, rest) = p

    if base_url == "" or rest == "":
        audit_logger.error("Bad URL: %s in doc[%s]. There is no 'u?' part." % (data["_id"], url))
        return body

    p = rest.split(",")

    if len(p) != 2:
        audit_logger.error("Bad URL %s in doc [%s]. Expected two parts at the end, used in " +
            "thumbnail URL for CISOROOT and CISOPTR." % (data["_id"], url))
        return body

    # Thumb url field.
    data["object"] = "%scgi-bin/thumbnail.exe?CISOROOT=%s&CISOPTR=%s" % \
        (base_url, p[0], p[1])

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
