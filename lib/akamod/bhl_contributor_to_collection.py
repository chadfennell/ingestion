from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.audit_logger import audit_logger

@simple_service('POST', 'http://purl.org/la/dp/contributor_to_collection',
    'bhl_contributor_to_collection', 'application/json')
def bhlcontributortocollection(body,ctype,contributor_field="sourceResource/contributor"):
    """ Copies BHL contributor field value to collection field
    """

    try:
        data = json.loads(body)
    except Exception as e:
        audit_logger.error("Bad JSON in %s: %s" % (__name__, e.args[0]))
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, contributor_field):
        contributor = getprop(data, contributor_field)
        acronym = "".join(c[0] for c in contributor.split())

        setprop(data, "sourceResource/collection/@id",
                "http://dp.la/api/collections/bhl--" + acronym)
        setprop(data, "sourceResource/collection/name", contributor)

    return json.dumps(data)
