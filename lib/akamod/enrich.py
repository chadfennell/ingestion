from akara.services import simple_service
from akara import request, response
from akara import module_config, logger
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
from amara.lib.iri import join, is_absolute
from urllib import quote, urlencode, quote_plus
import datetime
import uuid
import base64
import hashlib


COUCH_ID_BUILDER = lambda src, lname: "--".join((src,lname))
# Set id to value of the first identifier, disambiguated w source. Not sure if
# an OAI handle is guaranteed or on what scale it's unique.
# FIXME it's looking like an id builder needs to be part of the profile. Or UUID as fallback?
COUCH_REC_ID_BUILDER = lambda src, rec: COUCH_ID_BUILDER(src,rec.get(u'id','no-id').strip().replace(" ","__"))

H = httplib2.Http()
H.force_exception_as_status_code = True

COLLECTIONS = {}

# FIXME: should support changing media type in a pipeline
def pipe(content, ctype, enrichments, wsgi_header):
    body = json.dumps(content)
    for uri in enrichments:
        if not uri: continue # in case there's no pipeline
        if not is_absolute(uri):
            prefix = request.environ['wsgi.url_scheme'] + '://' 
            prefix += request.environ['HTTP_HOST'] if request.environ.get('HTTP_HOST') else request.environ['SERVER_NAME']
            uri = prefix + uri
        headers = copy_headers_to_dict(request.environ, exclude=[wsgi_header])
        headers['content-type'] = ctype
        logger.debug("Calling url: %s " % uri)
        resp, cont = H.request(uri, 'POST', body=body, headers=headers)
        if not str(resp.status).startswith('2'):
            logger.warn("Error in enrichment pipeline at %s: %s"%(uri,repr(resp)))
            continue

        body = cont
    return body

# FIXME: should be able to optionally skip the revision checks for initial ingest
def couch_rev_check_coll(docuri,doc):
    """Add current revision to body so we can update it"""
    resp, cont = H.request(docuri,'GET', headers=COUCH_AUTH_HEADER)
    if str(resp.status).startswith('2'):
        doc['_rev'] = json.loads(cont)['_rev']

def couch_rev_check_recs_old(docs, src):
    """
    Insert revisions for all records into structure using CouchDB bulk interface.
    Uses key ranges to narrow bulk query to the source being ingested.

    Deprecated: has performance issue
    """

    uri = join(COUCH_DATABASE,'_all_docs')
    start = quote(COUCH_ID_BUILDER(src,''))
    end = quote(COUCH_ID_BUILDER(src,'Z'*100)) # FIXME. Is this correct?
    uri += '?startkey=%s&endkey=%s'%(start,end)

    # REVU: it fetches all docs from db again and again for each doc bulk
    # by killing performance and can cause memory issues with big collections
    # so, if you need to set revisions for each 100 doc among 10000, you
    # will be getting by 10000 docs for each hundred (100 times)
    #
    # new version is implemented in couch_rev_check_recs2, see details
    resp, cont = H.request(join(COUCH_DATABASE,'_all_docs'), 'GET', headers=COUCH_AUTH_HEADER)
    if str(resp.status).startswith('2'):
        rows = json.loads(cont)["rows"]
        #revs = { r["id"]:r["value"]["rev"] for r in rows } # 2.7 specific
        revs = {}
        for r in rows:
            revs[r["id"]] = r["value"]["rev"]
        for doc in docs:
            id = doc['_id']
            if id in revs:
                doc['_rev'] = revs[id]
    else:
        logger.warn('Unable to retrieve document revisions via bulk interface: ' + repr(resp))
        logger.warn('Request old: ' + uri)

def couch_rev_check_recs(docs):
    """
    Insert revisions for all records into structure using CouchDB bulk interface.
    Uses key ranges to narrow bulk query to the source being ingested.

    Performance improved version of couch_rev_check_recs_old, but it uses another input format:
    Input:
     {doc["_id"]: doc, ...}
    """
    if not docs:
        return
    uri = join(COUCH_DATABASE, '_all_docs')
    docs_ids = sorted(docs)
    start = docs_ids[0]
    end = docs_ids[-1:][0]
#    uri += "?" + urlencode({"startkey": start, "endkey": end})
    uri += '?startkey="%s"&endkey="%s"' % (quote_plus(start), quote_plus(end))
    response, content = H.request(uri, 'GET', headers=COUCH_AUTH_HEADER)
    if str(response.status).startswith('2'):
        rows = json.loads(content)["rows"]
        for r in rows:
            if r["id"] in docs:
                docs[r["id"]]["_rev"] = r["value"]["rev"]
    else:
        logger.warn('Unable to retrieve document revisions via bulk interface: ' + repr(response))
        logger.warn('Request: ' + uri)

def set_ingested_date(doc):
    doc[u'ingestDate'] = datetime.datetime.now().isoformat()

def enrich_coll(ctype, source_name, collection_name, collection_title, coll_enrichments):
    cid = COUCH_ID_BUILDER(source_name, collection_name)
    id = hashlib.md5(cid).hexdigest()
    at_id = "http://dp.la/api/collections/" + id
    coll = {
        "id": id,
        "_id": cid,
        "@id": at_id,
        "title": collection_title,
        "ingestType": "collection"
    }
    set_ingested_date(coll)
    enriched_coll_text = pipe(coll, ctype, coll_enrichments, 'HTTP_PIPELINE_COLL')
    enriched_collection = json.loads(enriched_coll_text)

    return enriched_collection

@simple_service('POST', 'http://purl.org/la/dp/enrich', 'enrich', 'application/json')
def enrich(body, ctype):
    """
    Establishes a pipeline of services identified by an ordered list of URIs provided
    in two request headers, one for collections and one for records
    """

    request_headers = copy_headers_to_dict(request.environ)
    source_name = request_headers.get('Source')
    collection_name = request_headers.get('Collection')

    if not (collection_name or source_name):
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Source and Collection request headers are required"

    coll_enrichments = request_headers.get(u'Pipeline-Coll', '').split(',')
    rec_enrichments = request_headers.get(u'Pipeline-Rec', '').split(',')

    data = json.loads(body)

    # For non-OAI, the collection title is included as part of the data,
    # so we extract it here to pass it to def enrich_coll a few lines down.
    # For OAI, the collection enrichment pipeline with set the title and so
    # None will be overridden. 
    collection_title = data.get("title", None)

    docs = {}
    for record in data[u'items']:
        # Preserve record prior to any enrichments
        record['originalRecord'] = record.copy()         

        # Add collection(s)
        record[u'collection'] = []
        sets = record.get('setSpec', collection_name)
        for s in (sets if isinstance(sets, list) else [sets]):
            if s not in COLLECTIONS:
                COLLECTIONS[s] = enrich_coll(ctype, source_name, s,
                                             collection_title, coll_enrichments)
            rec_collection = {
                'id': COLLECTIONS[s].get('id', None),
                '@id': COLLECTIONS[s].get('@id', None),
                'title': COLLECTIONS[s].get('title', None),
                'description': COLLECTIONS[s].get('description', None)
            }
            record[u'collection'].append(dict((k, v) for k, v in
                                         rec_collection.items() if v))
                    
        if len(record[u'collection']) == 1:
            record[u'collection'] = record[u'collection'][0]

        record[u'ingestType'] = 'item'
        set_ingested_date(record)

        doc_text = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        doc = json.loads(doc_text)
        # After pipe doc must have _id and sourceResource
        if doc.get("_id", None):
            if "sourceResource" in doc:
                docs[doc["_id"]] = doc
            else:
                logger.error("Document does not have sourceResource: %s" % doc["_id"])

    # Add collections to docs
    for collection in COLLECTIONS.values():
        docs[collection["_id"]] = collection

    return json.dumps(docs)

@simple_service('POST', 'http://purl.org/la/dp/enrich_storage',
                'enrich_storage', 'application/json')
def enrich_storage(body, ctype):
    """Establishes a pipeline of services identified by an ordered list of URIs
       provided in request header 'Pipeline-Rec'
    """

    request_headers = copy_headers_to_dict(request.environ)
    rec_enrichments = request_headers.get(u'Pipeline-Rec','').split(',')

    data = json.loads(body)

    docs = {}
    for record in data:
        doc_text = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        doc = json.loads(doc_text)
        docs[doc["_id"]] = doc

    return json.dumps(docs)
