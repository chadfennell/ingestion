import os
import sys
import xmltodict
from akara import logger
from urllib import urlencode
from amara.thirdparty import json
from amara.thirdparty import httplib2
from amara.lib.iri import is_absolute
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict

from dplaingestion.selector import exists
from dplaingestion.selector import setprop
from dplaingestion.selector import getprop as get_prop

def getprop(obj, path):
    return get_prop(obj, path, keyErrorAsNone=True)

def iterify(iterable):
    '''
    Treat iterating over a single item or an interator seamlessly.
    '''
    if (isinstance(iterable, basestring) \
        or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable

ARC_PARSE = lambda doc: xmltodict.parse(doc, xml_attribs=True, attr_prefix='',
                                        force_cdata=False,
                                        ignore_whitespace_cdata=True)

class Fetcher(object):
    """The base class for all fetchers.
       Includes attributes and methods that are common to all types.
    """
    def __init__(self, profile, uri_base):
        """Set common attributes"""
        self.uri_base = uri_base
        self.provider = profile.get("name")
        self.blacklist = profile.get("blacklist")
        self.contributor = profile.get("contributor")
        self.subresources = profile.get("subresources")
        self.endpoint_url = profile.get("endpoint_url")
        self.collection_titles = profile.get("collection_titles")
        self.http_handle = httplib2.Http('/tmp/.pollcache')
        self.http_handle.force_exception_as_status_code = True

    def remove_blacklisted_subresources(self):
        if self.blacklist:
            for set in self.blacklist:
                if set in self.subresources:
                    del self.subresources[set]

    def request_content_from(self, url, params={}, attempts=5):
        error, content = None, None
        if params:
            if "?" in url:
                url += "&" + urlencode(params)
            else:
                url += "?" + urlencode(params)

        for i in range(attempts):
            resp, content = self.http_handle.request(url)
            # Break if 2xx response status
            if resp["status"].startswith("2"):
                break

        # Handle non 2xx response status
        if not resp["status"].startswith("2"):
            error = "Error ('%s') resolving URL %s" % (resp["status"], url)
        elif not len(content) > 2:
            error = "No sets received from URL %s" %  url

        return error, content

class OAIVerbsFetcher(Fetcher):
    def __init__(self, profile, uri_base):
        super(OAIVerbsFetcher, self).__init__(profile, uri_base)

    def list_sets(self):
        """Requests all sets via the ListSets verb

           Returns an (error, content) tuple where error is None if the
           request succeeds, the requested content is not empty and is
           parseable, and content is a dictionary with setSpec as keys and
           a dictionary with keys title and description as the values.
        """
        subresources = {}
        list_sets_url = self.uri_base + "/oai.listsets.json?endpoint=" + \
                        self.endpoint_url

        error, content = self.request_content_from(list_sets_url)
        if error is None:
            try:
                list_sets_content = json.loads(content)
            except ValueError:
                error = "Error decoding content from URL %s" % list_sets_url
                return error, subresources

            for s in list_sets_content:
                set = s["setSpec"]
                subresources[set] = {"id": set}
                subresources[set]["title"] = s["setName"]
                if "setDescription" in set:
                    subresources[set]["description"] = s["setDescription"]

            if not subresources:
                error = "No sets received from URL %s" % list_sets_url

        return error, subresources

    def list_records(self, url, params):
        records = None
        list_records_url = self.uri_base + "/dpla-list-records?endpoint=" + url

        error, content = self.request_content_from(list_records_url, params)
        if error is None:
            try:
                records_content = json.loads(content)
            except ValueError:
                error = "Error decoding content from URL %s with params %s" % \
                        (list_records_url, params)
                return

            if not records_content.get("items"):
                error = "No records received from URL %s with params %s" % \
                        (list_records_url, params)

        return error, records_content

    def fetch_all_data(self):
        """A generator to yeild batches of records along with the collection,
           if any, the provider, and any errors encountered along the way. The
           reponse dictionary has the following structure:

            response = {
                "error": <Any error encountered>,
                "data": {
                    "provider": <The provider>,
                    "records": <The batch of records fetched>,
                    "collection": {
                        "title": <The collection title, if any>,
                        "description": <The collection description, if any>
                    }
                }
            }
        """
        response = {
            "error": None,
            "data": {
                "provider": self.provider,
                "records": None,
                "collection": None
            }
        }

        if self.subresources == "NotSupported":
            self.subresources = {"": None}
        else:
            # Fetch all sets
            response["error"], sets = self.list_sets()
            if response["error"] is not None:
                self.subresources = {}
                yield response

            # Set the subresources
            if sets:
                if not self.subresources:
                    self.subresources = sets
                    self.remove_blacklisted_subresources()
                else:
                    for set in sets.keys():
                        if set not in self.subresources:
                            del sets[set]
                    self.subresources = sets

        # Fetch all records for each subresource
        for subresource in self.subresources.keys():
            print "Fetching records for subresource " + subresource

            # Set response["data"]["collection"]
            if not subresource == "":
                setprop(response, "data/collection",
                        self.subresources[subresource])
        
            request_more = True
            resumption_token = ""
            url = self.endpoint_url
            params = {"oaiset": subresource}

            # Flag to remove subresource if no records fetched
            remove_subresource = True

            # Request records until a resumption token is not received
            while request_more:
                if resumption_token:
                    params["resumption_token"] = resumption_token
                
                # Send request
                response["error"], content = self.list_records(url, params)

                if response["error"] is not None:
                    # Stop requesting from this subresource
                    request_more = False
                else:
                    # Get resumption token
                    remove_subresource = False
                    setprop(response, "data/records", content["items"])
                    resumption_token = content.get("resumption_token")
                    request_more = (resumption_token is not None and
                                    len(resumption_token) > 0)

                yield response

            if remove_subresource:
                del self.subresources[subresource]

class AbsoluteURLFetcher(Fetcher):
    def __init__(self, profile, uri_base):
        self.get_sets_url = profile.get("get_sets_url")
        self.get_records_url = profile.get("get_records_url")
        self.endpoint_url_params = profile.get("endpoint_url_params")
        super(AbsoluteURLFetcher, self).__init__(profile, uri_base)

    # NYPL-specific methods
    def nypl_request_subresources(self):
        url = self.get_sets_url
        error, content = self.request_content_from(url)
        if error is not None:
            return error, content

        error, content = self.extract_ARC_content(content, url)
        if error is not None:
            return error, content

        subresources = {}
        for item in content["response"]:
            if item == "collection":
                for coll in content["response"][item]:
                    if "uuid" in coll:
                        subresources[coll["uuid"]] = {}
                        subresources[coll["uuid"]]["title"] = coll["title"]

        if not subresources:
            error = "Error, no subresources from URL %s" % url

        return error, subresources

    def nypl_extract_ARC_content(self, content, url):
        error = None
        try:
            parsed_content = ARC_PARSE(content)
        except:
            error = "Error parsing content from URL %s" % url
            return error, content

        content = parsed_content.get("nyplAPI")
        if content is None:
            error = "Error, there is no \"nyplAPI\" field in content from " \
                    "URL %s" % url
        elif exists(content, "response/headers/code") and \
             getprop(content, "response/headers/code") != "200":
            error = "Error, response code is not 200 for request to URL %s" % \
                    url
        return error, content

    def nypl_request_records(self, content):
        error = None
        total_pages = getprop(content, "request/totalPages")
        current_page = getprop(content, "request/page")
        request_more = total_pages == current_page

        records = []
        for item in getprop(content, "response/capture"):
            record_url = self.get_records_url.format(item["uuid"])
            error, content = self.request_content_from(record_url)
            if error is None:
                error, content = self.extract_ARC_content(content, record_url)

            if error is None:
                record = getprop(content, "response/mods")
                record["_id"] = item["uuid"]
                record["tmp_image_id"] = item.get("itemLink")
                record["tmp_high_res_link"] = item.get("highResLink")
                records.append(record)

            if error is not None:
                return error, content, request_more

        return error, content, request_more

    # UVA-specific methods
    def uva_request_subresources(self):
        """Creates the subresources dictionary from self.collection_titles
        """
        subresources = self.collection_titles
        return None, subresources


    def uva_extract_records(self, content, url):
        error = None
        records = []

        # Handle "mods:<key>" in UVA book collection
        key_prefix = ""
        if "mods:mods" in content:
            key_prefix = "mods:"

        if key_prefix + "mods" in content:
            item = content[key_prefix + "mods"]
            for _id_dict in iterify(item[key_prefix + "identifier"]):
                if _id_dict["type"] == "uri":
                    item["_id"] = _id_dict["#text"]
                    records.append(item)

        if not records:
            error = "Error, no records found in content from URL %s" % url

        yield error, records

    def uva_request_records(self, content):
        error = None

        for item in content["mets:mets"]:
            if "mets:dmdSec" in item:
                records = content["mets:mets"][item]
                for rec in records:
                    if not rec["ID"].startswith("collection-description-mods"):
                        url = rec["mets:mdRef"]["xlink:href"]
                        error, cont = self.request_content_from(url)
                        if error is not None:
                            yield error, cont
                        else:
                            error, cont = self.extract_ARC_content(cont, url)
                            if error is not None:
                                yield error, cont
                            else:
                                for error, recs in \
                                    self.uva_extract_records(cont, url):
                                    yield error, recs

    # MWDL-specific methods
    def mwdl_extract_records(self, content):
        total_records = getprop(content,
                                "SEGMENTS/JAGROOT/RESULT/DOCSET/TOTALHITS")
        records = getprop(content, "SEGMENTS/JAGROOT/RESULT/DOCSET/DOC")

        records = iterify(records)
        for record in records:
            record["_id"] = getprop(record, "PrimoNMBib/record/control/recordid")

        return total_records, records

    # Non provider-specific methods
    def extract_ARC_content(self, content, url):
        if self.provider == "nypl":
            return self.nypl_extract_ARC_content(content, url)
        else:
            error = None
            try:
                content = ARC_PARSE(content)
            except:
                error = "Error parsing content from URL %s" % url

            return error, content

    def request_subresources(self):
        if self.provider == "nypl":
            error, subresources = self.nypl_request_subresources()
        elif self.provider.startswith("virginia"):
            error, subresources = self.uva_request_subresources()
        else:
            error = "Error, provider does not support subresources"
            subresources = []

        return error, subresources

    def fetch_all_data(self):
        """A generator to yeild batches of records along with the collection,
           if any, the provider, and any errors encountered along the way. The
           reponse dictionary has the following structure:

            response = {
                "error": <Any error encountered>,
                "data": {
                    "provider": <The provider>,
                    "records": <The batch of records fetched>,
                    "collection": {
                        "title": <The collection title, if any>,
                        "description": <The collection description, if any>
                    }
                }
            }
        """
        response = {
            "error": None,
            "data": {
                "provider": self.provider,
                "records": None,
                "collection": None
            }
        }

        if self.subresources == "NotSupported":
            self.subresources = {"": None}
        elif not self.subresources:
            response["error"], self.subresources = self.request_subresources()
            if response["error"] is not None:
                self.subresources = {}
                yield response
            else:
                self.remove_blacklisted_subresources()

        for subresource in self.subresources.keys():
            print "Fetching records for subresource " + subresource

            # Set response["data"]["collection"]
            if not subresource == "":
                setprop(response, "data/collection",
                        self.subresources[subresource])

            request_more = True
            if subresource:
                url = self.endpoint_url.format(subresource)
            else:
                url = self.endpoint_url
            params = self.endpoint_url_params

            count = 0 # To be used by MWDL
            while request_more:
                response["error"], content = self.request_content_from(url,
                                                                       params)

                if response["error"] is not None:
                    # Stop requesting from this subresource
                    request_more = False
                    yield response
                    continue

                response["error"], content = self.extract_ARC_content(content,
                                                                      url)
                if response["error"] is not None:
                    request_more = False
                    yield response
                    continue
                else:
                    # NYPL records
                    if self.provider == "nypl":
                        params["page"] += 1
                        (response["error"], response["data"]["records"],
                         request_more) = self.nypl_request_records(content)

                        if response["error"] is not None:
                            yield response
                            continue
                        else:
                            yield response
                    # UVA records
                    elif self.provider.startswith("virginia"):
                        # UVA will not use the request_more flag
                        request_more = False

                        for response["error"], response["data"]["records"] in \
                            self.uva_request_records(content):
                            yield response
                    # MWDL records
                    elif self.provider == "mwdl":
                        total_records, response["data"]["records"] = \
                            self.mwdl_extract_records(content)
                        params["indx"] += len(response["data"]["records"])
                        count += params["indx"]
                        if count >= total_records:
                            request_more = False
                        yield response

class FileFetcher(Fetcher):
    def __init__(self, profile, uri_base):
        super(FileFetcher, self).__init__(profile, uri_base)

def create_fetcher(profile_path, uri_base):
    fetcher_types = {
        'file': lambda p, u: FileFetcher(p, u),
        'oai_verbs': lambda p, u: OAIVerbsFetcher(p, u),
        'absolute_url': lambda p, u: AbsoluteURLFetcher(p, u),
    }

    with open(profile_path, "r") as f:
        profile = json.load(f)
    type = profile.get("type")
    fetcher = fetcher_types.get(type)(profile, uri_base)

    return fetcher
