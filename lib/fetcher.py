import os
import sys
import xmltodict
from urllib import urlencode
from amara.thirdparty import json
from amara.thirdparty import httplib2
from amara.lib.iri import is_absolute

class Fetcher(object):
    """The base class for all fetchers.
       Includes attributes and methods that are common to all types.
    """
    def __init__(self, profile):
        """Set common attributes"""
        self.uri_base = profile.get("uri_base")
        self.blacklist = profile.get("blacklist")
        self.contributor = profile.get("contributor")
        self.subresources = profile.get("subresources")
        self.endpoint_url = profile.get("endpoint_url")
        self.collection_titles = profile.get("collection_titles")
        self.http_handle = httplib2.Http('/tmp/.pollcache')
        self.http_handle.force_exception_as_status_code = True

    def request_content_from(self, url, attempts=5):
        response = {"error": None, "content": None}
        for i in range(attempts)
            resp, content = self.http_handle.request(url)
            # Break if 2xx response status
            if resp["status"].startswith("2"):
                break

        # Handle non 2xx response status
        if not resp["status"].startswith("2"):
            response["error"] = "Error ('%s') resolving URL: %s" % \
                                (resp["status"], list_sets_url)
        elif not len(content) > 2:
            response["error"] = "No sets received from URL: %s" % list_sets_url
        else:
            response["content"] = content

        return response
               
class ListVerbsFetcher(Fetcher):
    def __init__(self, profile):
        super(ListVerbsFetcher, self).__init__(profile)

    def request_list_sets(self):
        list_sets_url = self.uri_base + "%s/oai.listsets.json?endpoint=%s" + \
                        self.endpoint_url

        response = self.request_content_from(list_sets_url)
        if response["error"] is None:
            subresources = []
            try:
                set_content = json.loads(content)
            except ValueError:
                response["error"] = "Error decoding content from URL: %s" % \
                                    list_sets_url
                return response

            for s in set_content:
                subresources.append(s[0])

            if subresources:
                response["content"] = subresources
            else:
                response["error"] = "No sets received from URL %s" + \
                                    list_sets_url

        return response

    def request_list_records(self, url):
        list_records_url = self.uri_base + "/dpla-list-records?endpoint=" + url

        response = self.request_content_from(list_records_url)
        if response["error"] is None:
            try:
                records_content = json.loads(content)
            except ValueError:
                response["error"] = "Error decoding content from URL: %s" % \
                                    list_records_url
                return response

            if not records_content.get("items"):
                response["error"] = "No records received from URL: %s" % \
                                    list_records_url
            else:
                response["content"] = content

        return response

    def remove_blacklisted_subresources(self):
        if self.blacklist:
            subresources = set(self.subresources) - set(self.blacklist)
            self.subresources = list(subresources)

    def fetch_all_records(self):
        # Fetch subresources, if need be
        if not self.subresources:
            response = self.request_list_sets()
            if response.get("error") is not None:
                yield response
            else:
                self.subresources = response.get("content")
                self.remove_blacklisted_subresources()

        # Fetch all records for each subresource
        for subresource in self.subresources:
            request_more = True
            resumption_token = ""
            endpoint_url = self.endpoint_url + "&" + urlencode({"oaiset":
                                                                subresource})

            # Request records until a resumption token is not received
            while request_more:
                if resumption_token:
                    endpoint_url += "&" + urlencode({"resumption_token":
                                                     resumption_token})
                
                # Send request
                response = self.request_list_records(endpoint_url)
                if response.get("error") is not None:
                    request_more = False
                else:
                    request_more = (resumption_token is not None and
                                    len(resumption_token) > 0)
                    response["content"] = content

                yield response

class AbsoluteURLFetcher(Fetcher):
    def __init__(self, profile):
        super(AbsoluteURLFetcher, self).__init__(profile)

class FileFetcher(Fetcher):
    def __init__(self, profile):
        super(FileFetcher, self).__init__(profile)

class IA(Fetcher):
    def __init__(self, profile):
        """Set IA specific attributes"""
        self.prefix_dc = profile.get("prefix_dc")
        self.prefix_meta = profile.get("prefix_meta")
        self.shown_at_url = profile.get("shown_at_url")
        self.get_file_url = profile.get("get_file_url")
        self.prefix_files = profile.get("prefix_files")
        self.removed_enrichments_rec = profile.get("removed_enrichments_rec")
        super(IA, self).__init__(profile)

class ARC(Fetcher):
    def __init__(self, profile):
        """Set ARC specific attributes"""
        super(ARC, self).__init__(profile)

class OAI(Fetcher):
    def __init__(self, profile):
        """Set OAI specific attributes"""
        super(OAI, self).__init__(profile)

class METS(Fetcher):
    def __init__(self, profile):
        """Set METS specific attributes"""
        super(METS, self).__init__(profile)

class EDAN(Fetcher):
    def __init__(self, profile):
        """Set EDAN specific attributes"""
        super(EDAN, self).__init__(profile)

class NYPL(Fetcher):
    def __init__(self, profile):
        """Set NYPL specific attributes"""
        self.get_record_url = profile.get("get_record_url")
        super(NYPL, self).__init__(profile)

class MARC(Fetcher):
    def __init__(self, profile):
        """Set MARC specific attributes"""
        super(MARC, self).__init__(profile)

class PRIMO(Fetcher):
    def __init__(self, profile):
        """Set PRIMO specific attributes"""
        self.bulk_size = profile.get("bulk_size")
        super(PRIMO, self).__init__(profile)

def get_fetcher(profile_path):
    fetcher_types = {
        'ia': lambda p: IA(p),
        'arc': lambda p: ARC(p),
        'oai': lambda p: OAI(p),
        'mets': lambda p: METS(p),
        'edan': lambda p: EDAN(p),
        'nypl': lambda p: NYPL(p),
        'marc': lambda p: MARC(p),
        'primo': lambda p: PRIMO(p),
    }

    with open(profile_path, "r") as f:
        profile = json.load(f)
    type = profile.get("type")
    fetcher = fetcher_types.get(type)(profile)

    return fetcher
