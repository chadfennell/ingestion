import os
import sys
import ConfigParser
from nose import with_setup
from nose.tools import nottest
from server_support import server
from server_support import print_error_log
from amara.thirdparty import json
from amara.thirdparty import httplib2
from dplaingestion.fetcher import get_fetcher

# TODO:
# We should create our own data feed so as not to rely on a provider feed. 

# Remove trailing forward slash
uri_base = server()[:-1]

scdl_blacklist = ["ctm", "cfb", "spg", "jfb", "jbt", "pre", "dnc", "scp",
                  "swl", "weg", "ghs", "wsb", "mbe", "gcj", "cwp", "nev",
                  "hfp", "big"]
scdl_all_subresources = ["gmb", "ctm", "cfb", "spg", "jfb", "jbt", "pre",
                         "dnc", "scp", "swl", "weg", "ghs", "wsb", "mbe",
                         "gcj", "cwp", "nev", "hfp", "big", "dae"]

def test_oai_fetcher_valid_subresource():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.subresources = ["gmb"]
    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert response.get("records") is not None

    assert fetcher.subresources.keys() == ["gmb"]

def test_oai_fetcher_invalid_subresource():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.subresources = ["banana"]
    for response in fetcher.fetch_all_data():
        assert response.get("error") is not None
        assert response.get("records") is None

    assert fetcher.subresources.keys() == []

def test_oai_fetcher_all_subresources():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert response.get("records") is not None

    diff = [subresource for subresource in scdl_all_subresources if
            subresource not in fetcher.subresources]
    assert diff == []

def test_oai_fetcher_with_blacklist():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.blacklist = scdl_blacklist
    for response in fetcher.fetch_all_data():
        pass

    subresources = list(set(scdl_all_subresources) - set(scdl_blacklist))
    diff = [subresource for subresource in subresources if
            subresource not in fetcher.subresources]
    assert diff == []

def test_absolute_url_fetcher_nypl():
    profile_path = "profiles/nypl.pjs"
    fetcher =  get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "AbsoluteURLFetcher"

    count = 0
    for response in fetcher.fetch_all_data():
        count += 1
        assert response.get("error") is None
        assert response.get("records") is not None
        if count == 5:
            break

def test_absolute_url_fetcher_uva1():
    profile_path = "profiles/virginia.pjs"
    fetcher =  get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "AbsoluteURLFetcher"

    count = 0
    for response in fetcher.fetch_all_data():
        count += 1
        assert response.get("error") is None
        assert response.get("records") is not None
        if count == 5:
            break

def test_absolute_url_fetcher_uva2():
    profile_path = "profiles/virginia_books.pjs"
    fetcher =  get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "AbsoluteURLFetcher"

    count = 0
    for response in fetcher.fetch_all_data():
        count += 1
        assert response.get("error") is None
        assert response.get("records") is not None
        if count == 5:
            break

def test_all_oai_verb_fetchers():
    for profile in os.listdir("profiles"):
        profile_path = "profiles/" + profile
        print profile_path
        with open(profile_path, "r") as f:
            prof = json.loads(f.read())
        if prof.get("type") == "oai_verbs":
            fetcher =  get_fetcher(profile_path)
            fetcher.uri_base = uri_base
            assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

            # Digital Commonwealth set 218 is giving errors
            if prof.get("name") == "digital-commonwealth":
                fetcher.blacklist.append("218")

            for response in fetcher.fetch_all_data():
                print response.get("error")
                assert response.get("error") is None
                assert response.get("records") is not None
                break

def test_all_absolute_url_fetchers():
    pass

def test_all_file_fetchers():
    pass
    
