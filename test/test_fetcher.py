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
    assert fetcher.__class__.__name__ == "OAI"

    fetcher.subresources = ["gmb"]
    for response in fetcher.request_records():
        assert response.get("error") is None
        assert response.get("content") is not None

def test_oai_fetcher_invalid_subresource():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAI"

    fetcher.subresources = ["banana"]
    for response in fetcher.request_records():
        assert response.get("error") is not None
        assert response.get("content") is None

def test_oai_fetcher_all_subresources():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAI"

    for response in fetcher.request_records():
        assert response.get("error") is None
        assert response.get("content") is not None

    assert fetcher.subresources == scdl_all_subresources

def test_oai_fetcher_with_blacklist():
    profile_path = "profiles/clemson.pjs"
    fetcher = get_fetcher(profile_path)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAI"

    fetcher.blacklist = scdl_blacklist
    for response in fetcher.request_records():
        pass

    assert list(set(fetcher.subresources)) == list(set(scdl_all_subresources) -
                                                    set(scdl_blacklist))
