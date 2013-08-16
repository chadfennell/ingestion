import itertools
import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from nose.plugins.attrib import attr
from dict_differ import assert_same_jsons

@attr(travis_exclude='yes')    
def test_geocode():
    """
    Simple geocode
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "Boston, MA"
                }
            ]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Boston, MA",
                    "state": "Massachusetts",
                    "country": "United States",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), EXPECTED)


@attr(travis_exclude='yes')    
def test_close_multiple_results():
    """
    Geocode that returns multiple results from Bing, that are close enough to each other.
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "Philadelphia, PA"
                },
                { 
                    "name": "San Francisco, CA"
                },
                { 
                    "name": "New York, NY"
                },
                { 
                    "name": "Georgia"
                }
            ]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Philadelphia, PA",
                    "state": "Pennsylvania",
                    "country": "United States",
                    "coordinates": "39.9522781372, -75.1624526978"
                }, 
                { 
                    "county": "San Francisco County", 
                    "country": "United States", 
                    "state": "California", 
                    "name": "San Francisco, CA", 
                    "coordinates": "37.7771186829, -122.419639587"
                }, 
                {
                    "county": "New York County", 
                    "country": "United States", 
                    "state": "New York", "name": 
                    "New York, NY", 
                    "coordinates": "40.7145500183, -74.0071182251"
                },
                { 
                    "name": "Georgia"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), EXPECTED)


@attr(travis_exclude='yes')    
def test_geocode_coordinate_provided():
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "state": "Massachusetts",
                    "country": "United States",
                    "name": "42.358631134, -71.0567016602",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), EXPECTED)
