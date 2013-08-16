"""Support module to start a local Akara test server"""

import atexit
import os
from os.path import abspath, dirname
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import urllib2
import urlparse
import httplib
import ConfigParser

# XXX I only use one function from here. Bring it into this file?
import python_support

##############################################################################

class MyHttp:
    "Class providing request function for calling HTTP service."

    HEADERS = {
        "Content-Type": "application/json",
        "Context": "{}",
        "Connection": "close"
    }

    def request(self, url, method, body, headers=HEADERS):
        from amara.thirdparty import httplib2
        h = httplib2.Http()
        return h.request(url, method, body, headers)

# Use this object in tests.

H = MyHttp()

##############################################################################

# Set 'False' to keep the temporary directory used for the server tests
#DELETE_TEMPORARY_SERVER_DIRECTORY = False
DELETE_TEMPORARY_SERVER_DIRECTORY = True
if "AKARA_SAVE" in os.environ:
    DELETE_TEMPORARY_SERVER_DIRECTORY = False


######

# All of the tests use a single server instance.
# This is started the first time it's needed.
# It is stopped during test shutdown.

def _override_server_uri():
    server = os.environ.get("AKARA_TEST_SERVER", None)
    if not server:
        return None
    assert "/" not in server
    return "http://" + server + "/"
    
SERVER_URI = _override_server_uri()
config_root = None
config_filename = None
server_pid = None
server_did_not_start = False
thumbs_root = None

# Create a temporary directory structure for Akara.
# Needs a configuration .ini file and the logs subdirectory.
def create_server_dir(port):
    global config_root, config_filename, thumbs_root
    
    config_root = tempfile.mkdtemp(prefix="akara_test_")
    config_filename = os.path.join(config_root, "akara_test.config")
    thumbs_root=os.path.join(config_root, "thumbs_root")

    ini = ConfigParser.ConfigParser()
    ini.optionxform=str  # Maintain case for configuration keys 
    ini.read(os.path.dirname(os.path.realpath(__file__)) + "/../akara.ini")
    bing_apikey = "notset"
    if (ini.has_section("Bing") \
        and ini.has_option("Bing", "ApiKey")): 
        bing_apikey = ini.get("Bing", "ApiKey")
    geonames_username = "notset"
    geonames_token = "notset"
    if (ini.has_section("Geonames")):
        if ini.has_option("Geonames", "Username"): 
            geonames_username = ini.get("Geonames", "Username")
        if ini.has_option("Geonames", "Token"): 
            geonames_token = ini.get("Geonames", "Token")

    print thumbs_root

    f = open(config_filename, "w")
    f.write("""
class Akara:
  ConfigRoot = %(config_root)r
  InternalServerRoot = 'http://localhost:%(port)s/'
  Listen = 'localhost:%(port)s'
  LogLevel = 'DEBUG'
  MinSpareServers = 3
  # These affect test_server.py:test_restart
  MaxServers = 5
  MaxRequestsPerServer = 5

MODULES = [
    "freemix_akara.contentdm",
    "freemix_akara.oai",
    "dplaingestion.oai",
    "dplaingestion.couch",
    "dplaingestion.akamod.enrich",
    "dplaingestion.akamod.enrich-subject",
    "dplaingestion.akamod.enrich-type",
    "dplaingestion.akamod.enrich-format",
    "dplaingestion.akamod.enrich-date",
    "dplaingestion.akamod.select-id",
    "dplaingestion.akamod.shred",
    "dplaingestion.akamod.geocode",
    "dplaingestion.akamod.oai-to-dpla",
    "dplaingestion.akamod.oai-set-name",
    "dplaingestion.akamod.dpla-list-records",
    "dplaingestion.akamod.contentdm_identify_object",
    "dplaingestion.akamod.download_preview",
    "dplaingestion.akamod.download_test_image",
    "dplaingestion.akamod.filter_empty_values",
    "dplaingestion.akamod.artstor_select_isshownat",
    "dplaingestion.akamod.artstor_identify_object",
    "dplaingestion.akamod.digital_commonwealth_enrich_location",
    "dplaingestion.akamod.uiuc_enrich_location",
    "dplaingestion.akamod.move_date_values",
    "dplaingestion.akamod.enrich_location",
    "dplaingestion.akamod.geocode",
    "dplaingestion.akamod.lookup",
    "dplaingestion.akamod.georgia_identify_object",
    "dplaingestion.akamod.bhl_contributor_to_collection",
    "dplaingestion.akamod.copy_prop",
    "dplaingestion.akamod.cleanup_value",
    "dplaingestion.akamod.set_prop",
    "dplaingestion.akamod.enrich_language",
    "dplaingestion.akamod.dpla-get-record",
    "dplaingestion.akamod.mods_to_dpla",
    "dplaingestion.akamod.artstor_cleanup",
    "dplaingestion.akamod.nypl_identify_object",
    "dplaingestion.akamod.nypl_coll_title",
    "dplaingestion.akamod.nypl_select_hasview",
    "dplaingestion.akamod.primo-to-dpla",
    "dplaingestion.akamod.mwdl_cleanup_field",
    "dplaingestion.akamod.ia_to_dpla",
    "dplaingestion.akamod.ia_identify_object",
    "dplaingestion.akamod.dc_clean_invalid_dates",
    "dplaingestion.akamod.edan_to_dpla",
    "dplaingestion.akamod.cleanup_language",
    "dplaingestion.akamod.dc_clean_invalid_dates",
    "dplaingestion.akamod.decode_html",
    "dplaingestion.akamod.artstor_spatial_to_dataprovider",
    "dplaingestion.akamod.oai_mods_to_dpla",
    "dplaingestion.akamod.arc-to-dpla",
    "dplaingestion.akamod.dedup_value",
    "dplaingestion.akamod.set_type_from_physical_format",
    "dplaingestion.akamod.capitalize_value",
    "dplaingestion.akamod.artstor_cleanup_creator",
    "dplaingestion.akamod.replace_substring",
    "dplaingestion.akamod.uiuc_cleanup_spatial_name",
    "dplaingestion.akamod.remove_list_values",
    "dplaingestion.akamod.usc_enrich_location",
    ]

class download_preview:
    thumbs_root_path = '%(thumbs_root)s'
    thumbs_root_url  = 'http://aaa.bbb.com/'
    # Only these MIME types are supported
    # The value is the extension which will be used for storing the
    # file on disk.
    mime_to_type = {
        'image/jpeg' : '.jpg',
        'image/gif'  : '.gif',
        'image/png'  : '.png',
    }

class geocode: 
    bing_api_key = '%(bing_apikey)s'
    geonames_username = '%(geonames_username)s'
    geonames_token = '%(geonames_token)s'

class lookup:
    lookup_mapping = {
        'test': 'test_subst',
        'test2': 'test_2_subst',
        'dc_data_provider': 'DC_DATA_PROVIDER',
        'iso639_3': 'iso639_3_subst',
        'scdl_fix_format': 'SCDL_FIX_FORMAT'
    }

class identify_object:
    IGNORE = 0
    PENDING = 1

class contentdm_identify_object(identify_object):
    pass

class kentucky_identify_object(identify_object):
    pass

class artstor_identify_object(identify_object):
    pass

class georgia_identify_object(identify_object):
    pass

class nypl_identify_object(identify_object):
    pass

class ia_identify_object(identify_object):
    pass
""" % dict(config_root = config_root,
           port = port,
           thumbs_root = thumbs_root,
           bing_apikey = bing_apikey,
           geonames_username = geonames_username,
           geonames_token = geonames_token
           ))
    f.close()

    os.mkdir(os.path.join(config_root, "logs"))
    os.mkdir(thumbs_root)

    print thumbs_root

#FIXME: add back config for:
#[collection]
#folder=/tmp/collection

# Remove the temporary server configuration directory,
# if I created it
def remove_server_dir():
    global server_pid, config_root
    if server_pid is not None:
        # I created the server, I kill it
        os.kill(server_pid, signal.SIGTERM)
        server_pid = None

    if config_root is not None:
        # Very useful when doing development and testing.
        # Would like this as a command-line option somehow.
        if DELETE_TEMPORARY_SERVER_DIRECTORY:
            shutil.rmtree(config_root)
        else:
            print "Test server configuration and log files are in", config_root
        config_root = None

atexit.register(remove_server_dir)

# Start a new Akara server in server mode.
def start_server():
    global server_pid, server_did_not_start
    # There's a PID if the spawning worked
    assert server_pid is None
    # Can only tell if the server starts by doing a request
    # If the request failed, don't try to restart.
    if server_did_not_start:
        raise AssertionError("Already tried once to start the server")

    port = python_support.find_unused_port()
    create_server_dir(port)
    args = ['akara', "--config-file", config_filename, "start"]
    try:
        result = subprocess.call(args)
    except:
        print "Failed to start", args
        raise

    # Akara started, but it might have failed during startup.
    # Report errors by reading the error log
    if result != 0:
        f = open(os.path.join(config_root, "logs", "error.log"))
        err_text = f.read()
        raise AssertionError("Could not start %r:\n%s" % (args, err_text))

    # Akara server started in the background. The main
    # process will only exit with a success (0) if the
    # pid file has been created.
    f = open(os.path.join(config_root, "logs", "akara.pid"))
    line = f.readline()
    f.close()

    # Save the pid information now so the server will be shut down
    # if there are any problems.
    temp_server_pid = int(line)

    # Check to see that the server process really exists.
    # (Is this overkill? Is this portable for Windows?)
    os.kill(temp_server_pid, 0)  # Did Akara really start?

    server_did_not_start = True
    check_that_server_is_available(port)
    server_did_not_start = False

    # Looks like it's legit!
    server_pid = temp_server_pid
    return port

# It takes the server a little while to get started.
# In the worst case (trac #6), top-level import failures
# will loop forever, and the server won't hear requests.
def check_that_server_is_available(port):
    old_timeout = socket.getdefaulttimeout()
    try:
        # How long do you want to wait?
        socket.setdefaulttimeout(20.0)
        try:
            urllib2.urlopen("http://localhost:%d/" % port).read()
        except urllib2.URLError, err:
            print "Current error log is:"
            f = open(os.path.join(config_root, "logs", "error.log"))
            err_text = f.read()
            print err_text
            raise
    finally:
        socket.setdefaulttimeout(old_timeout)

# Get the server URI prefix, like "http://localhost:8880/"
def server():
    global SERVER_URI
    if SERVER_URI is None:
        # No server specified and need to start my own
        port = start_server()
        SERVER_URI = "http://localhost:%d/" % port

    return SERVER_URI

def httplib_server():
    url = server()
    # <scheme>://<netloc>/<path>?<query>#<fragment>
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    assert path in ("", "/"), "Cannot handle path in %r" % url
    assert query == "", "Cannot handle query in %r" % url
    assert fragment == "", "Cannot handle fragment in %r" % url
    assert "@" not in netloc, "Cannot handle '@' in %r" % url
    if ":" in netloc:
        host, port = netloc.split(":")
        port = int(port)
    else:
        host = netloc
        port = 80
    conn = httplib.HTTPConnection(host, port, strict=True)
    return conn

def print_error_log():
    """
    Prints the Akara's error log. Is useful when running `nodetests --verbose`.
    """
    print "ERROR LOG\n"
    print os.path.join(config_root, 'logs', 'error.log')
    with open(os.path.join(config_root, 'logs', 'error.log'), "r") as errfile:
        for line in errfile:
            sys.stdout.write(line)
    print "END OF ERROR LOG\n"

def get_thumbs_root():
    """
    Function returns thumbs_root variable.

    This is just a small workaround, as I need ot have the value of thumbs_root in
    one of the tests. However simple importing the variable in the test file gets
    None value all the time, as it is copied at the moment of importing.

    This function solves the problem.
    """
    return thumbs_root
