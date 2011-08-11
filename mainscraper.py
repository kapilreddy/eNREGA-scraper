#!/usr/bin/python

import yajl as json
from district_scraper import districtExtract
from taluka_scraper import talukaExtract
from panchayat_scraper import panchayatExtract
import time


import urllib
from urlparse import urlparse

start = time.time()


def _cleanUrl(url):
    url_parts = urlparse(url.encode('utf-8'))
    query_params = {}
    for part in url_parts.query.split("&"):
        key, value = part.split("=")
        query_params[key] = value
    query_string = urllib.urlencode(query_params)
    encoded_url = "%s://%s%s?%s" % (url_parts.scheme,
                                    url_parts.netloc,
                                    url_parts.path,
                                    query_string)
    return encoded_url


def fetch_panchayat(url, taluka):
    encoded_url = _cleanUrl(url)
    result = panchayatExtract(encoded_url)
    return result


def fetch_taluka(url, district):
    encoded_url = _cleanUrl(url)
    result = talukaExtract(encoded_url)
    print result
    for key, value in result.iteritems():
        result[key]["panchayat"] = fetch_panchayat(value['url'], key)
    return result


result = districtExtract("http://164.100.112.66/netnrega/writereaddata/citizen"\
                            "_out/phy_fin_reptemp_Out_18_local_1112.html")

for key, value in result.iteritems():
    result[key]["taluka"] = fetch_taluka(value["url"], key)


f = open('database/data.json', 'w')
output = json.dumps(result)
print time.time() - start
f.write(output)
