#!/usr/bin/python

import yajl as json
import gevent
from gevent import local, monkey
from district_scraper import districtExtract
from taluka_scraper import talukaExtract
from panchayat_scraper import panchayatExtract
import time

monkey.patch_all()

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


def fetch_panchayat(url, district, taluka):
    encoded_url = _cleanUrl(url)
    data = panchayatExtract(encoded_url)
    if "panchayat" in global_data[district]["taluka"][taluka].keys():
        global_data[district]["taluka"][taluka]["panchayat"] =\
            dict(data.items() +
                 global_data[district]["taluka"][taluka]["panchayat"].items())
    else:
        global_data[district]["taluka"][taluka]["panchayat"] = data


def fetch_taluka(url, district):
    encoded_url = _cleanUrl(url)
    data = talukaExtract(encoded_url)
    if "taluka" in global_data[district].keys():
        global_data[district]["taluka"] =\
        dict(data.items() + global_data[district]["taluka"].items())
    else:
        global_data[district]["taluka"] = data
    jobs = [gevent.spawn(fetch_panchayat, value['url'], district, key)
            for key, value in data.iteritems()]
    gevent.joinall(jobs)


global_data = local.local()
data = districtExtract("http://164.100.112.66/netnrega/writereaddata/citizen"\
                            "_out/phy_fin_reptemp_Out_18_local_1112.html")
global_data = data
jobs = [gevent.spawn(fetch_taluka,
                     value["url"],
                     key)
        for key, value in data.iteritems()]
gevent.joinall(jobs)

f = open('database/data.json', 'w')
output = json.dumps(global_data)
print time.time() - start
f.write(output)
