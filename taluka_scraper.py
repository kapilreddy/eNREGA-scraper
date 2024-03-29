import BeautifulSoup
import urllib2
from urlparse import urlparse


def talukaExtract(url):
    data = {}
    urlparts = urlparse(url)
    host = urlparts.hostname
    try:
        page = urllib2.urlopen(url)
        dir(BeautifulSoup)
        soup = BeautifulSoup.BeautifulSoup(page)
        table_block = soup('table', id = "Table2")[0]
    except:
        return None
    # there are five unwanted rows
    unwanted_row = table_block.next.nextSibling
    row_count = 1

    # traversing the table to remove unwanted rows
    while row_count < 5:
        unwanted_row = unwanted_row.nextSibling.nextSibling
        row_count += 1

    # first row of the required data for districts
    data_row = unwanted_row.nextSibling.nextSibling
    urls = []

    while data_row.td.nextSibling.nextSibling.next.string:
        print "Taluka %s " % (data_row.td.nextSibling.nextSibling.next.string)
        # assigning the value of the data_row to the data_col
        data_col = data_row
        # Pointing to the first column
        data_col = data_col.td.nextSibling.nextSibling
        # extracting the url, Code, Name via the href tag
        temp_url = data_col.next.get('href', None)

        if not temp_url:
            data_row = data_row.nextSibling
            continue
        # url value extraction
        # the url value is extracted as '../../citizen_html'
        # hence a small manipulation
        # appending the ip-address and the string block '
        url = "http://%s/netnrega%s" % (host, temp_url[5:])

        # district code index and value. district code is 4 characters
        index = temp_url.find("district_code=")
        index=index+11
        code = temp_url[index : index + 7]

        # district name is scrapped from the screen value
        name = data_col.next.string

        # Scrapping total no.of works, labor expenditure, material Expenditure
        # these are stored in 32nd column hence a manipulation
        col_count = 1
        while col_count < 32:
            data_col = data_col.nextSibling.nextSibling
            col_count += 1

        # scrapping no. of Works noWorks col: 32
        noWorks = data_col.next.string

        # scrapping labor expenditure col :33
        data_col = data_col.nextSibling.nextSibling
        labExpn = data_col.next.string

        # scrapping material Expenditure col:34
        data_col = data_col.nextSibling.nextSibling
        matExpn = data_col.next.string
        data[name] = {
            "works_no": noWorks,
            "labour_exp": labExpn,
            "matExpn": matExpn,
            "url": url}
        urls.append(url)
        data_row = data_row.nextSibling
    return data

if __name__ == "__main__":
    talukaExtract("http://164.100.112.66/netnrega/writereaddata/citizen_out/phy_fin_reptemp_Out_1809_local_1112.html")
