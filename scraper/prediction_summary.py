import grequests
import requests
import glob
import datetime
import os

from data import LINES, STATIONS, LINE_STATIONS
BASE_DIR='tfl_xml'
SUFFIX='.xml'
SUMMARY_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionSummary/'
DETAIL_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionDetailed/'
def getLineXMLAsync(lines):
    rs = (grequests.get(SUMMARY_URL+line) for line in lines.keys())
    return [r.text for r in grequests.map(rs)]

def getLineStationXMLAsync(stations, line):
    rs = (grequests.get(DETAIL_URL+line + '/' + station) for station in stations)
    return [r.text for r in grequests.map(rs)]

def getLineXML(line):
    r = requests.get(SUMMARY_URL+line)
    return r.text

def getLineStationXML(station, line):
    r = requests.get(DETAIL_URL+line + '/' + station)
    return r.text

def writeXMLtoFile(filename, text):
    with open(filename,'w') as f:
        f.write(text.encode('utf-8'))
        
def getFilename(line, pre = None):        
    date = datetime.datetime.today().strftime('%Y%m%d')
    linePrefix = ('-'+pre) if pre else ''
    prefix = os.path.join(os.path.expanduser(BASE_DIR),date+linePrefix+'-'+line+'-')
    count = 0
    try:
        count = max([int(x.replace(prefix,'').replace(SUFFIX,'')) for x in glob.glob(prefix+'*')]) + 1
    except:
        pass
    return prefix + str(count) + SUFFIX

def main():
    xmlTexts = getLineXMLAsync(LINES)
    filenames = map(lambda x:getFilename(x), LINES.keys())
    [writeXMLtoFile(filename, xmlText) for filename, xmlText in zip(filenames, xmlTexts)]
    print 'Done Lines!'
    for line, names in LINE_STATIONS.items():
        print 'Fetching the ' + LINES[line] + ' station'
        xmlTexts = getLineStationXMLAsync(names, line)
        filenames = map(lambda x:getFilename(x, line), names)
        [writeXMLtoFile(filename, xmlText) for filename, xmlText in zip(filenames, xmlTexts)]
    print 'Done Stations!'


if __name__ == '__main__':
    main()
