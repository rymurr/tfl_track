import grequests
import requests
import glob
import tarfile
import datetime
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from data import LINES, STATIONS, LINE_STATIONS
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

#TODO: Need some major refactoring for generalizations. The filename stuff especially is pretty horrific


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
    prefix = os.path.join(os.path.expanduser(BASE_DIR),date+linePrefix+'-'+line)
    #count = 0
    #try:
    #    count = max([int(x.replace(prefix,'').replace(SUFFIX,'')) for x in glob.glob(prefix+'*')]) + 1
    #except:
    #    pass
    return prefix + SUFFIX

def createTar():
    prefix = os.path.expanduser(BASE_DIR)
    tarfilename = prefix + datetime.datetime.today().strftime('-%Y%m%d-%H%M%S')+'.tar.bz2'
    with tarfile.open(tarfilename, 'w:bz2') as tar:
        for filename in glob.glob(os.path.join(prefix,'*.xml')):
            tar.add(filename)
    for filename in os.listdir(prefix):
        fpath = os.path.join(prefix, filename)
        try:
            if os.path.isfile(fpath):
                os.unlink(fpath)
        except:
            pass
    return tarfilename
 

def uploadToS3(filename):
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(AWS_ACCESS_KEY_ID.lower() + '_tfl_data') 
    key = Key(bucket)
    key.key = filename
    key.set_contents_from_filename(filename)
    #TODO: I dont think this is working...FIX IT!
    try:
        if os.path.isfile(filename):
            os.unlinke(filenmae)
    except:
        pass

    
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
    fname = createTar()
    print 'Done Tarfile'
    uploadToS3(fname)
    print 'Done S3 upload'


if __name__ == '__main__':
    main()
