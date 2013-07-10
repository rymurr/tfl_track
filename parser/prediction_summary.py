import xml.etree.ElementTree as ET
from StringIO import StringIO
from dateutil import parser
import glob
import json   
import os
   
attribMap = {'C':'timeToStation','D':'destinationCode','S':'setNumber','DE':'destination','L':'location','T':'idx'}
BASE_DIR = os.path.expanduser('~/tfl_xml')

def getXMLParser(filename):
    with open(filename) as f:
        x = f.read()
    tree = ET.parse(StringIO(x.decode('utf-8')[3:]))
    root = tree.getroot()
    return root

def parseToJson(root):
    time = parser.parse(root[0].attrib['TimeStamp'])
    items = []
    for station in root[1:]:
        for platform in station:
            for train in platform:
                item = {}
                item['timeStamp'] = time
                item['station'] = station.attrib
                item['platform'] = platform.attrib
                for k,v in train.attrib.items():
                    item[attribMap[k]] = v
                items.append(item)    
    return items,time            

def main():
    allitems = []
    alltimes = []
    badFilenames = []
    for filename in glob.glob(os.path.join(BASE_DIR,'*.xml')):
        try:
            print 'Parsing filename {0}'.format(filename)
            root = getXMLParser(filename)
            items,time = parseToJson(root)
            alltimes.append(time)
            allitems.extend(items)
            print 'Finished filename {0} with {1} items'.format(filename, len(items))
        except UnicodeEncodeError as e:
            badFilenames.append(filename)
            print 'Unable to run filename {0} because of UnicodeEncodeError {1} at {2}:{3}'.format(filename, e.reason, e.start, e.end)
    maxtime = max(alltimes).strftime('%Y%m%d')    
    mintime = min(alltimes).strftime('%Y%m%d')    
    filename = trains-maxtime-mintime.json    
    with open(os.path.join(BASE_DIR,filename)) as f:
        json.dump(items, f)    
    print badFilenames    
        
if __name__ == '__main__':
    main()
        
