from StringIO import StringIO
from dateutil import parser
import glob
import json   
import os
import xmltodict

   
attribMap = {'C':'timeToStation','D':'destinationCode','S':'setNumber','DE':'destination','L':'location','T':'idx'}
BASE_DIR = os.path.expanduser('~/tfl_xml')

def getXMLParser(filename):
    with open(filename) as f:
        x = f.read()
    tree = xmltodict.parse(x)
    return tree['ROOT']

def parseToDataFrame(root):
    columns = ('time', 'stationCode', 'stationName', 'platformCode', 'platformName', 'tripId', 'setId', 'destCode', 'timeToStation', 'location', 'destination')
    allTrains = []
    time = parser.parse(root['Time']['@TimeStamp'])
    stations = root['S']
    for station in stations:
        scode = station['@Code']
        sname = station['@N']
        platforms = station['P']
        for platform in platforms:
            pcode = int(platform['@Code'])
            pname = platform['@N']
            trains = platform.get('T', list())
            if isinstance(trains, dict):
                trains = [trains]
            for train in trains:
                tid = int(train['@T'])
                setId = int(train['@S'])
                destCode = int(train['@D'])
                timeToStation = train['@C'].split(':')
                timeToStation = datetime.timedelta() if len(timeToStation) == 1 else datetime.timedelta(minutes=int(timeToStation[0]), seconds=int(timeToStation[1]))
                location = train['@L']
                destination = train['@DE']
                allTrains.append((time, scode, sname, pcode, pname, tid, setId, destCode, timeToStation, location, destination))
    return pandas.DataFrame(allTrains, columns = columns)

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
        
