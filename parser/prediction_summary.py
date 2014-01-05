import pandas
import glob
import xmltodict
import datetime

from dateutil import parser
from pandas.io.pytables import HDFStore

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

def extractAll(directory):
    dfs = []
    for filename in glob.glob(directory + '*.xml'):
        with open(filename) as f:
            data = f.read()
            doc = xmltodict.parse(data[6:])
            df = parseToDataFrame(doc['ROOT'])
            dfs.append(df)
    return pandas.concat(dfs)                    

def toHDF(hdfstore, df):
    with HDFStore(hdfstore) as store:
        store.append('dfSummary', df)
                
def parsePredictionSummary(hdf, directory):
    df = extractAll(directory)
    toHDF(hdf, df)
