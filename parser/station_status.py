import pandas
import datetime
import glob
import xmltodict

from dateutil import parser
from pandas.io.pytables import HDFStore
   
def parseToDataFrame(root):
    columns = ('stationStatusId', 'stationDetails','stationId', 'name', 'statusId','cssClass','description','isActive','statusTypeId', 'statusDesc')
    allTrains = []
    stations = root['StationStatus']
    for station in stations:
        sid = int(station['@ID'])
        stationDetails = station['@StatusDetails']
        stationStatus = station['Station']
        status = station['Status']
        sid2 = int(stationStatus['@ID'])
        name = stationStatus['@Name']
        sid3 = status['@ID']
        css = status['@CssClass']
        desc = status['@Description']
        active = True if status['@IsActive'] == 'true' else False
        stype = status['StatusType']
        sid4 = int(stype['@ID'])
        desc2 = stype['@Description']
        allTrains.append((sid, stationDetails, sid2, name, sid3, css, desc, active, sid4, desc2))
    return pandas.DataFrame(allTrains, columns = columns)


def extractAll(directory):
    dfs = []
    for filename in glob.glob(directory + '*.xml'):
        with open(filename) as f:
            data = f.read()
            doc = xmltodict.parse(data[6:])
            df = parseToDataFrame(doc['ArrayOfStationStatus'])
            dfs.append(df)
    dfd = pandas.concat(dfs)
    dfd['dateTime'] = datetime.datetime.now()
    return dfd

def toHDF(hdfstore, df):
    with HDFStore(hdfstore) as store:
        store.append('dfStation', df)
                    
                
def parsePredictionDetailed(hdf, directory):
    df = extractAll(directory)
    toHDF(hdf, df)

