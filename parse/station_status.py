import pandas
import datetime

from dateutil import parser
from parse import base

class StationStatusParse(base.Base):   
    def __init__(self, directory, hdf):
        super(StationStatusParse, self).__init__(directory, hdf, 'dfStation')

  
    def parseToDataFrame(self, doc):
        root = doc['ArrayOfStationStatus']
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
            active = 0 if status['@IsActive'] == 'true' else 1
            stype = status['StatusType']
            sid4 = int(stype['@ID'])
            desc2 = stype['@Description']
            allTrains.append((sid, stationDetails, sid2, name, sid3, css, desc, active, sid4, desc2))
        return pandas.DataFrame(allTrains, columns = columns)

