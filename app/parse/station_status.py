import datetime

from dateutil import parser
from collections import namedtuple
from parse import base

StationStatusRow = namedtuple('StationStatusRow', ('stationStatusId', 'stationDetails','stationId', 'name', 'statusId','cssClass','description','isActive','statusTypeId', 'statusDesc'))
class StationStatusParse(base.Base):   
    def __init__(self, hdf):
        super(StationStatusParse, self).__init__(hdf, 'dfStation', 'station')

  
    def parseToDataFrame(self, doc):
        root = doc['ArrayOfStationStatus']
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
            allTrains.append(StationStatusRow(sid, stationDetails, sid2, name, sid3, css, desc, active, sid4, desc2))
        return allTrains

