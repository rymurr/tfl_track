import datetime

from dateutil import parser
from parse import base
from collections import namedtuple
   
PredictionSummaryRow = namedtuple('PredictionSummaryRow', ('time', 'stationCode', 'stationName', 'platformCode', 'platformName', 'tripId', 'setId', 'destCode', 'timeToStation', 'location', 'destination'))
class PredictionSummaryParse(base.Base):   
    def __init__(self, hdf):
        super(PredictionSummaryParse, self).__init__(hdf, 'dfSummary', 'summary')

    def parseToDataFrame(self, doc):
        root = doc['ROOT']
        allTrains = []
        time = parser.parse(root['Time']['@TimeStamp'])
        stations = root['S']
        for station in stations:
            scode = station['@Code']
            sname = station['@N']
            platforms = station['P']
            for platform in platforms:
                if isinstance(platform, unicode):
                    continue
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
                    allTrains.append(PredictionSummaryRow(time, scode, sname, pcode, pname, tid, setId, destCode, timeToStation, location, destination))
        return allTrains
