import pandas
import datetime
import numpy as np

from dateutil import parser
from parse import base
   
class PredictionSummaryParse(base.Base):   
    def __init__(self, directory, hdf):
        super(PredictionSummaryParse, self).__init__(directory, hdf, 'dfSummary')

    def parseToDataFrame(self, doc):
        root = doc['ROOT']
        columns = ('time', 'stationCode', 'stationName', 'platformCode', 'platformName', 'tripId', 'setId', 'destCode', 'timeToStation', 'location', 'destination')
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
                    allTrains.append((time, scode, sname, pcode, pname, tid, setId, destCode, timeToStation, location, destination))
        if len(allTrains) == 0:
            return pandas.DataFrame(columns=columns)
        df = pandas.DataFrame(allTrains, columns = columns)
        df['timeToStation'] = df.timeToStation.map(lambda x:x.astype(np.float)/1E9)
        return df
