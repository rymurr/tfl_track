import datetime

from dateutil import parser
from collections import namedtuple
from parse import base
   
PredictionDetailedRow = namedtuple('PredictionDetailedRow', ('time', 'lineCode', 'lineName', 'stationCode', 'stationName', 'message', 'currentTime', 'platformCode', 'platformName', 'leadCarId', 'setId', 'tripId', 'secToStation', 'timeToStation', 'location', 'destCode', 'destination', 'departTime', 'departInterval', 'departed', 'direction', 'trackCode'))
class PredictionDetailedParse(base.Base):   
    def __init__(self, hdf):
        super(PredictionDetailedParse, self).__init__(hdf, 'dfDetail', 'detailed')


    def parseToDataFrame(self, doc):
        root = doc['ROOT']
        allTrains = []
        time = parser.parse(root['WhenCreated'])
        lineCode = root['Line']
        lineName = root['LineName']
        station = root['S']
        scode = station['@Code']
        sname = station['@N']
        smess = station['@Mess']
        curTime = station['@CurTime']
        platforms = station['P']
        for platform in platforms:
            if isinstance(platform, unicode):
                continue
            pcode = int(platform['@Num'])
            pname = platform['@N']
            trains = platform.get('T', list())
            if isinstance(trains, dict):
                trains = [trains]
            for train in trains:
                lcid = int(train['@LCID'])
                setId = int(train['@SetNo'])
                tid = int(train['@TripNo'])
                secToStation = int(train['@SecondsTo'])
                timeToStation = train['@TimeTo'].split(':')
                timeToStation = datetime.timedelta() if len(timeToStation) == 1 else datetime.timedelta(minutes=int(timeToStation[0]), seconds=int(timeToStation[1]))
                location = train.get('@Location','')
                destCode = int(train['@DestCode'])
                destination = train['@Destination']
                departTime = train['@DepartTime']
                departInterval = int(train['@DepartInterval'])
                departed = int(train['@Departed'])
                direction = train['@Direction']
                trackCode = train['@TrackCode']
                allTrains.append(PredictionDetailedRow(time, lineCode, lineName, scode, sname, smess, curTime, pcode, pname, lcid, setId, tid, secToStation, timeToStation, location, destCode, destination, departTime, departInterval, departed, direction, trackCode))
        return allTrains
