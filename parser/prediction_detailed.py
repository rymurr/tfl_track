from StringIO import StringIO
from dateutil import parser
import pandas
import datetime
import glob
import json   
import os
import xmltodict
import tarfile

   
def parseToDataFrame(root):
    columns = ('time', 'lineCode', 'lineName', 'stationCode', 'stationName', 'message', 'currentTime', 'platformCode', 'platformName', 'leadCarId', 'setId', 'tripId', 'secToStation', 'timeToStation', 'location', 'destCode', 'destination', 'departTime', 'departInterval', 'departed', 'direction', 'trackCode')
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
            location = train['@Location']
            destCode = int(train['@DestCode'])
            destination = train['@Destination']
            departTime = train['@DepartTime']
            departInterval = int(train['@DepartInterval'])
            departed = int(train['@Departed'])
            direction = train['@Direction']
            trackCode = train['@TrackCode']
            allTrains.append((time, lineCode, lineName, scode, sname, smess, curTime, pcode, pname, lcid, setId, tid, secToStation, timeToStation, location, destCode, destination, departTime, departInterval, departed, direction, trackCode))
    return pandas.DataFrame(allTrains, columns = columns)


def extractAll():
    dfs = []
    for filename in glob.glob('../tfl_xml-20131221*.tar.bz2'):
        print filename
        with tarfile.open(filename) as tar:
            xmls = [i for i in tar.getnames()]
            for xml in xmls:
                try:
                    data = tar.extractfile(xml).read()
                    doc = xmltodict.parse(data[6:])
                    df = parseToDataFrame(doc['ROOT'])
                    dfs.append(df)
                except:
                    pass
    return pandas.concat(dfs)                    
                    
                


