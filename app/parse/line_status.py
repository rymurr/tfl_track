from collections import namedtuple
from dateutil import parser
from parse import base
   
LineStatusRow = namedtuple('LineStatus', ('lineStatusId', 'lineDetails','lineId', 'name', 'statusId','cssClass','description','isActive','statusTypeId', 'statusDesc', 'lineDisruptionId', 'disruptionCssClass','disruptionDesc', 'lineIsActive', 'lineStatusTypeId', 'lineStatusDesc','stationToId', 'stationToName', 'stationFromId', 'stationFromName'))
class LineStatusParse(base.Base):   
    def __init__(self, hdf):
        super(LineStatusParse, self).__init__(hdf, 'dfLine', 'line')

    def parseToDataFrame(self, doc):
        root = doc['ArrayOfLineStatus']
        allTrains = []
        stations = root['LineStatus']
        for station in stations:
            sid = int(station['@ID'])
            stationDetails = station['@StatusDetails']
            stationStatus = station['Line']
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
            ld = station['BranchDisruptions']
            if ld is not None:
                lineDisruptions = ld['BranchDisruption']
                if isinstance(lineDisruptions, dict):
                    lineDisruptions = [lineDisruptions]
                for lineDisruption in lineDisruptions:    
                    stationTo = lineDisruption['StationTo']
                    stationFrom = lineDisruption['StationFrom']
                    lineStatus = lineDisruption['Status']
                    sid5 = lineStatus['@ID']
                    css2 = lineStatus['@CssClass']
                    desc3 = lineStatus['@Description']
                    active2 = 0  if lineStatus['@IsActive'] == 'true' else 1
                    stype2 = lineStatus['StatusType']
                    sid6 = int(stype['@ID'])
                    desc4 = stype['@Description']
                    sid7 = stationTo['@ID']
                    sid8 = stationFrom['@ID']
                    toName = stationTo['@Name']
                    fromName = stationFrom['@Name']
                    allTrains.append(LineStatusRow(sid, stationDetails, sid2, name, sid3, css, desc, active, sid4, desc2, sid5, css2, desc3, active2, sid6, desc4, sid7, toName, sid8, fromName))
            else:
                sid5 = None
                css2 = None
                desc3 = None
                active2 = None
                sid6 = None
                desc4 = None
                sid7 = None
                toName = None
                sid8 = None
                fromName = None
                allTrains.append((sid, stationDetails, sid2, name, sid3, css, desc, active, sid4, desc2, sid5, css2, desc3, active2, sid6, desc4, sid7, toName, sid8, fromName))
        return allTrains


