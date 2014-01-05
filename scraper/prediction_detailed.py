import datetime
import os
import base

from data import LINE_STATIONS, LINES


BASE_DIR='data/xml/prediction_detailed'
DETAIL_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionDetailed/'

class PredictionDetailed(base.Base):
    def __init__(self):
        super(PredictionDetailed, self).__init__(DETAIL_URL, BASE_DIR)

    def getURL(self, **kwargs):
        line = kwargs['line']
        station = kwargs['station']
        return self.url + line + '/' + station

    def getLineXMLAsync(self, **kwargs):
        stations = kwargs['stations']
        line = kwargs['line']
        urls = [self.getURL(line=line, station=station) for station in stations]
        return self.getXMLAsync(urls)

    def __call__(self):
        for line, names in LINE_STATIONS.items():
            print 'Fetching the ' + LINES[line] + ' station'
            xmlTexts = self.getLineXMLAsync(stations=names, line=line)
            filenames = map(lambda x:self.getFilename(line=x, pre=line), names)
            [self.writeXMLtoFile(filename, xmlText) for filename, xmlText in zip(filenames, xmlTexts)]
        print 'Done Stations!'

