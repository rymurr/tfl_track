import datetime
import gevent
import itertools
import os
import base

from data import LINE_STATIONS, LINES


BASE_DIR='data/xml/prediction_detailed/'
DETAIL_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionDetailed/'

class PredictionDetailed(base.Base):
    def __init__(self):
        super(PredictionDetailed, self).__init__(DETAIL_URL, BASE_DIR, 'detailed')

    def getURL(self, **kwargs):
        line = kwargs['line']
        station = kwargs['station']
        return self.url + line + '/' + station

    def getLineXMLAsync(self, **kwargs):
        stations = kwargs['stations']
        line = kwargs['line']
        urls = [self.getURL(line=line, station=station) for station in stations]
        return self.getXMLAsync(urls)

    def fetch(self, **kwargs):
        names = kwargs['names']
        line = kwargs['line']
        print 'Fetching the ' + LINES[line] + ' station'
        xmlTexts = self.getLineXMLAsync(stations=names, line=line)
        return line, names, xmlTexts

    def write(self, **kwargs):
        names = kwargs['names']
        line = kwargs['line']
        xmlTexts = kwargs['xmlTexts']
        filenames = map(lambda x:self.getFilename(line=x, pre=line), names)
        [self.writeXMLtoFile(filename, xmlText) for filename, xmlText in zip(filenames, xmlTexts)]
        return filenames

    def __call__(self):
        jobs = [gevent.spawn(self.fetch, line=line, names=names) for line, names in LINE_STATIONS.items()]
        gevent.joinall(jobs)
        jobs = [gevent.spawn(self.write, line=i.value[0], names=i.value[1], xmlTexts=i.value[2]) for i in jobs]
        gevent.joinall(jobs)
        filenames = list(itertools.chain(*[i.value for i in jobs]))
        print 'Done Stations!'
        return self.sendToSqs(filenames, 'tfl_qeueu_xml_'+self.name)

