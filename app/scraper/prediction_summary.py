import datetime
import os
import base

from data import LINES


BASE_DIR='data/xml/prediction_summary/'
SUMMARY_URL = 'http://cloud.tfl.gov.uk/TrackerNet/PredictionSummary/'

class PredictionSummary(base.Base):
    def __init__(self):
        super(PredictionSummary, self).__init__(SUMMARY_URL, BASE_DIR, 'summary')

    def getURL(self, **kwargs):
        line = kwargs['line']
        return self.url + line

    def getLineXMLAsync(self, **kwargs):
        lines = kwargs['lines']
        urls = [self.getURL(line=line) for line in lines.keys()]
        return self.getXMLAsync(urls)

    def __call__(self):
        xmlTexts = self.getLineXMLAsync(lines=LINES)
        filenames = map(lambda x:self.getFilename(line=x), LINES.keys())
        [self.writeXMLtoFile(filename, xmlText) for filename, xmlText in zip(filenames, xmlTexts)]
        print 'done summary'
        return self.sendToSqs(filenames, 'tfl_queue_xml_' + self.name)

