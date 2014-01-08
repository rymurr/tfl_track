import base

BASE_DIR='data/xml/line_status/'
SUMMARY_URL = 'http://cloud.tfl.gov.uk/TrackerNet/LineStatus'

class LineStatus(base.Base):
    def __init__(self):
        super(LineStatus, self).__init__(SUMMARY_URL, BASE_DIR)

