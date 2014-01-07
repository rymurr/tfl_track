import base

BASE_DIR='data/xml/station_status/'
SUMMARY_URL = 'http://cloud.tfl.gov.uk/TrackerNet/StationStatus'
class StationStatus(base.Base):
    def __init__(self):
        super(StationStatus, self).__init__(SUMMARY_URL, BASE_DIR)

