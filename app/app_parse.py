from cStringIO import StringIO
from logbook import Logger

from engine import mainLoop

from parse.line_status import LineStatusParse 
from parse.station_status import StationStatusParse
from parse.prediction_detailed import PredictionDetailedParse
from parse.prediction_summary import PredictionSummaryParse

from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from cleanup import createTar, uploadToS3, getHDFStore, midnightRollHDF

log = Logger(__name__)
def main():
    hdf = getHDFStore()
    log.info('HDF file is {0}'.format(hdf))
    fetchCallbacks = []
    parseCallbacks = [StationStatusParse( hdf),
                      LineStatusParse( hdf),
                      PredictionSummaryParse( hdf),
                      PredictionDetailedParse( hdf)
                     ]
    cleanupCallbacks = []
    midnightCallbacks = [midnightRollHDF]
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks)

def parse_all(stationStatus, lineStatus, predictionSummary, predictionDetailed):
    filenames = recieveAllFromSqs('tfl_queue_parse')
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    parseObjs = [stationStatus, lineStatus, predictionDetailed, predictionSummary]
    for filename in filenames:
        key = bucket.get_key(filename)
        if key is None:
            continue
        strIO = StringIO()
        key.get_contents_to_file(strIO)
        strIO.seek(0)
        parse_one(tarObj, parseObjs)

def parse_one(tarObj, parseObjs):
    with tarfile.open(mode="r:bz2", fileobj=strIO) as tar:
        for member in tar.getnames():
            xml = tar.extractfile(member).read()
            parse_xml(parseObjs, xml)

def parse_xml(parseObjs, xml):
    for parseObj in parseObjs:
        try:
            parseObj(xml)
        except:
            pass


if __name__ == '__main__':
    main()
