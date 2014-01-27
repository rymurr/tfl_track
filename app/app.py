from logbook import Logger
from engine import mainLoop

from scraper.line_status import LineStatus
from scraper.station_status import StationStatus
from scraper.prediction_detailed import PredictionDetailed 
from scraper.prediction_summary import PredictionSummary

from parse.line_status import LineStatusParse 
from parse.station_status import StationStatusParse
from parse.prediction_detailed import PredictionDetailedParse
from parse.prediction_summary import PredictionSummaryParse

from cleanup import createTar, uploadToS3, getHDFStore, midnightRollHDF

log = Logger(__name__)
def main():
    hdf = getHDFStore()
    log.info('HDF file is {0}'.format(hdf))
    fetchCallbacks = [StationStatus(), LineStatus(), PredictionSummary(), PredictionDetailed()]
    parseCallbacks = [StationStatusParse( hdf),
                      LineStatusParse( hdf),
                      PredictionSummaryParse( hdf),
                      PredictionDetailedParse( hdf)
                     ]
    cleanupCallbacks = [createTar, uploadToS3]
    midnightCallbacks = [midnightRollHDF]
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks)

if __name__ == '__main__':
    main()
