from engine import mainLoop

from parse.line_status import LineStatusParse 
from parse.station_status import StationStatusParse
from parse.prediction_detailed import PredictionDetailedParse
from parse.prediction_summary import PredictionSummaryParse

from cleanup import createTar, uploadToS3, getHDFStore, midnightRollHDF

def main():
    hdf = getHDFStore()
    fetchCallbacks = []
    parseCallbacks = [StationStatusParse( hdf),
                      LineStatusParse( hdf),
                      PredictionSummaryParse( hdf),
                      PredictionDetailedParse( hdf)
                     ]
    cleanupCallbacks = [createTar, uploadToS3]
    midnightCallbacks = [midnightRollHDF]
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks, period=150)

if __name__ == '__main__':
    main()
