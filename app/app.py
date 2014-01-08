from engine import mainLoop

from scraper.line_status import LineStatus
from scraper.station_status import StationStatus
from scraper.prediction_detailed import PredictionDetailed 
from scraper.prediction_summary import PredictionSummary

from parse.line_status import LineStatusParse 
from parse.station_status import StationStatusParse
from parse.prediction_detailed import PredictionDetailedParse
from parse.prediction_summary import PredictionSummaryParse

from cleanup import createTar, uploadToS3

def getHDFStore():
    #create filename
    #open object with compression
    #write metadata table (datetime created, and???)
    #close and return filename
    return 'store.h5'

def midnightRollHDF():
    #set indicies
    #close hdf5 and write any more metadata?
    #copy to S3
    #delete
    #call for new store
    pass

def main():
    hdf = getHDFStore()
    fetchCallbacks = [StationStatus(), LineStatus(), PredictionSummary(), PredictionDetailed()]
    parseCallbacks = [StationStatusParse(fetchCallbacks[0].base, hdf),
                      LineStatusParse(fetchCallbacks[1].base, hdf),
                      PredictionSummaryParse(fetchCallbacks[2].base, hdf),
                      PredictionDetailedParse(fetchCallbacks[3].base, hdf)
                     ]
    cleanupCallbacks = [createTar, uploadToS3]
    midnightCallbacks = [midnightRollHDF]
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks)

if __name__ == '__main__':
    main()
