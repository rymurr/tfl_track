from engine import mainLoop

from scraper.line_status import LineStatus
from scraper.station_status import StationStatus
from scraper.prediction_detailed import PredictionDetailed 
from scraper.prediction_summary import PredictionSummary

def main():
    fetchCallbacks = [StationStatus(), LineStatus(), PredictionSummary(), PredictionDetailed()]
    parseCallbacks = []
    cleanupCallbacks = []
    midnightCallbacks = []
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks)

if __name__ == '__main__':
    main()
