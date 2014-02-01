from engine import mainLoop

from scraper.base import Base

def main():
    fetchCallbacks = [Base()]
    parseCallbacks = []
    cleanupCallbacks = []
    midnightCallbacks = []
    mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks)

if __name__ == '__main__':
    main()
