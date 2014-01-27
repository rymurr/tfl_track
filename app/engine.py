import datetime
import gevent

from logbook import Logger
from gevent import monkey;monkey.patch_all()

#TODO:
#more testing
#logging and instrumentation

log = Logger(__name__)
def getMidnight(date):
    return date.replace(hour=0, minute=0, second=0, microsecond=0)

def getNextMidnight(date):
    x = getMidnight(date)
    return x + datetime.timedelta(1)

def getSecondsDiff(now, later):
    return (later-now).total_seconds()


def mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks , period = 30):
    now = datetime.datetime.today()
    log.info('Started main look at {0}'.format(now.strftime('%Y%m%d-%H%M%s')))
    log.info('We are doing {0} fetch callbacks, {1} parse callbacks, {2} cleanup callbacks and {3} midnight callbacks'.format(len(fetchCallbacks), len(parseCallbacks), len(cleanupCallbacks), len(midnightCallbacks)))
    log.info('Tasks will be run every {0} seconds'.format(period))
    later = getNextMidnight(now)
    repeat = repeatTask(fetchCallbacks, parseCallbacks, cleanupCallbacks, period)
    midnight = midnightTask(midnightCallbacks)
    gevent.joinall([repeat, midnight])

def repeatTask(fetchCallbacks, parseCallbacks, cleanupCallbacks, period):
    log.info('Starting repeated task')
    try:
        log.info('Starting fetch callbacks')
        doAll(fetchCallbacks)
        log.info('Starting parse callbacks') 
        doAll(parseCallbacks)
        log.info('Starting cleanup callbacks')
        doAll(cleanupCallbacks)
    except:
        log.exception('Failed callback set!')
    finally:
        log.info('Finished all callbacks, scheduled to start again in {0} secods'.format(period))
        repeat = gevent.spawn_later(period, repeatTask, fetchCallbacks, parseCallbacks, cleanupCallbacks, period)
    return repeat    

def doAll(callbacks):
    log.debug('Spawing jobs')
    jobs = [gevent.spawn(callback) for callback in callbacks]
    log.debug('Joining jobs')
    gevent.joinall(jobs)
    log.debug('All jobs have returned')

def midnightTask(midnightCallbacks):
    log.info('Running midnight task!')
    try:
        doAll(midnightCallbacks)
        log.info('Midnight task completed successfully')
    except:
        log.exception('Midnight task failed!')
    finally:
        now = datetime.datetime.today()
        later = getNextMidnight(now)
        log.info('Scheduling next midnight job for {0} in {1} seconds'.format(later.strftime('%Y%m%d-%H%M%s'), str(getSecondsDiff(now, later))))
        midnight = gevent.spawn_later(getSecondsDiff(now, later), midnightTask, midnightCallbacks)
    return midnight    

