import datetime
import gevent

from gevent import monkey;monkey.patch_all()

#TODO:
#more testing
#logging and instrumentation

def getMidnight(date):
    return date.replace(hour=0, minute=0, second=0, microsecond=0)

def getNextMidnight(date):
    x = getMidnight(date)
    return x + datetime.timedelta(1)

def getSecondsDiff(now, later):
    return (later-now).total_seconds()


def mainLoop(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks = None, period = 30):
    now = datetime.datetime.today()
    later = getNextMidnight(now)
    repeat = repeatTask(fetchCallbacks, parseCallbacks, cleanupCallbacks, period)
    midnight = midnightTask(midnightCallbacks)
    gevent.joinall([repeat, midnight])

def repeatTask(fetchCallbacks, parseCallbacks, cleanupCallbacks, period):
    try:
        doAll(fetchCallbacks)
        doAll(parseCallbacks)
        doAll(cleanupCallbacks)
    except:
        print 'yikes!'
        pass   
    finally:
        repeat = gevent.spawn_later(period, repeatTask, fetchCallbacks, parseCallbacks, cleanupCallbacks, period)
    return repeat    

def doAll(callbacks):
    jobs = [gevent.spawn(callback) for callback in callbacks]
    gevent.joinall(jobs)

def midnightTask(midnightCallbacks):
    try:
        doAll(midnightCallbacks)
    except:
        print 'yikes!'
    finally:
        now = datetime.datetime.today()
        later = getNextMidnight(now)
        midnight = gevent.spawn_later(getSecondsDiff(now, later), midnightTask, midnightCallbacks)
    return midnight    

