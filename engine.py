import datetime
import gevent

def getMidnight(date):
    return date.replace(hour=0, minute=0, second=0, microsecond=0)

def getNextMidnight(date):
    x = getMidnight(date)
    return x + datetime.timedelta(1)

def getSecondsDiff(now, later):
    return (later-now).total_seconds()


def main(fetchCallbacks, parseCallbacks, cleanupCallbacks, midnightCallbacks = None, period = 30):
    now = datetime.datetime.today()
    later = getNextMidnight(now)
    repeat = gevent.spawn(repeatTask, fetchCallbacks, parseCallbacks, cleanupCallbacks, period)
    midnight = gevent.spawn_later(getSecondsDiff(now, later), midnightTask, midnightCallbacks)
    gevent.joinall([repeat, midnight])

def repeatTask(fetchCallbacks, parseCallbacks, cleanupCallbacks, period):
    print 'foo'
    gevent.spawn_later(period, repeatTask, fetchCallbacks, parseCallbacks, cleanupCallbacks, period)

def midnightTask(midnightCallbacks):
    pass



