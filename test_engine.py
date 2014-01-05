import datetime
import engine

def test_getMidnight():
    date = datetime.datetime(2014,1,1)
    dt = datetime.datetime(2014,1,1,13,0,2,123)
    assert engine.getMidnight(dt) == date

def test_getNextMidnight():
    date = datetime.datetime(2014,1,2)
    dt = datetime.datetime(2014,1,1,13,0,2,123)
    assert engine.getNextMidnight(dt) == date

def test_getGetSecondsDiff():
    date = datetime.datetime(2014,1,1)
    dt = datetime.datetime(2014,1,1,13,0,2,123)
    assert engine.getSecondsDiff(date, dt) == 13*60*60+0*60+2+123.*1E-6

