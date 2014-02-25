import settings
import os
import boto
import tarfile
import xmltodict
import msgpack
from cStringIO import StringIO
from collections import defaultdict

def getKeyMap():
    s3 = boto.connect_s3(settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY)
    bucket = s3.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    bucket_list = bucket.list()
    keys = defaultdict(list)
    for key in bucket_list:
        day = key.name.split('-')[0].replace('xml/',''))
        keys[day].append(key)
    return keys    
    
def parseToDataFrame(doc):
    root = doc['ROOT']
    allTrains = []
    time = parser.parse(root['WhenCreated'])
    lineCode = root['Line']
    lineName = root['LineName']
    station = root['S']
    scode = station['@Code']
    sname = station['@N']
    smess = station['@Mess']
    curTime = station['@CurTime']
    platforms = station['P']
    for platform in platforms:
        if isinstance(platform, unicode):
            continue
        pcode = int(platform['@Num'])
        pname = platform['@N']
        trains = platform.get('T', list())
        if isinstance(trains, dict):
            trains = [trains]
        for train in trains:
            lcid = int(train['@LCID'])
            setId = int(train['@SetNo'])
            tid = int(train['@TripNo'])
            secToStation = int(train['@SecondsTo'])
            timeToStation = train['@TimeTo'].split(':')
            timeToStation = datetime.timedelta() if len(timeToStation) == 1 else datetime.timedelta(minutes=int(timeToStation[0]), seconds=int(timeToStation[1]))
            location = train.get('@Location','')
            destCode = int(train['@DestCode'])
            destination = train['@Destination']
            departTime = train['@DepartTime']
            departInterval = int(train['@DepartInterval'])
            departed = int(train['@Departed'])
            direction = train['@Direction']
            trackCode = train['@TrackCode']
            allTrains.append({'destination':destCode, 'direction':direction, 'leadCarId':lcid, 'line':lineCode, 'location':location, 'platform':pcode, 'secToStation':secToStation, 'setId':setId, 'station':scode, 'time':time, 'tripId':tid})
    return allTrains

def decode_datetime(obj):
    if b'__datetime__' in obj:
        obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
    elif b'__timedelta__' in obj:
        obj = datetime.timedelta(0, obj['as_sec'])
    return obj

def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    elif isinstance(obj, datetime.timedelta):
        return {'__timedelta__':True, 'as_sec': obj.total_seconds()}
    raise ValueError(obj)
    
def addToTar(tar, data, name):
    info = tarfile.TarInfo(name)
    data.seek(0, os.SEEK_END)
    info.size = data.tell()
    data.seek(0)
    tar.addfile(info, data)   

def getRows(key, packer, fout):
    fin = StringIO()
    key.get_contents_to_file(fin)
    fin.seek(0)
    with tarfile.open(fileobj=fin) as tar:
        while True:
            fname = tar.next()
            if fname is None:
                break
            f = tar.extractfile(fname)
            try:
                for row in parseToDataFrame(xmltodict.parse(f.read())):
                    fout.write(packer.pack(row))
            except:
                pass    
            
def uploadToS3(fobj, filename):
    log.debug('Filename is: {0}'.format(filename))
    conn = S3Connection(secret.AWS_ACCESS_KEY_ID, secret.AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    try:
        key = Key(bucket)
        key.key = 'msgpack/'+filename
        log.debug('Sending file {0} with key {1}'.format(filename, key.key))
        key.set_contents_from_filename(fobj)
    except:
        print 'Unable to upload file {0}'.format(filename)
            
def getDayMsgPack(date, keys):    
    with tarfile.open(date+'.tar.bz2', mode='w:bz2') as tar:
        for i in range(24):
            packer = msgpack.Packer(default=encode_datetime)
            foo = StringIO()
            hour = date+'T'+str(i).zfill(2)
            tmp = [getRows(key, packer, foo) for key in keys if (hour+':') in key.name]
            print i,len(tmp)
            foo.seek(0)
            addToTar(tar, foo, hour+'.msgpack')
    fout.close()
    uploadToS3(date+'.tar.bz2', date+'.tar.bz2')
    os.unlink(date+'.tar.bz2')
    
def main():
    dateKeys = getKeyMap()
    for date, keys in dateKeys.items():
        getDayMsgPack(date, keys)

if __name__ == '__main__':
    main()

