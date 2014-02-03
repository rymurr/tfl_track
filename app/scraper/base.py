import os
import datetime
import grequests
import tarfile

from cStringIO import StringIO
from logbook import Logger
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from settings import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

from sqsController import sendToSqs
from data import URLS

log = Logger(__name__)
class Base(object):

    def sendToSqs(self, filenames, name):
        return sendToSqs(filenames, name)

    def __call__(self):
        filename = datetime.datetime.today().strftime('%Y%m%d-%H%M%S')+'.tar.bz2'
        log.info('Writing to tarfile: {0}'.format(filename))
        rs = (grequests.get(url) for url in URLS)
        tf = StringIO()
        with tarfile.open(mode='w:bz2',fileobj=tf) as tar:
            [addToTar(tar, r.text[3:].encode('ascii','ignore'),'{0}.xml'.format(i)) for i,r in enumerate(grequests.map(rs))]
        log.info('Done base fetch, fetched {0} files'.format(len(URLS)))
        tf.seek(0)
        return uploadToS3(tf, filename)

def uploadToS3(fobj, filename):
    log.debug('Filename is: {0}'.format(filename))
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket('0nk38gf20cct6vytbg02_tfl_data')
    try:
        key = Key(bucket)
        key.key = 'xml/'+filename
        log.debug('Sending file {0} with key {1}'.format(filename, key.key))
        key.set_contents_from_file(fobj)
        sendToSqs([filename], 'tfl_queue_parse')
    except:
        log.exception('Unable to upload file {0}'.format(filename))

def addToTar(tar, data, name):
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tar.addfile(info, StringIO(data))

if __name__ == '__main__':
    Base()()
