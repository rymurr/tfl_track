import os
import datetime
import grequests
import tarfile

from cStringIO import StringIO
from logbook import Logger

from sqsController import sendToSqs
from data import URLS

TAR_DIR='data/tarballs/'
log = Logger(__name__)
class Base(object):

    def sendToSqs(self, filenames, name):
        return sendToSqs(filenames, name)

    def __call__(self):
        prefix = os.path.expanduser(TAR_DIR)
        checkBase(prefix)
        filename = prefix + datetime.datetime.today().strftime('%Y%m%d-%H%M%S')+'.tar.bz2'
        log.info('Writing to tarfile: {0}'.format(filename))
        rs = (grequests.get(url) for url in URLS)
        with tarfile.open(filename,'w:bz2') as tar:
            [addToTar(tar, r.text[3:],'{0}.xml'.format(i)) for i,r in enumerate(grequests.map(rs))]
        log.info('Done base fetch, fetched {0} files'.format(len(URLS)))
        return self.sendToSqs([filename], 'tfl_queue_tar')
 
def addToTar(tar, data, name):
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tar.addfile(info, StringIO(data))

def checkBase(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        assert os.path.isdir(path)


if __name__ == '__main__':
    Base()()
