import os
import datetime
import grequests

SUFFIX='.xml'

class Base(object):

    def __init__(self, url, base):
        self.url = url
        self.base = base
        checkBase(base)

    def writeXMLtoFile(self, filename, text):
        with open(filename,'w') as f:
            f.write(text.encode('utf-8'))

    def getXMLAsync(self, urls):
        rs = (grequests.get(url) for url in urls)
        return [r.text for r in grequests.map(rs)]

    def getFilename(self, **kwargs):        
        line = kwargs.get('line', None)
        pre = kwargs.get('pre', None)
        date = datetime.datetime.today().strftime('%Y%m%d')
        linePrefix = ('-'+pre) if pre else ''
        pprefix = ('-'+line) if line else ''
        prefix = os.path.join(os.path.expanduser(self.base),date+linePrefix+pprefix)
        return prefix + SUFFIX

    def getURL(self, **kwargs):
        return self.url

    def getLineXMLAsync(self, **kwargs):
        urls = [self.getURL(**kwargs)]
        return self.getXMLAsync(urls)

    def __call__(self):
        xmlText = self.getLineXMLAsync()[0]
        filename = self.getFilename()
        self.writeXMLtoFile(filename, xmlText) 
 
def checkBase(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        assert os.path.isdir(path)


