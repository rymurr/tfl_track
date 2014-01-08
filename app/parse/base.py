import glob
import datetime

import xmltodict
import pandas

from pandas.io.pytables import HDFStore

class Base(object):
    
    def __init__(self, directory, hdf, name):
        self.hdf = hdf
        self.directory = directory
        self.name = name

    def parseToDataFrame(self, doc):
        return pandas.DataFrame()

    def extractAll(self, directory):
        dfs = []
        for filename in glob.glob(directory + '*.xml'):
            with open(filename) as f:
                data = f.read()
                try:
                    doc = xmltodict.parse(data[6:])
                    df = self.parseToDataFrame(doc)
                    dfs.append(df)
                except:
                    continue
        dfd = pandas.concat(dfs)
        dfd['dateTime'] = datetime.datetime.now()
        return dfd

    def toHDF(self, hdfstore, df, name):
        for i in df.dtypes[df.dtypes==object].index:
            df[i] = df[i].map(lambda x:convert(x))
        dfx = df.dropna(how='all', axis=1)    
        try:
            store = HDFStore(hdfstore)
            store.append(name, dfx)
            store.close()
        except:
            import pdb;pdb.set_trace()
                
    def __call__(self):
        df = self.extractAll(self.directory)
        self.toHDF(self.hdf, df, self.name)


def convert(x):
    if isinstance(x,unicode):
        return x.encode('ascii', 'ignore')
    return x
