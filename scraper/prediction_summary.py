import requests
import glob
import datetime
import os

BASE_DIR='~/tfl_xml'
SUFFIX='.xml'

def getLineXML(line):
    r = requests.get('http://cloud.tfl.gov.uk/TrackerNet/PredictionSummary/'+line)
    return r.text

def writeXMLtoFile(filename, text):
    with open(filename,'w') as f:
        f.write(text.encode('utf-8'))
        
def getFilename(line):        
    date = datetime.datetime.today().strftime('%Y%m%d')
    prefix = os.path.join(os.path.expanduser(BASE_DIR),date+'-'+line+'-')
    count = 0
    try:
        count = max([int(x.replace(prefix,'').replace(SUFFIX,'')) for x in glob.glob(prefix+'*')]) + 1
    except:
        pass
    return prefix + str(count) + SUFFIX

def main():
    lines = {
            'B': 'Bakerloo',
            'C': 'Central',
            'D': 'District',
            'H': 'Hammersmith & Circle',
            'J': 'Jubilee',
            'M': 'Metropolitan',
            'N': 'Northern',
            'P': 'Picadilly',
            'V': 'Victoria',
            'W': 'Waterloo & City'
            }
    for line, name in lines.items():
        print 'Fetching the ' + name + ' line'
        xmlText = getLineXML(line)
        filename = getFilename(line)
        writeXMLtoFile(filename, xmlText)

if __name__ == '__main__':
    main()
