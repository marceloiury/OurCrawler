'''
Created on 12/11/2014

@author: Marcelo
'''

import byterange
import requests
import time
import urllib2
import urllib

import json
import os
import csv
import codecs
import sys



class myURLOpener(urllib.FancyURLopener):
    """Create sub-class in order to overide error 206.  This error means a
       partial file is being sent,
       which is ok in this case.  Do nothing with this error.
    """
    def http_error_206(self, url, fp, errcode, errmsg, headers, data=None):
        pass

class HTTPRangeHandler(urllib2.BaseHandler):
    def http_error_206(self, req, fp, code, msg, hdrs):
        # 206 Partial Content Response
        r = urllib.addinfourl(fp, hdrs, req.get_full_url())
        r.code = code
        r.msg = msg
        return r
    
    def http_error_416(self, req, fp, code, msg, hdrs):
        # HTTP's Range Not Satisfiable error
        raise RangeError('Requested Range Not Satisfiable')
    
def tab (valor):
    if (valor == None) : 
        return "\t";
    elif (isinstance(valor,list)) :
        return ''.join(str(e) for e in valor).join('\t');
    elif (not isinstance(valor,basestring)) :
        return str(valor) + "\t";
    else:
        return valor + "\t";

def normalizeURL(url):
    length = len(url)
    if (url[length-1] == '/') :
        return url[0:length-1];
    else:
        return url;
    
def cleanText(text):
    #text = text.encode('utf-8')
    text = text.replace('\n', '')
    text = text.replace('\t', '')
    text = text.replace('\r', '')
    return text
    
    
def getCKANVersion(portalUrl):
    fp = urllib2.urlopen(portalUrl+'/api/util/status');
    result = json.loads(fp.read());
    fp.close();
    version = result['ckan_version'];
    print version;
    return version[0]


def getDatasetList(portalUrl, ckanVersion):
    print ckanVersion
    actionURL = ''
    if (ckanVersion == 1):
        actionURL = portalUrl+'/api/rest/package'
    elif (ckanVersion == 2) :
        actionURL = portalUrl+'/api/2/action/package_list'
    else :
        actionURL = portalUrl+'/api/action/package_list'
    
    print actionURL
    fp = urllib2.urlopen(actionURL);
    print 'OPENED ' + actionURL
    results = json.loads(fp.read());
    print 'LOADED ' + actionURL
    fp.close();
    print 'OK ' + actionURL
    
    if (ckanVersion == 1):
        return results
    else :
        return results['result'];

def getDatasetMetadata(portalUrl, datasetName, ckanVersion):
    
    actionURL = ''
    if (ckanVersion == 1):
        actionURL = portalUrl+'/api/rest/package/'+datasetName
    elif (ckanVersion == 2) :
        actionURL = portalUrl+'/api/action/package_show?id='+datasetName
    else :
        actionURL = portalUrl+'/api/3/action/package_show?id='+datasetName
    

    # Try three times to make contact
    tries = 0;
    results = None;
    while True:
        try:
            # Connect to API 
            connection = urllib2.urlopen(actionURL, timeout=60)
            print 'open ' + actionURL
            xmlstring = connection.read()
            print 'read ' + actionURL
            results = json.loads(xmlstring);
            
            break
        except :
            tries += 1
            if tries >= 10:
                exit()
    
    
    #print actionURL
    #fp = urllib2.urlopen(actionURL);
    #print 'opened ' + actionURL
    #t = fp.read()
    #print 'read ' + actionURL
    #results = json.loads(t);
    print 'loaded ' + actionURL
    #fp.close();
    
    print 'OK ' + actionURL
    if (ckanVersion == 1):
        return results
    else :
        return results['result'];


def getFile2DownloadSize(url):
    u = urllib2.urlopen(url, timeout=10)
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    f.close()
    return file_size



def downloadFile(url, filePath):
    tries = 0;
    timeoutValue = 60;
    while True:
        #try:
            print 'Downloading try #' + str(tries) 
            loop = 1
            existSize = 0
            file_name = url.split('/')[-1]
            
            
            range_handler = range.HTTPRangeHandler()
            opener = urllib2.build_opener(range_handler)
        
            # install it
            urllib2.install_opener(opener)
        
            # create Request and set Range header
            req = urllib2.Request(url)
            
            
            if os.path.exists(filePath):
                outputFile = open(filePath,"ab")
                existSize = os.path.getsize(filePath)
                #myUrlclass.addheader("Range","bytes=%s-" % (existSize))
                req.header['Range'] = "bytes=%s-" % (existSize)
            else:
                outputFile = open(filePath,"wb")
    
            #fileDownloadChannel = myUrlclass.open(url, timeout=timeoutValue)
            
            fileDownloadChannel = urllib2.urlopen(req, timeout=timeoutValue)

            

            file_size = getFile2DownloadSize(url)
            print "Downloading: %s Bytes: %s" % (filePath, file_size)

            if int(file_size) == existSize:
                loop = 0
                print "File already downloaded"
            
            numBytes = 0
            while loop:
                print 1
                data = fileDownloadChannel.read(8192*10)
                if not data:
                    break
                print 2
                outputFile.write(data)
                print 3
                numBytes = numBytes + len(data)
                print 4
                status = r"%10d  [%3.2f%%]" % (numBytes, numBytes * 100. / file_size)
                status = status + chr(8) * (len(status) + 1)
                print status,
            
            fileDownloadChannel.close()
            outputFile.close()
            
            f.close()
            break
       
       


def getDatasets(portalUrl, datasetDataFile):
    
    ckanVersion = getCKANVersion(portalUrl);
    results = getDatasetList(portalUrl, ckanVersion)
    datasetID = 0
    for datasetName in results:
        datasetID += 1;
        #print '\n\n\n'+ datasetName;
        #print portalURL+'/api/rest/package/'+datasetName
        
        if (datasetID < 52):
            continue
        
        datasetMetaData = getDatasetMetadata(portalUrl, datasetName, ckanVersion)
        resources = datasetMetaData['resources'];
        
        datasetFolder = portalDatasetsFolders +"/" + str(datasetID).zfill(3);
        if not os.path.exists(datasetFolder):
            os.mkdir(datasetFolder)
    
            
        for resource in resources:
            
            cleanText(resource['description'])
            
            text = "";
            text += tab(portalId);
            text += tab(portalName);
            text += tab(portalUrl);
            text += tab(datasetID);
            text += tab(datasetMetaData['name']);
            text += tab(resource['name']);
            text += tab(resource['url']);
            text += tab(cleanText(resource['description']))
            text += tab(resource['format']);
            text += tab(resource['last_modified']);
            text += tab(resource['created']);
            text += tab(datasetMetaData['maintainer']);
            text += tab(datasetMetaData['maintainer_email']);
            text += tab(datasetMetaData['metadata_created']);
            text += tab(datasetMetaData['metadata_modified']);
            text += tab(datasetMetaData['groups']);
            text += tab(datasetMetaData['tags']);
            text += tab(datasetMetaData['license_title']);
            
            datasetUrl = resource['url'];
            
            print 'init download ' + datasetUrl
            fileName =  datasetFolder +"/" + resource['name']+'.'+resource['format']
            downloadFile(datasetUrl, fileName)
            
            print 'downloaded ' + datasetUrl
            
            
            size = os.path.getsize( fileName ) 
            text += tab(size);
            
            print text;
            datasetDataFile.write(text + "\n")
            
            




with open('portais.dat', 'r') as f:
    reader = csv.reader(f, dialect='excel', delimiter='\t')
    print sys.stdin.encoding
    
    for row in reader:
        portalId = row[0];
        portalName = row[1];
        portalUrl = normalizeURL(row[2]);
        
        millis = int(round(time.time() * 1000)) 
        
        portalDatasetsFolders = "datasets/" +portalId.zfill(2);

        #arq = open("datasetData.dat", "a+");        
        with codecs.open('datasetData.dat', 'a+', encoding='utf-8') as arq: 
        
            #Folder to store datasets from current portal 
            if not os.path.exists(portalDatasetsFolders):
                os.makedirs(portalDatasetsFolders);
            
            print portalUrl+'/api/rest/package';
    
            getDatasets(portalUrl, arq);
         
        #arq.close();
        


    
    
    
    
    
