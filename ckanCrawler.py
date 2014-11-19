'''
Created on 12/11/2014

@author: Marcelo
'''

import unicodedata
import requests
import time
import urllib2
import urllib
import socket
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

def removeAccents(input_str):
    nkfd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])
    
def cleanText(text):
    text = text.replace('\n', '')
    text = text.replace('\t', '')
    text = text.replace('\r', '')
    return text
    
    
def getCKANVersion(portalUrl):
    fp = urllib2.urlopen(portalUrl+'/api/util/status');
    
    print portalUrl+'/api/util/status'
    result = json.loads(fp.read());
    fp.close();
    version = result['ckan_version'];
    print version;
    return int(version[0])


def getDatasetList(portalUrl, ckanVersion):
    print ckanVersion
    actionURL = ''
    if (ckanVersion == 1):
        actionURL = portalUrl+'/api/rest/package'
    elif (ckanVersion == 2) :
        actionURL = portalUrl+'/api/action/package_list'
    else :
        actionURL = portalUrl+'/api/action/package_list'
    
    print actionURL
    fp = urllib2.urlopen(actionURL);
    results = json.loads(fp.read());
    fp.close();
    
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
    
    tries = 0;
    results = None;
    while True:
        try:
            connection = urllib2.urlopen(actionURL, timeout=60)
            xmlstring = connection.read()
            results = json.loads(xmlstring);
            break
        except :
            tries += 1
            if tries >= 10:
                exit()
    
    if (ckanVersion == 1):
        return results
    else :
        return results['result'];


def getFile2DownloadSize(url):
    u = urllib2.urlopen(url, timeout=60)
    meta = u.info()
    file_size = 0;

    if (len(meta.getheaders("Content-Length")) > 0) :         
        file_size = int(meta.getheaders("Content-Length")[0])
    f.close()
    return file_size


def downloadFile(url, filePath):
    tries = 0;
    timeoutValue = 60;
    while True:
        try:
            print 'Downloading try #' + str(tries) 
            existSize = 0
            
            range_handler = HTTPRangeHandler()
            opener = urllib2.build_opener(range_handler)
        
            urllib2.install_opener(opener)
            req = urllib2.Request(url)
            
            file_size = getFile2DownloadSize(url)
            print "Downloading: %s Bytes: %s" % (filePath, file_size)
            
            if os.path.exists(filePath):
                existSize = os.path.getsize(filePath)
                if ((file_size == existSize)  and  (file_size != 0 )):
                    print 'File already downloaded'
                    return
                try :
                    req.header['Range'] = "bytes=%s-" % (existSize)
                    outputFile = open(filePath,"ab")
                except:
                    outputFile = open(filePath,"wb")
            else:
                outputFile = open(filePath,"wb")
                
            socket.setdefaulttimeout(timeoutValue)
            fileDownloadChannel = urllib2.urlopen(req, timeout=timeoutValue)

            data = fileDownloadChannel.read()
            outputFile.write(data)
            fileDownloadChannel.close()
            outputFile.close()
            
            break
       
        except :
            print timeoutValue
            tries += 1
            timeoutValue += 20
            if tries >= 25:
                exit()
   


def getDatasets(portalUrl, datasetDataFile):
    
    ckanVersion = getCKANVersion(portalUrl);
    results = getDatasetList(portalUrl, ckanVersion)
    datasetID = 0
    for datasetName in results:
        datasetID += 1;
        
        datasetMetaData = getDatasetMetadata(portalUrl, datasetName, ckanVersion)
        resources = datasetMetaData['resources'];
        
        datasetFolder = portalDatasetsFolders +"/" + str(datasetID).zfill(3);
        if not os.path.exists(datasetFolder):
            os.mkdir(datasetFolder)
    
            
        for resource in resources:
            text = "";
            text += tab(portalId);
            text += tab(portalName);
            text += tab(portalUrl);
            text += tab(datasetID);
            text += tab(datasetMetaData['name']);
            text += tab(datasetMetaData['title']);
            text += tab(resource['name']);
            text += tab(resource['id']);
            text += tab(resource['url']);
            text += tab(cleanText(resource['description']))
            text += tab(resource['format']);
            text += tab(resource['last_modified']);
            text += tab(resource['created']);
            text += tab(datasetMetaData['maintainer']);
            text += tab(datasetMetaData['maintainer_email']);
            text += tab(datasetMetaData['author']);
            text += tab(datasetMetaData['author_email']);
            text += tab(datasetMetaData['state']);
            text += tab(datasetMetaData['isopen']);
            text += tab(datasetMetaData['metadata_created']);
            text += tab(datasetMetaData['metadata_modified']);
            text += tab(datasetMetaData['groups']);
            text += tab(datasetMetaData['tags']);
            text += tab(datasetMetaData['license_title']);
            text += tab(resource['size']);
            
            datasetUrl = resource['url'];
            fileName =  datasetFolder +"/" + unicode(resource['name'].replace('/','-'))+  '.'+resource['format']
            fileName = removeAccents(fileName) 

            
            #Como nao vamos baixar o dataset, escrevi comentei a funcao de download, e coloquei a chamada da funcao que baixa o tamanho do dataset
            #downloadFile(datasetUrl, fileName)
            #size = os.path.getsize( fileName )
            
            size = getFile2DownloadSize(url)
            text += tab(size);
            
            print text;
            datasetDataFile.write(text + "\n")
            
            



with open('portais.dat', 'r') as f:
    reader = csv.reader(f, dialect='excel', delimiter='\t')
    
    i = 0
    for row in reader:
        
        portalId = row[0];
        portalName = row[1];
        portalUrl = normalizeURL(row[2]);
        
        portalDatasetsFolders = "datasets/" +portalId.zfill(2);

        with codecs.open('datasetData.dat', 'a+', encoding='utf-8') as arq: 
        
            if not os.path.exists(portalDatasetsFolders):
                os.makedirs(portalDatasetsFolders);
            
            print portalUrl+'/api/rest/package';
    
            getDatasets(portalUrl, arq);
         
        


    
    
    
    
    
