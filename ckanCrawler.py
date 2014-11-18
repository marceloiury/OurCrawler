'''
Created on 12/11/2014

@author: Marcelo
'''

import requests
import time
import urllib2

import json
import os
import csv
import codecs
import sys

  

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

def downloadFile(url, filePath):
    tries = 0;
    timeoutValue = 60;
    while True:
        try:
            print 'Downloading try #' + str(tries) 
            file_name = url.split('/')[-1]
            u = urllib2.urlopen(url, timeout=timeoutValue)
            f = open(filePath, 'wb')
            meta = u.info()
            file_size = int(meta.getheaders("Content-Length")[0])
            print "Downloading: %s Bytes: %s" % (file_name, file_size)
        
            file_size_dl = 0
            block_sz = 8192 * 100;
            while True:
                buffer = u.read(block_sz)
                if not buffer:
                    break
            
                file_size_dl += len(buffer)
                f.write(buffer)
                status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                status = status + chr(8) * (len(status) + 1)
                print status,
            
            f.close()
            break
        except :
            print 'Failed',  sys.exc_info()[0]
            tries += 1
            timeoutValue *= tries
            if tries >= 10:
                exit()


def getDatasets(portalUrl, datasetDataFile):
    
    ckanVersion = getCKANVersion(portalUrl);
    results = getDatasetList(portalUrl, ckanVersion)
    datasetID = 0
    for datasetName in results:
        datasetID += 1;
        #print '\n\n\n'+ datasetName;
        #print portalURL+'/api/rest/package/'+datasetName
        
        if (datasetID < 41):
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
        


    
    
    
    
    