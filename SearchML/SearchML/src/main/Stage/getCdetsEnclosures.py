'''
Created on Jun 6, 2016

@author: gopasing
'''

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, countDistinct

import re
import subprocess
import shlex
import io

APP_NAME="Initial Load from CDET Webservice"
Spark_Master = "local[*]"

cdetEnclDict = {'cdetID': None, 'enclosure': None}

def getEnclosureList(cdetID):
    pat = re.compile(r"Diffs-(?P<typ>release|commit)*" +
                         r"-(comp_)*-*(?P<branch>\w*)" +
                         r"\.*(?P<part>\w*)")
    findcr_cmd = "findcr -w Attachment-title -i %s" % cdetID
    print(findcr_cmd)
    args = shlex.split(findcr_cmd)
    listOfEnclosures = subprocess.getoutput(args)
    enclosures = listOfEnclosures.rstrip('\n').split(',')
    enclosures.sort()
    print(enclosures)

    for encl in enclosures:
        cdetEnclDict={}
        print(encl)
        res = pat.match(encl)
        if not res:
            continue
 
        typ = res.group('typ')
        print(typ)
        branch = res.group('branch')
        print(branch)
        if not typ:
            # IOS component publication : Diffs--v153_3_s_xe310_throttle
            typ = 'release'
 
        with open('/scratch/CDETS_Enclosures.txt','a') as out:
            out.write(cdetID+'|'+encl+ '\n')
        cdetEnclDict = {'cdetID': cdetID, 'enclosure': encl}
        if not enclosures[typ].get(branch):
            enclosures[typ][branch] = []
#         #self._parse_enclosure(data)
        enclosures[typ][branch].append(cdetEnclDict)
        return cdetEnclDict
    

def clean_cdetsIdentifier(cdet):
    return re.sub("\'\)","",re.sub("Row\(bugs_Identifier=\'","",cdet))

def clean_row(row):
    return re.sub(',','',re.sub('\"','',str(row)))

header = "CdetID|DiffFile".split("|")

def make_row(row):
    row_list = row.split("|")
    
    dictObject = dict(zip(header,row_list))
    
    return dictObject

def getBugID(content):
    bugid=""
    bugregex = re.compile(r'(# BugId:\s\w+)')
    print(bugregex)
    print(content)
    lines = content.split('\n')
    for line in lines:
        line = line.rstrip('\n')
        bugidmatch = bugregex.match(line)
        if bugidmatch:
            bugid = bugidmatch.group(1)
            
    
    return bugid

def getFilenames(content):
    filenames =[]
    filenameregex = re.compile(r'Index:(.*)')
    lines = content.split('\n')
    for line in lines:
        line = line.rstrip('\n')
        filematch = filenameregex.match(line)
        if filematch:
            filenames.append(filematch.group(1))
    
    return filenames         

def saveTodisk(content):
    with io.FileIO("/scratch/cdetsEnclosureDetails.csv","a") as file:
        file.write(content)
    

def generateRow(bugID,filenameList,AcmeComponentList):
    for comp in AcmeComponentList:
        #print(comp[0])
        for file in filenameList:
            #print(file)
            if str(comp[0]) in str(file):
                print(bugID,file,comp[0],comp[1])
                saveTodisk(bugID+"|"+file+"|"+comp[0]+"|"+comp[1])
                
    return 0 

def getAcmeComponentBranchVersion(bugID,filenameList,content):
    #AcmeComponetDict ={}
    flagRefPointStart = 0
    dictheader = "AcmeComponent|ComponentVersion".split("|")
    refpointregex = re.compile(r'Refpoints:')
    endlogentryregex = re.compile(r'end log-entry')
    lines = content.split('\n')
    for line in lines:
        line = line.rstrip('\n')
        RefpointMatch = refpointregex.match(line)
        if RefpointMatch:
            flagRefPointStart = 1
            #print("RefpointMatch = " + str(flagRefPointStart))
        endlogentryMatch =endlogentryregex.match(line)
        if endlogentryMatch:
            flagRefPointStart = 0
            #print("flagRefPointStart = " + str(flagRefPointStart))
        if flagRefPointStart == 1 and line !='Refpoints:':
            print(line)
            row_list = line.strip().split()
            row_list = list(filter(None,row_list))
            print(row_list)
            
            generateRow(bugID,filenameList,row_list)
            
            AcmeComponetDict = dict(zip(dictheader,row_list))
            print(AcmeComponetDict)    
                
    return AcmeComponetDict

def get_enclosure_content(cdetID_encl):
    fileregex = re.compile(r'(.*)(Diffs-release*|Diffs-commit*)')
    cdetID = cdetID_encl.split("|")[0]
    encl= cdetID_encl.split("|")[1]
    if re.match(fileregex, encl):
        dumpcr_cmd = "dumpcr -e -u -a %s %s" % (encl,cdetID)
        print(dumpcr_cmd)
        args = shlex.split(dumpcr_cmd)
        try:
            content = subprocess.run(args, stdout=subprocess.PIPE,universal_newlines=True)
            print(content.stdout)
            bugID = re.sub("# BugId: ","",getBugID(content.stdout))
            print("bugid = " + str(bugID))
            filenameList = getFilenames(content.stdout)
            print(filenameList)
            acmecomponentBranchVersionDict = getAcmeComponentBranchVersion(bugID,filenameList,content.stdout)
            print(acmecomponentBranchVersionDict)
            #acmecomponentBranchVersionDict = getAcmeComponentBranchVersion(re.sub('\n','--->',content.stdout))
        except UnicodeDecodeError as e:
            print(e, " while running " + dumpcr_cmd)
            with io.FileIO("/scratch/error.txt","a") as file:
                file.write(e.encode('utf-8') + " while running " + dumpcr_cmd)
            pass    


def main(sc):
    cdetEnclosureData = sc.textFile("file:///scratch/CDETS_Enclosures.txt")
    cdetEnclosureDataClean = cdetEnclosureData.map(clean_row)
    dataDict = cdetEnclosureDataClean.map(make_row)
    cdetEnclosureDataCleanDF = sqlContext.createDataFrame(dataDict)
    cdetEnclosureDataCleanDF.groupBy("CdetID").count().show()
    cdetEnclosureDataCleanDF.agg(countDistinct("CdetID")).show()
    
    #Call the dumpcr command
    cdetEnclosureData.foreach(get_enclosure_content)
    
#     cdetsJsonDF = sqlContext.read.json("file:///scratch/tux/cdetsweb_prd_Json.json")
#     bugsDF = cdetsJsonDF.select("bugs")
#     explodedbugsDF= bugsDF.select(explode(bugsDF.bugs)).withColumnRenamed("col","bugs")
#     cdets = explodedbugsDF.select("bugs.Identifier").rdd
#     cdetsIdentifierRDDClean = cdets.map(lambda row: clean_cdetsIdentifier(str(row)))
#     print(cdetsIdentifierRDDClean.take(4))
#     cdetsIdentifierRDDClean.saveAsTextFile("file:///scratch/cdets.txt")
#     cdetsIdentifierRDDClean.foreach(getEnclosureList)

    



if __name__=="__main__":
    print("start")
    sconf = SparkConf().setAppName(APP_NAME).setMaster("local[4]")
    #conf = SparkConf().setAppName("PySpark Cassandra").setMaster().set("spark.driver.memory","2g").set("spark.executor.memory", "4g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.cleaner.ttl","300") #.set("spark.cleaner.ttl","300")
    sc = SparkContext(conf=sconf)
    sqlContext = SQLContext(sc)
    #main(sqlContext)
    main(sc)