'''
Created on Dec 24, 2014

This class is used by the RunYcsb's fabfile script to parse and 
collate the result of a batch execution.

@author: rich
'''

import os, logging, glob

from CollateElement import CollateElement
from SeriesEnv import SeriesEnv
from mystats import stddev

class CollateResults:
    '''
    classdocs
    '''
    
    # --------------------------------------------------------
    # Constants
    # --------------------------------------------------------
    MONGO_LOG_PREFIX = "mongod-"
    YCSB_LOG_PREFIX  = "ycsb-"
    CSV_FILENAME = "RunYcsb.csv"
    
    # --------------------------------------------------------
    # Class Variables
    # --------------------------------------------------------
    # Set the log level here.
    LOG = logging.getLogger('CollateResults')
    LOG.setLevel(logging.INFO)
    LOG.addHandler(logging.StreamHandler())
    
    # --------------------------------------------------------
    # Constructor
    # --------------------------------------------------------
    def __init__(self, seriesEnvParm=None):
        # The mapping of configuration parameters 
        # read from the configuration json file.
        if (seriesEnvParm):
            self.seriesEnv = seriesEnvParm
        else: 
            self.seriesEnv = SeriesEnv()
            
        # Initialize the result dictionary.
        self.resultDict = {}
            
        # Collate the mongod and ycsb log file information.
        self.collate()
            
    # --------------------------------------------------------
    # collate
    # --------------------------------------------------------
    def collate(self):
        # Check input.
        if (not self.seriesEnv.logpath):
            msg = "The series environment variable does not have a valid logpath."
            raise Exception(msg) 
        
        # Read list of mongo result files from result directory.
        mongoLogFilter = os.path.join(self.seriesEnv.logpath, self.MONGO_LOG_PREFIX + "*.log")
        mongoLogPaths = glob.glob(mongoLogFilter)
        mongoLogPaths.sort() 
        
        # Main read loop.
        print("Number of mongo log files found: " + str(len(mongoLogPaths)))
        for mongoLog in mongoLogPaths:
            
            # Get the mongod settings.
            element = CollateElement(mongoLog)
            element.readMongoOptions()
                
            # Get the ycsb settings.
            element.readYcsbOptions()
            
            # Tracing.
            self.LOG.debug(vars(element))
            
            # Add the collation results to the result dictionary.
            dictList = self.resultDict.get(element.getKey())
            if dictList:
                dictList.append(element)
            else:
                dictList = [element]
                self.resultDict[element.getKey()] = dictList
        
    # --------------------------------------------------------
    # report
    # --------------------------------------------------------
    def report(self):
        # Sort the list of result keys.
        keyList = self.resultDict.keys()
        keyList.sort()
        self.LOG.debug("Number of result elements: " + str(len(keyList)))
        
        # Initialize CSV variables
        csvList = []
        csvDelimiter = self.seriesEnv.seriesConfig['csv_delimiter']
        
        # Write results for each key.
        for key in keyList:
            # Initialize loop vars.
            firstTime = True
            loadAvg = 0
            runAvg  = 0
            loadList = []
            runList  = []
            loadStdev = 0
            runStdev  = 0
            loadCnt = 0
            runCnt  = 0
            loadString = "Load (ops/s): "
            runString  = "Run  (ops/s): "
            
            # Create load and run throughput lists.
            elements = self.resultDict[key]
            for element in elements:
                if element.ycsbLoad:
                    loadList.append(element.ycsbLoad.throughput)
                    loadAvg += element.ycsbLoad.throughput
                    loadCnt += 1
                    if firstTime:
                        loadString += str(element.ycsbLoad.throughput)
                    else:
                        loadString += ", " + str(element.ycsbLoad.throughput)
                if element.ycsbRun:
                    runList.append(element.ycsbRun.throughput)
                    runAvg += element.ycsbRun.throughput
                    runCnt += 1
                    if firstTime:
                        runString += str(element.ycsbRun.throughput)
                    else:
                        runString += ", " + str(element.ycsbRun.throughput)
                firstTime = False    
            
            # Calculate throughput averages.
            loadAvg = loadAvg // loadCnt
            runAvg  = runAvg  // runCnt
            
            # Calculate throughput standard deviations.
            loadStdev = int(stddev(loadList))
            runStdev  = int(stddev(runList))
            
            # Conditionally print the current element's output.
            if self.seriesEnv.seriesConfig['report']:
                output = '------ ' + key + "\n"
                output += loadString + "\n"
                output += "Load average: " + str(loadAvg) + "\n"
                output += "Load stdev: " + str(loadStdev) + "\n"
                output += runString + "\n"
                output += "Run average : " + str(runAvg) + "\n"
                output += "Run stdev: " + str(runStdev) + "\n"
                print(output)
    
            # Conditionally accumulate csv file records.  
            # Note that RFC 4180 specifies DOS-style line end and
            # two double quotes in a string to signify a double 
            # quote character.  If strings are double quoted then
            # internal delimiter characters won't be interpreted as
            # delimiters.
            if self.seriesEnv.seriesConfig['csv_file']:
                formattedKey = '"' + key.replace('"', '""') + '"'
                loadRec = formattedKey + csvDelimiter + '"load"' + csvDelimiter + str(loadAvg) + \
                            csvDelimiter + str(loadCnt) + csvDelimiter + str(loadStdev)
                runRec = formattedKey + csvDelimiter + '"run"' + csvDelimiter + str(runAvg) + \
                            csvDelimiter + str(runCnt) + csvDelimiter + str(runStdev)
                csvList.append(loadRec + "\r\n")
                csvList.append(runRec + "\r\n")
         
        # Optionally write csv file. 
        if self.seriesEnv.seriesConfig['csv_file']:
            csvFile = os.path.join(self.seriesEnv.logpath, self.CSV_FILENAME)
            with open(csvFile, 'w') as f:
                for rec in csvList:
                    f.write(rec)
            
# -------------------------------------------------------- 
# Main
# --------------------------------------------------------
if __name__ == '__main__':
    x = CollateResults()
    x.LOG.debug("SeriesEnv settings:")
    x.LOG.debug(vars(x.seriesEnv))
    x.report()
