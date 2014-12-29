'''
Created on Dec 24, 2014

This class is used by the RunYcsb's fabfile script to parse and 
collate the result of a batch execution.

@author: rich
'''

import os, logging, glob

from CollateElement import CollateElement
from SeriesEnv import SeriesEnv


class CollateResults:
    '''
    classdocs
    '''
    
    # --------------------------------------------------------
    # Constants
    # --------------------------------------------------------
    MONGO_LOG_PREFIX = "mongod-"
    YCSB_LOG_PREFIX  = "ycsb-"
    
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
        
        # Write results for each key.
        for key in keyList:
            # Initialize loop vars.
            firstTime = True
            loadAvg = 0
            runAvg  = 0
            loadCnt = 0
            runCnt  = 0
            loadString = "Load (ops/s): "
            runString  = "Run  (ops/s): "
            
            # Create load and run throughput lists.
            elements = self.resultDict[key]
            for element in elements:
                if element.ycsbLoad:
                    loadAvg += element.ycsbLoad.throughput
                    loadCnt += 1
                    if firstTime:
                        loadString += str(element.ycsbLoad.throughput)
                    else:
                        loadString += ", " + str(element.ycsbLoad.throughput)
                if element.ycsbRun:
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
            
            # Create final output string.
            output = '------ ' + key + "\n"
            output += loadString + "\n"
            output += "Load average: " + str(loadAvg) + "\n"
            output += runString + "\n"
            output += "Run average : " + str(runAvg) + "\n"
            
            # Print the current element's output.
            print(output)
    
# -------------------------------------------------------- 
# Main
# --------------------------------------------------------
if __name__ == '__main__':
    x = CollateResults()
    x.LOG.debug("SeriesEnv settings:")
    x.LOG.debug(vars(x.seriesEnv))
    x.report()
