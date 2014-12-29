'''
Created on Dec 28, 2014

This class is used by the RunYcsb's CollateElement script  
as a container for the results of individual YCSB executions.

@author: rich
'''

class CollateYcsb:
    
    # --------------------------------------------------------
    # Constants
    # --------------------------------------------------------
    SEARCH_RECORD_COUNT = "recordcount="
    SEARCH_OP_COUNT = "operationcount="
    SEARCH_LOAD = " -load"
    SEARCH_WORKLOAD_FILE = " -P "
    
    # --------------------------------------------------------
    # Constructor
    # --------------------------------------------------------
    def __init__(self, ycsbLogFileName):
        '''
        Assign current ycsb log file name and initialize other fields.
        Note that numeric fields are stored as integers with any 
        fractional parts truncated.
        '''
        self.ycsbLogFileName = ycsbLogFileName
        self.workloadFile = "unknown_workload"
        self.recordCount = -1
        self.opCount = -1
        self.runtimeMs = -1
        self.totalOps = -1
        self.throughput = -1
        self.load = False
        
    # --------------------------------------------------------
    # parseCmdLine
    # --------------------------------------------------------
    def parseCmdLine(self, line):
        # Find the record count and split on whitespace.
        recordCountIndex = line.find(self.SEARCH_RECORD_COUNT)
        if recordCountIndex > -1:
            self.recordCount = line[recordCountIndex+len(self.SEARCH_RECORD_COUNT):]
            self.recordCount = self.recordCount.split(None, 1)[0]
            self.recordCount = int(float(self.recordCount))
                    
        # Find the operation count and split on whitespace.
        opCountIndex = line.find(self.SEARCH_OP_COUNT)
        if opCountIndex > -1:
            self.opCount = line[opCountIndex+len(self.SEARCH_OP_COUNT):]
            self.opCount = self.opCount.split(None, 1)[0] 
            self.opCount = int(float(self.opCount))
            
        # Get the last segment of workloadFile pathname.
        workloadIndex = line.find(self.SEARCH_WORKLOAD_FILE)
        if workloadIndex > -1:
            self.workloadFile = line[workloadIndex+len(self.SEARCH_WORKLOAD_FILE):]
            self.workloadFile = self.workloadFile.strip()
            self.workloadFile = self.workloadFile.split(None, 1)[0]
            self.workloadFile = self.workloadFile.rsplit("/", 1)[1]
                    
        # Make sure everything went according to plan.
        if (not self.recordCount) or (not self.opCount):
            msg = "Expected recordcount and operationcount settings in " + \
                  self.ycsbLogFileName + ":\n  " + line
            raise Exception(msg)
                    
        # Determine if this is the load or run invocation.
        if line.find(self.SEARCH_LOAD) > -1:
            self.load = True
                        
    # --------------------------------------------------------
    # parseRuntime
    # --------------------------------------------------------
    def parseRuntime(self, line):
        self.runtimeMs = line.rsplit(None, 1)[1]
        self.runtimeMs = int(float(self.runtimeMs))
    
    # --------------------------------------------------------
    # parseOps
    # --------------------------------------------------------
    def parseOps(self, line):
        self.totalOps = line.rsplit(None, 1)[1]
        self.totalOps = int(float(self.totalOps))
        
    # --------------------------------------------------------
    # parseThroughput
    # --------------------------------------------------------
    def parseThroughput(self, line):
        self.throughput = line.rsplit(None, 1)[1]
        self.throughput = int(float(self.throughput))
