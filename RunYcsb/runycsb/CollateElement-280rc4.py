'''
Created on Dec 27, 2014

This class is used by the RunYcsb's CollateResults script  
as a container for the results of mongod and YCSB executions.

@author: rich
'''
import logging
from CollateYcsb import CollateYcsb

class CollateElement:
    '''
    This class recognizes the mongod configString option used with WiredTiger
    before 2.8.0 rc5.  Use CollateElement.py for 2.8.0 release candidates after rc4
    and this class for release candidates up to and including rc4.  Using this 
    class entails changing the CollateResults.py code or renaming the CollateElement*.py
    files.  Either way, it's not pretty but this code will probably be obsolete soon. 
    '''
    # --------------------------------------------------------
    # Constants
    # --------------------------------------------------------
    # Supported storage engine types.
    STORAGE_ENGINE_MMAPV1 = 'mm'
    STORAGE_ENGINE_WIREDTIGER = 'wt'
    
    # Log records line search filters.
    SEARCH_CMDLINE = "Command line: "
    SEARCH_RUNTIME = "[OVERALL], RunTime(ms), "
    SEARCH_OPS = "[OVERALL], Operations, "
    SEARCH_THROUGHPUT = "[OVERALL], Throughput(ops/sec), "
                
    # --------------------------------------------------------
    # Class Variables
    # --------------------------------------------------------
    # Set the log level here.
    LOG = logging.getLogger('CollateElement')
    LOG.setLevel(logging.INFO)
    LOG.addHandler(logging.StreamHandler())
    
    # Input parm
    mongoLogFileName = None
    
    # --------------------------------------------------------
    # Constructor
    # --------------------------------------------------------
    def __init__(self, mongoLogFileName):
        '''
        Validate parms and assign instance fields.
        '''
        if not mongoLogFileName:
            msg = 'Missing mongod log file name.'
            raise Exception(msg) 
        else:
            self.mongoLogFileName = mongoLogFileName

        # YCSB parameters gleaned from mongod log file.
        self.storageEngine = None
        self.isJournaling = True
        self.checkpointSetting = None
    
        # YCSB results gleaned from ycsb log file.
        self.ycsbLoad = None
        self.ycsbRun = None

    # --------------------------------------------------------
    # readMongoOptions
    # --------------------------------------------------------
    def readMongoOptions(self):
        '''
        Read the mongod execution options from the mongo log file
        used to initialize this object. 
        '''
        
        # Open the log file for reading and iterate through its
        # lines until the options record is found.
        with open(self.mongoLogFileName, 'r') as f:
            for line in f:
                startOptions = line.find(" options:")
                if startOptions > -1:
                    options = line[startOptions:-1]
                    self.LOG.debug(options)
                    
                    # Determine storage engine and its configuration parms.
                    # -- MMAPV1
                    if options.find(' engine: "mmapv1"') > -1:
                        self.storageEngine = self.STORAGE_ENGINE_MMAPV1
                        self.LOG.debug(" ** found mmapv1")
                    # -- WiredTiger
                    elif options.find(' engine: "wiredTiger"') > -1:
                        self.storageEngine = self.STORAGE_ENGINE_WIREDTIGER
                        
                        # -- Get journaling setting.
                        self.isJournaling = True
                        if options.find(' journal: { enabled: false }') > -1: 
                            self.isJournaling = False
                            
                        # Get checkpoint setting.  If it exists, it looks something like:
                        #
                        #  configString: "checkpoint=(wait=10)"
                        #  configString: "checkpoint="
                        #
                        configString = ' configString: "'
                        startConfigString = options.find(configString)
                        if startConfigString > -1:
                            # Find the closing double quote (which we expect to be there).
                            # We extract the substring that contains the opening and closing
                            # double quotes.  Note that this parse is sensitive to whitespace
                            # and otherwise not particularly resilient.
                            nextDQuote = options[startConfigString+len(configString):].find('"')
                            self.checkpointSetting = options[
                                    startConfigString+len(configString)-1:
                                      startConfigString+len(configString)+1+nextDQuote
                                ]
                        self.LOG.debug(" ** found wiredTiger: journal=" + str(self.isJournaling) + \
                                      ", checkpointSetting=" + str(self.checkpointSetting)) 
                    # -- No known storage engine specified.
                    else:
                        msg = "No known storage engine designated in options record in log file" + \
                              self.mongoLogFileName + ":\n  " + line 
                        self.LOG.error(msg)
                        raise Exception(msg)
                    
                    # No need to read any more lines in the current file.
                    break

        # Did we find everything we needed?
        if not self.storageEngine:
            msg = "No storage engine designation found in log file " + self.mongoLogFileName + "." 
            raise Exception(msg)     
 
    # --------------------------------------------------------
    # readYcsbOptions
    # --------------------------------------------------------
    def readYcsbOptions(self):
        '''
        Read the YCSB execution options from the ycsb log file
        that corresponds to the mongo log file set in this object. 
        '''
        
        # Get the ycsb log file for this mongod execution.
        ycsbLogFileName = self._getYcsbLogFileName()
        self.LOG.debug("Reading " + ycsbLogFileName)
        
        # We are only concerned with 4 types of log file lines.
        # We iterate through the log file looking for these 4
        # line types, one set for load and one set for run.
        # We only record the ycsb results if we find the last
        # line, which contains the throughput information.
        #
        # Note that only the last instance of the load or run
        # results are recorded.  This behavior allows for 
        # manual restarts to log to an existing file since the
        # latest load or run results will always be appended to 
        # the end of the file.
        with open(ycsbLogFileName, 'r') as f:
            collateYcsb = None
            for line in f:
                if line.find(self.SEARCH_CMDLINE) > -1:
                    collateYcsb = CollateYcsb(ycsbLogFileName)
                    collateYcsb.parseCmdLine(line)
                elif line.find(self.SEARCH_RUNTIME) > -1:
                    collateYcsb.parseRuntime(line)
                elif line.find(self.SEARCH_OPS) > -1:
                    collateYcsb.parseOps(line)
                elif line.find(self.SEARCH_THROUGHPUT) > -1:
                    collateYcsb.parseThroughput(line)
                    
                    # Save the completed result information
                    # in the appropriate instance field.
                    if collateYcsb.load:
                        self.ycsbLoad = collateYcsb
                    else:
                        self.ycsbRun = collateYcsb
                    self.LOG.debug(vars(collateYcsb))
                    collateYcsb = None

    # --------------------------------------------------------
    # _getYcsbLogFileName
    # --------------------------------------------------------
    def getKey(self):
        '''
        Create a key string from the value in this element.
        It is expected that all mongod and ycsb values have
        been parsed and assigned to instance variables. 
        '''
        
        # The key will determine sort order in the final output,
        # so the order in which values are composed is significant.
        key = self.storageEngine
        key += "|" + self.ycsbLoad.workloadFile
        if not self.isJournaling:
            key += "|nojournal"
        if self.storageEngine is self.STORAGE_ENGINE_WIREDTIGER:
            if self.checkpointSetting:
                key += "|" + self.checkpointSetting
                
        # We use the load count parameters which are always the
        # same as the run count parameters.
        key += "|recs=" + str(self.ycsbLoad.recordCount)
        key += "|ops=" + str(self.ycsbLoad.opCount)
                
        return key                
        
    # --------------------------------------------------------
    # _getYcsbLogFileName
    # --------------------------------------------------------
    def _getYcsbLogFileName(self):
        '''
        Get the ycsb log file name that corresponds to this
        instance's mongod log file name.
        '''
        
        # Replace the leading "mongod" characters in the mongo file
        # name with "ycsb".  Handle cases where the mongod prefix
        # appears in more than 1 place in the pathname as well as
        # the case when the log file is in the root directory.
        if self.mongoLogFileName.find("/") > 0:
            namelist = self.mongoLogFileName.rsplit('/', 1)
            return namelist[0] + "/" + namelist[1].replace("mongod-", "ycsb-")
        else:
            return self.mongoLogFileName.replace("mongod-", "ycsb-")
