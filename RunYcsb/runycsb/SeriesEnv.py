'''
Created on Dec 12, 2014

This class is used by the RunYcsb's fabfile script to load, parse, validate 
and initialize the configuration data in the series configuration json file.

@author: rich
'''
# bin/mongod --dbpath /home/rich/pkgs/mongodb-latest/mongodb-linux-x86_64-2.8.0-rc1/data/db 
# --logpath /mnt/Disk1-2TB/logs/mongo-thread/mongod-15million-12thread.log --storageEngine wiredTiger

# time bin/ycsb load mongodb -p recordcount=15000000 -p operationcount=30000000 -p threadcount=1 -p hosts=localhost -P workloads/workloada

from fabric.api import env
import json, os, logging

# Constants.
SERIES_CONFIG_FILE = 'seriesConfig.json'

class SeriesEnv():
    '''
    classdocs
    '''
    
    # --------------------------------------------------------
    # Constants
    # --------------------------------------------------------
    DEFAULT_SERIES_REPEAT = 1
    DEFAULT_DRY_RUN = False
    
    # --------------------------------------------------------
    # Fields
    # --------------------------------------------------------
    # The mapping of configuration parameters 
    # read from the configuration json file.
    seriesConfig = {}  
    
    # Paths calculated from input file parameters.
    logpath = None
    dbpath = None
    
    # --------------------------------------------------------
    # Constructor
    # --------------------------------------------------------
    def __init__(self):
        '''
        Constructor
        '''
        
        # Set logging level.
        logging.root.setLevel(logging.INFO)
        
        # Properties expected in the config file are:
        #
        #  hosts                mandatory    array of string
        #  dbpath_root          mandatory    string, path to database directory
        #  logpath_root         mandatory    string, path to root directory for all logs
        #  series_name          mandatory    string, subdirectory name for this run's mongo and ycsb logs 
        #  series_repeat        optional     integer, number of time whole series repeats (default = 1)
        #
        #  dry_run              optional     boolean, echo command be don't execute anything (default = False)
        #
        #  ycsb_bin_path        mandatory    string, path to ycsb bin directory
        #  ycsb_operationcount  optional     integer (ignores negative numbers)
        #  ycsb_recordcount     mandatory    integer
        #  ycsb_threadcount     mandatory    integer
        #  ycsb_workloads       mandatory    array of string
        #   
        #  mongo_bin_path       mandatory    string, path to bin directory containing mongod 
        #  mongo_parms          mandatory    string, all parms other than --dbpath and --logpath 
        #                                            - specify storageEngine with its parms here
        #   
        # Read the configuration file.
        with open(SERIES_CONFIG_FILE, 'r') as fp:
            self.seriesConfig = json.load(fp)
            
        # Validate the configuration. 
        self._validateConfig(self.seriesConfig)
        
        # Process the configuration.
        self._processConfig(self.seriesConfig)
        
        # Write configuration debug message   
        logging.debug("----- seriesConfig:")
        logging.debug(self.seriesConfig)
            
    # --------------------------------------------------------
    # _validateConfig
    # --------------------------------------------------------
    def _validateConfig(self, config):
            
        # Check hosts array
        if (not config.has_key('hosts')) or (not config['hosts']) or (not isinstance(config['hosts'], list)):
            msg = "The hosts parameter is missing, empty or non-array in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check for missing dbpath_root
        if (not config.has_key('dbpath_root')) or (not config['dbpath_root']):
            msg = "The dbpath_root parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check for missing logpath_root
        if (not config.has_key('logpath_root')) or (not config['logpath_root']):
            msg = "The logpath_root parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check for missing mongo_bin_path
        if (not config.has_key('mongo_bin_path')) or (not config['mongo_bin_path']):
            msg = "The mongo_bin_path parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
        
        # Check for missing mongo_parms
        if (not config.has_key('mongo_parms')) or (not config['mongo_parms']) \
            or (not isinstance(config['mongo_parms'], list)):
            msg = "The mongo_storage_engine parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check for missing ycsb_bin_path
        if (not config.has_key('ycsb_bin_path')) or (not config['ycsb_bin_path']):
            msg = "The ycsb_bin_path parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
        
        # Check ycsb_workloads array
        if (not config.has_key('ycsb_workloads')) or (not config['ycsb_workloads']) \
                or (not isinstance(config['ycsb_workloads'], list)):
            msg = "The ycsb_workloads parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check ycsb_recordcount array
        if (not config.has_key('ycsb_recordcount')) or (not config['ycsb_recordcount']) \
                or (not isinstance(config['ycsb_recordcount'], list)):
            msg = "The ycsb_recordcount parameter is missing, empty or non-array in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
         
        # Check ycsb_operationcount array
        # If the key exists and is a non-empty array, it must have the same number of elements
        # as the ycsb_recordcount array.  If it's not an array, it will be replaced by an
        # array in the process method.
        if config.has_key('ycsb_operationcount') and isinstance(config['ycsb_operationcount'], list) \
                and config['ycsb_operationcount'] \
                and (not len(config['ycsb_operationcount']) == len(config['ycsb_recordcount'])):
            msg = "Arrays ycsb_recordcount and ycsb_operationcount are not the same length in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
         
        # Check ycsb_threadcount
        if (not config.has_key('ycsb_threadcount')) or (not config['ycsb_threadcount']):
            msg = "The ycsb_threadcount parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check series_name
        if (not config.has_key('series_name')) or (not config['series_name']):
            msg = "The series_name parameter is missing or empty in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check series_repeat
        if (config.has_key('series_repeat')) and config['series_repeat'] \
                and (not isinstance(config['series_repeat'], (long, int))):
            msg = "The optional series_repeat parameter must be specified as an integer value in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
        # Check dry_run
        if (config.has_key('dry_run')) and config['dry_run'] \
                and (not isinstance(config['dry_run'], bool)):
            msg = "The optional dry_run parameter must be specified as a boolean value in configuration file " \
                  + SERIES_CONFIG_FILE + "."
            raise Exception(msg) 
            
    # --------------------------------------------------------
    # _processConfig
    # --------------------------------------------------------
    def _processConfig(self, config):
            
        # Assign fabric env hosts variable
        env.hosts = config['hosts']
            
        # Construct mongo_logpath (assumes Linux)
        self.logpath = os.path.join(config['logpath_root'], config['series_name'])
           
        # Construct mongo_dbpath (assumes Linux)
        self.dbpath = config['dbpath_root']
            
        # Make sure the series repeat number is always assigned.
        if (not config.has_key('series_repeat')) or (not config['series_repeat']):  
            config['series_repeat'] = self.DEFAULT_SERIES_REPEAT;  
            
        # Make sure the dry_run value is always assigned.
        if (not config.has_key('dry_run')) or (not config['dry_run']):  
            config['dry_run'] = self.DEFAULT_DRY_RUN;  
            
        # Assign default operation count if none provided.  On average,
        # we do a read and an update on each record.
        if (not config.has_key('ycsb_operationcount')) or (not config['ycsb_operationcount']) \
                or (not isinstance(config['ycsb_operationcount'], list)):
            config['ycsb_operationcount'] = map(lambda x : 2 * x, config['ycsb_recordcount'][:]) 
        
# -------------------------------------------------------- 
# Main
# --------------------------------------------------------
if __name__ == '__main__':
    x = SeriesEnv()
    logging.info("SeriesEnv settings:")
    logging.info(vars(x))

