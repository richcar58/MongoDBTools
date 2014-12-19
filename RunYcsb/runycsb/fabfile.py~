'''
Created on Dec 12, 2014

This script times mongod execution under various configuration and with
various loads as specified in the seriesConfig.json file that must 
reside in the same directory as this script. 

@author: rich
'''

# Imports
from SeriesEnv import SeriesEnv
import sys, os
from datetime import datetime
from time import sleep
from fabric.api import run, settings, env
from fabric.main import main

# Sample commands.
# bin/mongod --dbpath /home/rich/pkgs/mongodb-latest/mongodb-linux-x86_64-2.8.0-rc1/data/db 
# --logpath /mnt/Disk1-2TB/logs/mongo-thread/mongod-15million-12thread.log --storageEngine wiredTiger

# time bin/ycsb load mongodb -p recordcount=15000000 -p operationcount=30000000 -p threadcount=1 -p hosts=localhost -P workloads/workloada

# --------------------------------------------------------
# Fields
# --------------------------------------------------------
# The mapping of configuration parameters 
# read from the configuration json file.
seriesEnv = SeriesEnv()  

# -------------------------------------------------------- 
# run_all
# --------------------------------------------------------
def run_all():
    """
    Highest level test.
    """ 
    print(sys.argv)
    print(vars(seriesEnv))
    run_mongo()
    
# -------------------------------------------------------- 
# run_mongo
# --------------------------------------------------------
def run_mongo():
    """
    Run the mongo test suite
    """
    starttime = datetime.now()
    print('>> Starting run_mongo [' + str(starttime) + ']') 
    _mongo_stop()
    _mongo_setup()
    _mongo_clean()  
    
    # We know the recordCount and operationCount arrays are the
    # same non-zero length, so the inner loop is sound.
    for workload in seriesEnv.seriesConfig['ycsb_workloads']:
        for j in range(len(seriesEnv.seriesConfig['mongo_parms'])):
            for i in range(len(seriesEnv.seriesConfig['ycsb_recordcount'])):
                # Get the corresponding counts from the two arrays.
                recordCount = seriesEnv.seriesConfig['ycsb_recordcount'][i]
                operationCount = seriesEnv.seriesConfig['ycsb_operationcount'][i]
                
                # Determine the storage abbreviation for file naming.
                mongoParms = seriesEnv.seriesConfig['mongo_parms'][j]
                storageAbbrev = _getStorageAbbreviation(mongoParms, j)
                
                # Start mongo, load and run ycsb, and clean up.
                _mongo_start(recordCount, mongoParms, workload, storageAbbrev)
                _ycsb('load', 'mongodb', recordCount, operationCount, workload, storageAbbrev)
                _ycsb('run', 'mongodb', recordCount, operationCount, workload, storageAbbrev)
                _mongo_stop()
                _mongo_clean()

    endtime = datetime.now()
    print('>>>> Completing run_mongo [' + str(endtime) + ', duration = ' + str(endtime-starttime) + ']') 
    
# -------------------------------------------------------- 
# mongo_clean
# --------------------------------------------------------
def _mongo_clean():
    """
    Remove all existing mongodb databases.
    """
    starttime = datetime.now()
    print('>>>> Starting mongo_clean [' + str(starttime) + ']') 
    with settings(warn_only=True):
        run("rm -rf %s/*" % seriesEnv.dbpath)

# -------------------------------------------------------- 
# mongo_setup
# --------------------------------------------------------
def _mongo_setup():
    """
    Ensure that required directories exist.
    """
    starttime = datetime.now()
    print('>>>> Starting mongo_setup [' + str(starttime) + ']') 
    
    # Construct mongo_logpath (assumes Linux)
    with settings(warn_only=True):
        if run("test -d %s" % seriesEnv.logpath).failed:
            run("mkdir -p %s" % seriesEnv.logpath)
           
    # Construct mongo_dbpath (assumes Linux)
    with settings(warn_only=True):
        if run("test -d %s" % seriesEnv.dbpath).failed:
            run("mkdir -p %s" % seriesEnv.dbpath)
  
# -------------------------------------------------------- 
# mongo_start
# --------------------------------------------------------
def _mongo_start(recordCount, mongoParms, workload, storageAbbrev):
    """
    Start mongod in the background.
    """
    starttime = datetime.now()
    print('>>>> Starting run_mongo [' + str(starttime) + ']') 
    
    # Start command string.
    mongoCmd = os.path.join(seriesEnv.seriesConfig['mongo_bin_path'], 'mongod')
    mongoCmd += " --dbpath " + seriesEnv.dbpath 
    
    # Add log file.
    # Determine storage engine abbreviation for log naming purposes.
    mongoCmd += " --logpath " + os.path.join(seriesEnv.logpath, 
                _make_log_filename('mongod', storageAbbrev, recordCount,
                     seriesEnv.seriesConfig['ycsb_threadcount'], workload))
    
    # Add all other parms.
    mongoCmd += " " + mongoParms
    mongoCmd += " --fork" 
    run(mongoCmd)
    sleep(8)

# -------------------------------------------------------- 
# mongo_stop
# --------------------------------------------------------
def _mongo_stop():
    """Stop all mongod instances"""
    starttime = datetime.now()
    print('>>>> Starting mongo_stop [' + str(starttime) + ']') 
    with settings(warn_only=True):
        run('killall mongod')
        sleep(5)

# -------------------------------------------------------- 
# _ycsb
# --------------------------------------------------------
def _ycsb(action, product, recordCount, operationCount, workload, storageAbbrev):
    """Execute ycsb load or run actions."""
    starttime = datetime.now()
    print('>>>> Starting _ycsb [' + str(starttime) + ']') 
    
    # Construct the logfile name.
    logfile = os.path.join(seriesEnv.logpath, 
                    _make_log_filename('ycsb', storageAbbrev, recordCount, seriesEnv.seriesConfig['ycsb_threadcount'], workload))
    
    # Start command string.
    ycsbCmd = os.path.join(seriesEnv.seriesConfig['ycsb_bin_path'], 'ycsb')
    ycsbCmd += " " + action + " " + product 
    ycsbCmd += " -p recordcount=" + str(recordCount)
    ycsbCmd += " -p operationcount=" + str(operationCount) 
    ycsbCmd += " -p threadcount=" + str(seriesEnv.seriesConfig['ycsb_threadcount']) 
    ycsbCmd += " -p env.hosts=" + env.host
    ycsbCmd += " -P " + os.path.normpath(os.path.join(seriesEnv.seriesConfig['ycsb_bin_path'], 
                           "../workloads/"+workload)) 
    ycsbCmd += " >> " + logfile + " 2>&1"
    run(ycsbCmd)
    
    endtime = datetime.now()
    print('>>>> Completing _ycsb [' + str(endtime) + ', duration = ' + str(endtime-starttime) + ']\n') 

# -------------------------------------------------------- 
# _make_log_filename
# --------------------------------------------------------
def _make_log_filename(program, storageAbbrev, records, threads, workload):
    """Construct a log file name based on this run's parameters."""
    fn = program + "-" + storageAbbrev
    fn += "-" + workload
    fn += "-" + str(records) + "recs"
    fn += "-" + str(threads) + 'thrds.log'
    return fn

# -------------------------------------------------------- 
# _getStorageAbbreviation
# --------------------------------------------------------
def _getStorageAbbreviation(mongoParms, index):
    """
    Get the storage engine abbreviation for log naming.
    The name is made of a storage engine moniker and
    an index number that distinguishes every execution
    by its mongod parameter set.
    """
    
    # The elif statement helps futureproof for when wt becomes the default.
    abbrev = "mm"
    if mongoParms.find("wiredTiger") >= 0:
        abbrev = "wt"
    elif mongoParms.find("mmapv1") >= 0:
        abbrev = "mm"
    return abbrev + str(index)

# -------------------------------------------------------- 
# Main
# --------------------------------------------------------
# Allow execution from within Eclipse   
if __name__ == '__main__':
    sys.argv = ['fab', '-f', __file__, 'run_all']
    main()    
    