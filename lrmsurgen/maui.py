#
# Maui log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import os
import time
import datetime
import logging

from lrmsurgen import config, usagerecord

try:
    from xml.etree import ElementTree as ET
except ImportError:
    # Python 2.4 compatability
    from elementtree import ElementTree as ET


MAUI_DATE_FORMAT = '%a_%b_%d_%Y'
DEFAULT_LOG_DIR  = '/var/spool/maui'
STATS_DIR        = 'stats'
STATE_FILE       = 'maui.state'



class MauiLogParser:
    """
    Parser for maui stats log.
    """
    def __init__(self, log_file):
        self.log_file = log_file
        self.file_ = None


    def openFile(self):
        self.file_ = open(self.log_file)


    def splitLineEntry(self, line):
        fields = [ e.strip() for e in line.split(' ') if e != '' ]
        assert len(fields) == 44, 'Incorrect number of fields in Maui log entry'
        return fields


    def getNextLogLine(self):
        if self.file_ is None:
            self.openFile()

        while True:
            line = self.file_.readline()
            if line.startswith('VERSION'):
                continue # maui log files starts with a version, typically 230
            if line.startswith('#'):
                continue # maui somtimes creates explanatory lines in the log file
            if line == '': # last line
                return None

            return line


    def getNextLogEntry(self):
        line = self.getNextLogLine()
        if line is None:
            return None
        return self.splitLineEntry(line)


    def spoolToEntry(self, entry_id):
        while True:
            log_entry = self.getNextLogEntry()
            if log_entry is None or log_entry[0] == entry_id:
                break




def createUsageRecord(log_entry, hostname, usermap):
    """
    Creates a Usage Record object given a Maui log entry.
    """
    ur = usagerecord.UsageRecord()

    job_id    = log_entry[0]
    user_name = log_entry[3]
    job_state = log_entry[6]

    if not user_name in usermap:
        logging.warning('Job %s: No mapping for username %s in user map.' % (job_id, user_name))

    ur.record_id = job_id + '.' + hostname

    ur.local_job_id = job_id
    ur.global_job_id = job_id + '.' + hostname

    ur.local_user_id = user_name
    ur.global_user_name = usermap.get(user_name)

    ur.machine_name = hostname

    r_class = log_entry[7]
    r_class = r_class.replace('[','').replace(']','')
    if ':' in r_class:
        r_class = r_class.split(':')[0]

    ur.queue = r_class
    ur.node_count = int(log_entry[1]) or 1 # set to 1 if 0

    ur.submit_time = usagerecord.epoch2isoTime(int(log_entry[8]))
    ur.start_time  = usagerecord.epoch2isoTime(int(log_entry[10]))
    ur.end_time    = usagerecord.epoch2isoTime(int(log_entry[11]))

    ur.cpu_duration = float(log_entry[29])
    ur.wall_duration = float(log_entry[11]) - float(log_entry[10])

    acc_name = log_entry[25]
    if acc_name != '[NONE]':
        ur.project_name = acc_name

    return ur



def shouldGenerateUR(log_entry, usermap):
    """
    Decides wheater a log entry is 'suitable' for generating
    a ur from.
    """
    job_id    = log_entry[0]
    user_name = log_entry[3]
    job_state = log_entry[6]

    if not job_state == 'Completed':
        logging.info('Job %s: Skipping UR generation (state %s)' % (job_id, job_state))
        return False
    if user_name in usermap and usermap[user_name] is None:
        logging.info('Jobs %s: User configured to skip UR generation' % job_id)
        return False

    return True



def getMauiDate(gmtime):
    """
    Returns a maui date, e.g., 'Thu_Dec_10_2009', given a time.gmtime object.
    """
    return time.strftime(MAUI_DATE_FORMAT, gmtime)



def getIncrementalMauiDate(maui_date):
    """
    Returns the following day in maui date format, given a date
    in maui date format.
    """
    gm_td = time.strptime(maui_date, MAUI_DATE_FORMAT)
    d = datetime.date(gm_td.tm_year, gm_td.tm_mon, gm_td.tm_mday)
    day = datetime.timedelta(days=1)
    d2 = d + day
    next_maui_date = time.strftime(MAUI_DATE_FORMAT, d2.timetuple())
    return next_maui_date



def getStateFileLocation(cfg):
    """
    Returns the location of state file
    The state file contains the information of whereto the ur generation has been processed
    """
    state_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.STATEDIR, config.DEFAULT_STATEDIR)
    state_file = os.path.join(state_dir, STATE_FILE)
    return state_file



def getGeneratorState(cfg):
    """
    Get state of where to the UR generation has reached in the maui log.
    This is two string tuple containing the jobid and the log file.
    """
    state_file = getStateFileLocation(cfg)
    if not os.path.exists(state_file):
        # no statefile -> we start from today
        t_old = time.time() - 500000
        return None, getMauiDate(time.gmtime(t_old))

    state_data = open(state_file).readline().strip() # state is only on the first line
    job_id, maui_date = state_data.split(' ', 2)
    if job_id == '-':
        job_id = None
    return job_id, maui_date


def writeGeneratorState(cfg, job_id, maui_log_file):
    """
    Write the state of where the maui logs have been parsed to.
    This is a job id and maui date (log file and entry).
    """
    state_file = getStateFileLocation(cfg)
    state_data = '%s %s' % (job_id or '-', maui_log_file)

    dirpath = os.path.dirname(state_file)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, mode=0750)

    f = open(state_file, 'w')
    f.write(state_data)
    f.close()



def generateUsageRecords(cfg, hostname, usermap):
    """
    Starts the UR generation process.
    """

    maui_spool_dir = config.getConfigValue(cfg, config.SECTION_MAUI, config.MAUI_SPOOL_DIR,
                                           config.DEFAULT_MAUI_SPOOL_DIR)

    maui_date_today = getMauiDate(time.gmtime())
    job_id, maui_date = getGeneratorState(cfg)
    #print job_id, maui_date

    while True:

        log_file = os.path.join(maui_spool_dir, STATS_DIR, maui_date)
        mlp = MauiLogParser(log_file)
        if job_id is not None:
            mlp.spoolToEntry(job_id)

        while True:

            try:
                log_entry = mlp.getNextLogEntry()
            except IOError, e:
                if maui_date == maui_date_today: # todays entry might not exist yet
                    #logging.info('Error opening log file for today')
                    break
                logging.error('Error opening log file at %s for date %s' % (log_file, maui_date))
                break

            if log_entry is None:
                break # no more log entries

            job_id = log_entry[0]
            if not shouldGenerateUR(log_entry, usermap):
                logging.debug('Job %s: No UR will be generated.' % job_id)
                continue

            ur = createUsageRecord(log_entry, hostname, usermap)
            log_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)
            ur_dir = os.path.join(log_dir, 'urs')
            if not os.path.exists(ur_dir):
                os.makedirs(ur_dir)

            ur_file = os.path.join(ur_dir, job_id)
            ur.writeXML(ur_file)
            writeGeneratorState(cfg, job_id, maui_date)
            logging.info('Wrote usage record to %s' % ur_file)

            job_id = None

        if maui_date == maui_date_today:
            break

        maui_date = getIncrementalMauiDate(maui_date)
        job_id = None

    #print job_id, maui_date
    #writeGeneratorState(cfg, job_id, maui_date)


