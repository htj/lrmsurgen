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



MAUI_DATE_FORMAT = '%a_%b_%d_%Y'
DEFAULT_LOG_DIR  = '/var/spool/maui'
STATS_DIR        = 'stats'
STATE_FILE       = 'maui.state'
MAUI_CFG_FILE    = 'maui.cfg'



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


def getMauiServer(maui_spool_dir):

    SERVERHOST = 'SERVERHOST'

    maui_cfg_path = os.path.join(maui_spool_dir, MAUI_CFG_FILE)

    if os.path.exists(maui_cfg_path):
        for line in file(maui_cfg_path):
            line = line.strip()
            if line.startswith(SERVERHOST):
                entry = line.replace(SERVERHOST,'').strip()
                return entry

    logging.warning('Could not get Maui server host setting')
    return None



def createUsageRecord(log_entry, hostname, user_map, project_map, maui_server_host):
    """
    Creates a Usage Record object given a Maui log entry.
    """

    # extract data from the workload trace (log_entry)

    job_id       = log_entry[0]
    user_name    = log_entry[3]
    req_class    = log_entry[7]
    submit_time  = int(log_entry[8])
    start_time   = int(log_entry[10])
    end_time     = int(log_entry[11])
    alo_tasks    = int(log_entry[21])
    account_name = log_entry[25]
    utilized_cpu = float(log_entry[29])
    hosts        = log_entry[37].split(':')

    # clean data and create various composite entries from the work load trace

    if job_id.isdigit() and maui_server_host is not None:
        job_identifier = job_id + '.' + maui_server_host
    else:
        job_identifier = job_id
    fqdn_job_id = hostname + ':' + job_identifier

    if not user_name in user_map:
        logging.warning('Job %s: No mapping for username %s in user map.' % (job_id, user_name))

    queue = req_class.replace('[','').replace(']','')
    if ':' in queue:
        queue = queue.split(':')[0]

    if account_name == '[NONE]':
        account_name = None

    vo_info = []
    if account_name is not None:
        mapped_project = project_map.get(account_name)
        if mapped_project is not None:
            voi = usagerecord.VOInformation()
            voi.type = 'lrmsurgen-projectmap'
            voi.name = mapped_project

    wall_time = end_time - start_time

    # okay, this is somewhat ridiculous and complicated:
    # When compiled on linux, maui will think that it will only get cputime reading
    # from the master node. To compensate for this it multiples the utilized cpu field
    # with the number of tasks. However on most newer torque installations the correct
    # cpu utilization is reported. When combined this creates abnormally high cpu time
    # values for parallel jobs. The following heuristic tries to compensate for this,
    # by checking if the cpu time is higher than wall_time * cpus (which is never should)
    # be, and then correct the number. However this will not work for jobs with very
    # low efficiancy

    if utilized_cpu > wall_time * alo_tasks:
        utilized_cpu /= alo_tasks

    ## fill in usage record fields

    ur = usagerecord.UsageRecord()

    ur.record_id = fqdn_job_id

    ur.local_job_id = job_identifier
    ur.global_job_id = fqdn_job_id

    ur.local_user_id = user_name
    ur.global_user_name = user_map.get(user_name)

    ur.machine_name = hostname
    ur.queue = queue

    ur.node_count = alo_tasks
    ur.host = ','.join(hosts)

    ur.submit_time = usagerecord.epoch2isoTime(submit_time)
    ur.start_time  = usagerecord.epoch2isoTime(start_time)
    ur.end_time    = usagerecord.epoch2isoTime(end_time)

    ur.cpu_duration = utilized_cpu
    ur.wall_duration = wall_time

    ur.project_name = account_name
    ur.vo_info += vo_info

    return ur



def shouldGenerateUR(log_entry, user_map):
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
    if user_name in user_map and user_map[user_name] is None:
        logging.info('Job %s: User configured to skip UR generation' % job_id)
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



def generateUsageRecords(cfg, hostname, user_map, project_map):
    """
    Starts the UR generation process.
    """

    maui_spool_dir = config.getConfigValue(cfg, config.SECTION_MAUI, config.MAUI_SPOOL_DIR,
                                           config.DEFAULT_MAUI_SPOOL_DIR)
    maui_server_host = getMauiServer(maui_spool_dir)
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
            except IOError:
                if maui_date == maui_date_today: # todays entry might not exist yet
                    #logging.info('Error opening log file for today')
                    break
                logging.error('Error opening log file at %s for date %s' % (log_file, maui_date))
                break

            if log_entry is None:
                break # no more log entries

            if len(log_entry) != 44:
                logging.error('Read entry with an invalid number fields:')
                logging.error(' - File %s contains entry with %i fields. First field: %s' % (log_file, len(log_entry), log_entry[0]))
                logging.error(' - No usage record will be generated from this line')
                continue

            job_id = log_entry[0]
            if not shouldGenerateUR(log_entry, user_map):
                logging.debug('Job %s: No UR will be generated.' % job_id)
                continue

            ur = createUsageRecord(log_entry, hostname, user_map, project_map, maui_server_host)
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


