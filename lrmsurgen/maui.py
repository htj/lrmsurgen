#
# Maui log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import os
import time
import logging

from lrmsurgen import config, common, usagerecord



MAUI_DATE_FORMAT = '%a_%b_%d_%Y'
DEFAULT_LOG_DIR  = '/var/spool/maui'
STATS_DIR        = 'stats'
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



def createUsageRecord(log_entry, hostname, user_map, vo_map, maui_server_host, missing_user_mappings):
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
    core_count   = int(log_entry[31])*alo_tasks
    hosts        = log_entry[37].split(':')

    # clean data and create various composite entries from the work load trace

    if job_id.isdigit() and maui_server_host is not None:
        job_identifier = job_id + '.' + maui_server_host
    else:
        job_identifier = job_id
    fqdn_job_id = hostname + ':' + job_identifier

    if not user_name in user_map:
        missing_user_mappings[user_name] = True

    queue = req_class.replace('[','').replace(']','')
    if ':' in queue:
        queue = queue.split(':')[0]

    if account_name == '[NONE]':
        account_name = None

    vo_info = []
    if account_name is not None:
        mapped_vo = vo_map.get(account_name)
    else:
        mapped_vo = vo_map.get(user_name)
    if mapped_vo is not None:
        voi = usagerecord.VOInformation(name=mapped_vo, type_='lrmsurgen-vomap')
        vo_info = [voi]

    wall_time = end_time - start_time

    # okay, this is somewhat ridiculous and complicated:
    # When compiled on linux, maui will think that it will only get cputime reading
    # from the master node. To compensate for this it multiples the utilized cpu field
    # with the number of tasks. However on most newer torque installations the correct
    # cpu utilization is reported. When combined this creates abnormally high cpu time
    # values for parallel jobs. The following heuristic tries to compensate for this,
    # by checking if the cpu time is higher than wall_time * cpus (which it never should
    # be), and then correct the number. However this will not work for jobs with very
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

    ur.processors = core_count
    ur.node_count = len(hosts)
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



def generateUsageRecords(cfg, hostname, user_map, vo_map):
    """
    Starts the UR generation process.
    """

    maui_spool_dir = config.getConfigValue(cfg, config.SECTION_MAUI, config.MAUI_SPOOL_DIR,
                                           config.DEFAULT_MAUI_SPOOL_DIR)
    maui_server_host = getMauiServer(maui_spool_dir)
    maui_date_today = time.strftime(MAUI_DATE_FORMAT, time.gmtime())
    job_id, maui_date = common.getGeneratorState(cfg, MAUI_DATE_FORMAT)

    missing_user_mappings = {}

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

            ur = createUsageRecord(log_entry, hostname, user_map, vo_map, maui_server_host, missing_user_mappings)
            log_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)
            ur_dir = os.path.join(log_dir, 'urs')
            if not os.path.exists(ur_dir):
                os.makedirs(ur_dir)

            ur_file = os.path.join(ur_dir, job_id)
            ur.writeXML(ur_file)
            common.writeGeneratorState(cfg, job_id, maui_date)
            logging.info('Wrote usage record to %s' % ur_file)

            job_id = None

        if maui_date == maui_date_today:
            break

        maui_date = common.getIncrementalDate(maui_date, MAUI_DATE_FORMAT)
        job_id = None

    if missing_user_mappings:
        users = ','.join(missing_user_mappings)
        logging.info('Missing user mapping for the following users: %s' % users)

