#
# Torque log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Copyright: Nordic Data Grid Facility (2010)

import os
import time
import datetime
import logging

from lrmsurgen import config, usagerecord


TORQUE_DATE_FORMAT = '%Y%m%d'
STATE_FILE       = 'torque.state'

class TorqueLogParser:
    """
    Parser for torque accounting log.
    """
    def __init__(self, log_file):
        self.log_file = log_file
        self.file_ = None


    def openFile(self):
        self.file_ = open(self.log_file)


    def splitLineEntry(self, line):
        fields = line.split(' ')
        fieldsTmp = {}
        startFields = fields[1].split(';')
        fieldsTmp['entrytype'] = startFields[1]
        fieldsTmp['jobid'] = startFields[2]
        fieldsTmp['user'] = startFields[3].split('=')[1]
        for e in fields:
            e = e.strip()
            r = e.split('=')
            if len(r) >= 2:
                fieldsTmp[r[0]] = '='.join(r[1:len(r)])

        return fieldsTmp


    def getNextLogLine(self):
        if self.file_ is None:
            self.openFile()

        while True:
            line = self.file_.readline()
            if line == '': #last line
                return None
            if line[20] == 'E':
                return line


    def getNextLogEntry(self):
        line = self.getNextLogLine()
        if line is None:
            return None
        return self.splitLineEntry(line)


    def spoolToEntry(self, entry_id):
        while True:
            log_entry = self.getNextLogEntry()
            if log_entry is None or log_entry['jobid'] == entry_id:
                break

def getSeconds(time_str):
    """
    Convert time string in the form HH:MM:SS to seconds
    """
    time_arr = time_str.split(':')
    return (int(time_arr[0])*60+int(time_arr[1]))*60+int(time_arr[2])

def getCoreCount(nodeList):
    """
    Find number of cores used by parsing the Resource_List.nodes value
 	{<node_count> | <hostname>} [:ppn=<ppn>][:<property>[:<property>]...] [+ ...]
    http://www.clusterresources.com/torquedocs21/2.1jobsubmission.shtml#nodeExamples
    """
    cores = 0
    for nodeReq in nodeList.split('+'):
        listTmp = nodeReq.split(':')
        if listTmp[0].isdigit():
            first = int(listTmp[0])
        else:
            first = 1

        cores += first
        if len(listTmp) > 1:

            for e in listTmp:
                if len(e) > 3:
                    if e[0:3] == 'ppn':
                        cores -= first
                        cores += first*int(e.split('=')[1])
                        break
    return cores


def createUsageRecord(log_entry, hostname, user_map, project_map, missing_user_mappings):
    """
    Creates a Usage Record object given a Torque log entry.
    """

    # extract data from the workload trace (log_entry)
    job_id       = log_entry['jobid']
    user_name    = log_entry['user']
    queue        = log_entry['queue']
    submit_time  = int(log_entry['ctime'])
    start_time   = int(log_entry['start'])
    end_time     = int(log_entry['end'])
    account_name = log_entry['group']
    utilized_cpu = getSeconds(log_entry['resources_used.cput'])
    wall_time    = getSeconds(log_entry['resources_used.walltime'])
    core_count   = getCoreCount(log_entry['Resource_List.nodes'])
    hosts        = list(set([hc.split('/')[0] for hc in log_entry['exec_host'].split('+')]))

    # clean data and create various composite entries from the work load trace
    if job_id.isdigit() and hostname is not None:
        job_identifier = job_id + '.' + hostname
    else:
        job_identifier = job_id
    fqdn_job_id = hostname + ':' + job_identifier

    if not user_name in user_map:
        missing_user_mappings[user_name] = True

    vo_info = []
    if account_name is not None:
        mapped_project = project_map.get(account_name)
        if mapped_project is not None:
            voi = usagerecord.VOInformation()
            voi.type = 'lrmsurgen-projectmap'
            voi.name = mapped_project

    ## fill in usage record fields
    ur = usagerecord.UsageRecord()
    ur.record_id        = fqdn_job_id
    ur.local_job_id     = job_identifier
    ur.global_job_id    = fqdn_job_id
    ur.local_user_id    = user_name
    ur.global_user_name = user_map.get(user_name)
    ur.machine_name     = hostname
    ur.queue            = queue
    ur.processors       = core_count
    ur.node_count       = len(hosts)
    ur.host             = ','.join(hosts)
    ur.submit_time      = usagerecord.epoch2isoTime(submit_time)
    ur.start_time       = usagerecord.epoch2isoTime(start_time)
    ur.end_time         = usagerecord.epoch2isoTime(end_time)
    ur.cpu_duration     = utilized_cpu
    ur.wall_duration    = wall_time
    ur.project_name     = account_name
    ur.vo_info         += vo_info

    return ur

def getDate(gmtime, format_str):
    """
    Returns a date given a time.gmtime object and a format string.
    """
    return time.strftime(format_str, gmtime)


def getIncrementalDate(date, format_str):
    """
    Returns the following day in date format given as argument, given a date
    in that date format.
    """
    gm_td = time.strptime(date, format_str)
    d = datetime.date(gm_td.tm_year, gm_td.tm_mon, gm_td.tm_mday)
    day = datetime.timedelta(days=1)
    d2 = d + day
    next_date = time.strftime(format_str, d2.timetuple())
    return next_date



def getStateFileLocation(cfg):
    """
    Returns the location of state file
    The state file contains the information of whereto the ur generation has been processed
    """
    state_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.STATEDIR, config.DEFAULT_STATEDIR)
    state_file = os.path.join(state_dir, STATE_FILE)
    return state_file



def getGeneratorState(cfg, format_str):
    """
    Get state of where to the UR generation has reached in the log.
    This is two string tuple containing the jobid and the log file.
    """
    state_file = getStateFileLocation(cfg)
    if not os.path.exists(state_file):
        # no statefile -> we start from a couple of days back
        t_old = time.time() - 500000
        return None, getDate(time.gmtime(t_old), format_str)

    state_data = open(state_file).readline().strip() # state is only on the first line
    job_id, date = state_data.split(' ', 2)
    if job_id == '-':
        job_id = None
    return job_id, date


def writeGeneratorState(cfg, job_id, log_file):
    """
    Write the state of where the logs have been parsed to.
    This is a job id and date (log file and entry).
    """
    state_file = getStateFileLocation(cfg)
    state_data = '%s %s' % (job_id or '-', log_file)

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

    torque_accounting_dir = config.getConfigValue(cfg, config.SECTION_TORQUE,
        config.TORQUE_ACCOUNTING_DIR, config.DEFAULT_TORQUE_ACCOUNTING_DIR)
    torque_date_today = getDate(time.gmtime(), TORQUE_DATE_FORMAT)
    job_id, torque_date = getGeneratorState(cfg, TORQUE_DATE_FORMAT)

    missing_user_mappings = {}
    print "job id og date", job_id, torque_date

    while True:

        log_file = os.path.join(torque_accounting_dir, torque_date)
        tlp = TorqueLogParser(log_file)
        if job_id is not None:
            tlp.spoolToEntry(job_id)

        while True:

            try:
                log_entry = tlp.getNextLogEntry()
            except IOError:
                if torque_date == torque_date_today: # todays entry might not exist yet
                    #logging.info('Error opening log file for today')
                    break
                logging.error('Error opening log file at %s for date %s' % (log_file, torque_date))
                break

            if log_entry is None:
                break # no more log entries

            #if len(log_entry) != 44:
            #    logging.error('Read entry with an invalid number fields:')
            #    logging.error(' - File %s contains entry with %i fields. First field: %s' % (log_file, len(log_entry), log_entry[0]))
            #    logging.error(' - No usage record will be generated from this line')
            #    continue

            job_id = log_entry['jobid']
            #if not shouldGenerateUR(log_entry, user_map):
            #    logging.debug('Job %s: No UR will be generated.' % job_id)
            #    continue

            ur = createUsageRecord(log_entry, hostname, user_map, project_map, missing_user_mappings)
            log_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)
            ur_dir = os.path.join(log_dir, 'urs')
            if not os.path.exists(ur_dir):
                os.makedirs(ur_dir)

            ur_file = os.path.join(ur_dir, job_id)
            ur.writeXML(ur_file)
            writeGeneratorState(cfg, job_id, torque_date)
            logging.info('Wrote usage record to %s' % ur_file)

            job_id = None

        if torque_date == torque_date_today:
            break

        torque_date = getIncrementalDate(torque_date, TORQUE_DATE_FORMAT)
        job_id = None

    if missing_user_mappings:
        users = ','.join(missing_user_mappings)
        logging.info('Missing user mapping for the following users: %s' % users)

