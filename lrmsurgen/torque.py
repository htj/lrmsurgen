#
# Torque log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Copyright: Nordic Data Grid Facility (2010)

import os
import time
import logging

from lrmsurgen import config, common, usagerecord



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
        line_tokens = line.split(' ')
        fields = {}

        start_fields = line_tokens[1].split(';')
        fields['entrytype'] = start_fields[1]
        fields['jobid'] = start_fields[2]
        fields['user'] = start_fields[3].split('=')[1]

        for e in line_tokens:
            e = e.strip()
            r = e.split('=')
            if len(r) >= 2:
                fields[r[0]] = '='.join(r[1:len(r)])

        return fields


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



def getSeconds(torque_timestamp):
    """
    Convert time string in the form HH:MM:SS to seconds
    """
    (hours, minutes, seconds) = torque_timestamp.split(':')
    return int(hours)*3600 + int(minutes)*60 + int(seconds)


def getCoreCount(nodes):
    """
    Find number of cores used by parsing the Resource_List.nodes value
    {<node_count> | <hostname>} [:ppn=<ppn>][:<property>[:<property>]...] [+ ...]
    http://www.clusterresources.com/torquedocs21/2.1jobsubmission.shtml#nodeExamples
    """
    cores = 0
    for node_req in nodes.split('+'):
        listTmp = node_req.split(':')
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


def createUsageRecord(log_entry, hostname, user_map, vo_map, missing_user_mappings):
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
    mapped_vo = vo_map.get(user_name)
    if mapped_vo is not None:
        voi = usagerecord.VOInformation(name=mapped_vo, type_='lrmsurgen-vomap')
        vo_info.append(voi)

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
    ur.vo_info         += vo_info

    return ur


def generateUsageRecords(cfg, hostname, user_map, vo_map):
    """
    Starts the UR generation process.
    """

    torque_spool_dir = config.getConfigValue(cfg, config.SECTION_TORQUE,
                                             config.TORQUE_SPOOL_DIR, config.DEFAULT_TORQUE_SPOOL_DIR)
    torque_accounting_dir = os.path.join(torque_spool_dir, 'server_priv', 'accounting')

    torque_date_today = time.strftime(TORQUE_DATE_FORMAT, time.gmtime())
    job_id, torque_date = common.getGeneratorState(cfg, TORQUE_DATE_FORMAT)

    missing_user_mappings = {}

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

            job_id = log_entry['jobid']

            ur = createUsageRecord(log_entry, hostname, user_map, vo_map, missing_user_mappings)
            log_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.LOGDIR, config.DEFAULT_LOG_DIR)
            ur_dir = os.path.join(log_dir, 'urs')
            if not os.path.exists(ur_dir):
                os.makedirs(ur_dir)

            ur_file = os.path.join(ur_dir, job_id)
            ur.writeXML(ur_file)
            common.writeGeneratorState(cfg, job_id, torque_date)
            logging.info('Wrote usage record to %s' % ur_file)

            job_id = None

        if torque_date == torque_date_today:
            break

        torque_date = common.getIncrementalDate(torque_date, TORQUE_DATE_FORMAT)
        job_id = None

    if missing_user_mappings:
        users = ','.join(missing_user_mappings)
        logging.info('Missing user mapping for the following users: %s' % users)

