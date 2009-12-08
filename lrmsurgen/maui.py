#
# Maui log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)


from lrmsurgen import usagerecord

try:
    from xml.etree import ElementTree as ET
except ImportError:
    # Python 2.4 compatability
    from elementtree import ElementTree as ET

DEFAULT_LOG_DIR = '/var/spool/maui'
STATS_DIR       = 'stats'



class MauiLogParser:

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
        return self.splitLineEntry(line)



def createUsageRecord(log_entry, hostname, usermap):

    ur = usagerecord.UsageRecord()

    job_id    = log_entry[0]
    user_name = log_entry[3]
    job_state = log_entry[6]

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

    ur.wall_duration = float(log_entry[29])

    return ur



def generateUsageRecords(cfg, hostname, usermap):

    log_file = 'samples/maui.entry'

    mlp = MauiLogParser(log_file)

    log_entry = mlp.getNextLogEntry()

    job_id    = log_entry[0]
    job_state = log_entry[6]
    if not job_state == 'Completed':
        print 'Skipping UR generation for job %s (state %s)' % (job_id, job_state)
        return

    ur = createUsageRecord(log_entry, hostname, usermap)

    ET.dump(ur.generateTree())

