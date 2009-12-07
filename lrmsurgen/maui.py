#
# Maui log parsing -> UR module
#
# Module for the LRMS UR Generator module.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)


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
            if line.startwith('VERSION'):
                continue # all maui log files starts with a version, typically 230
            if line.startswith('#'):
                continue # maui somtimes creates explanatory lines in the log file
            if line == '': # last line
                return None

            return line


    def getNextLogEntry(self):
        line = self.getNextLogLine()
        return splitLineEntry(line)



