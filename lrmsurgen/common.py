#
# Common functions for log parsing
#
# Module for the LRMS UR Generator module.
#
# Author: Andreas Engelbredt Dalsgaard <andreas.dalsgaard@gmail.com>
# Copyright: Nordic Data Grid Facility (2010)

import os
import time
import datetime

from lrmsurgen import config



def getIncrementalDate(date, DATE_FORMAT):
    """
    Returns the following day in date format given as argument, given a date
    in that date format.
    """
    gm_td = time.strptime(date, DATE_FORMAT)
    d = datetime.date(gm_td.tm_year, gm_td.tm_mon, gm_td.tm_mday)
    day = datetime.timedelta(days=1)
    d2 = d + day
    next_date = time.strftime(DATE_FORMAT, d2.timetuple())
    return next_date


def getStateFileLocation(cfg):
    """
    Returns the location of state file
    The state file contains the information of whereto the ur generation has been processed
    """
    state_dir = config.getConfigValue(cfg, config.SECTION_COMMON, config.STATEDIR, config.DEFAULT_STATEDIR)
    state_file = os.path.join(state_dir, config.getStateFile(cfg))
    return state_file


def getGeneratorState(cfg, DATE_FORMAT):
    """
    Get state of where to the UR generation has reached in the log.
    This is two string tuple containing the jobid and the log file.
    """
    state_file = getStateFileLocation(cfg)
    if not os.path.exists(state_file):
        # no statefile -> we start from a couple of days back
        t_old = time.time() - 500000
        return None, time.strftime(DATE_FORMAT, time.gmtime(t_old))

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


