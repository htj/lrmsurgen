#
# Configration and parsing module.
#
# Module for the LRMS UR Generator module.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import re
import ConfigParser
from optparse import OptionParser

DEFAULT_CONFIG_FILE     = '/etc/lrmsurgen/lrmsurgen.conf'
DEFAULT_USERMAP_FILE    = '/etc/lrmsurgen/usermap'
DEFAULT_PROJECTMAP_FILE = '/etc/lrmsurgen/projectmap'
DEFAULT_LOG_FILE        = '/var/log/lrmsurgen.log'
DEFAULT_LOG_DIR         = '/var/spool/lrmsurgen/usagerecords'
DEFAULT_STATEDIR        = '/var/spool/lrmsurgen'
DEFAULT_MAUI_SPOOL_DIR  = '/var/spool/maui'

SECTION_COMMON = 'common'
SECTION_MAUI   = 'maui'

HOSTNAME   = 'hostname'
USERMAP    = 'usermap'
PROJECTMAP = 'projectmap'
LOGDIR     = 'logdir'
LOGFILE    = 'logfile'
STATEDIR   = 'statedir'

MAUI_SPOOL_DIR  = 'spooldir'


# regular expression for matching usermap/projectmap lines
rx = re.compile('''\s*(.*)\s*"(.*)"''')



def getParser():

    parser = OptionParser()
    parser.add_option('-l', '--log-file', dest='logfile', help='Log file (overwrites config option).')
    parser.add_option('-c', '--config', dest='config', help='Configuration file.',
                      default=DEFAULT_CONFIG_FILE, metavar='FILE')
    return parser


def getConfig(config_file):

    cfg = ConfigParser.ConfigParser()
    cfg.read(config_file)
    return cfg


def getConfigValue(cfg, section, value, default=None):

    try:
        return cfg.get(section, value)
    except ConfigParser.NoSectionError:
        return default
    except ConfigParser.NoOptionError:
        return default


def readFileMap(map_file):

    map_ = {}

    for line in open(map_file).readlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = rx.match(line)
        if not m:
            continue
        key, mapped_value = m.groups()
        key = key.strip()
        mapped_value = mapped_value.strip()
        if mapped_value == '-':
            mapped_value = None
        map_[key] = mapped_value

    return map_


def getUserMap(user_map_file):

    user_map = readFileMap(user_map_file)
    return user_map


def getProjectMap(project_map_file):

    project_map = readFileMap(project_map_file)
    return project_map

