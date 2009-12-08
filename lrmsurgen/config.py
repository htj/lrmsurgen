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

DEFAULT_CONFIG_FILE='/etc/lrmsurgen.conf'
DEFAULT_USERMAP_FILE='/etc/lrmsurgen.usermap'

# redularr expression for matching authz lines
rx = re.compile('''\s*(.*)\s*"(.*)"''')



def getParser():

    parser = OptionParser()
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
    except ConfigParser.NoOptionError:
        return default


def getUserMap(usermap_file):

    usermap = {}

    for line in open(usermap_file).readlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = rx.match(line)
        if not m:
            continue
        local_user, user_dn = m.groups()
        local_user = local_user.strip()
        user_dn    = user_dn.strip()
        usermap[local_user] = user_dn

    return usermap

