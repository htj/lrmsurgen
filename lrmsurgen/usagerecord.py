#
# Usage Record representation and generation.
#
# Module for the LRMS UR Generator module.
# Some code copied from the arc-ur-logger in ARC.
#
# Author: Henrik Thostrup Jensen <htj@ndgf.org>
# Copyright: Nordic Data Grid Facility (2009)

import time
#import logging
#import datetime

try:
    from xml.etree import ElementTree as ET
except ImportError:
    # Python 2.4 compatability
    from elementtree import ElementTree as ET


# constant / defaults
ISO_TIME_FORMAT    = "%Y-%m-%dT%H:%M:%S"
XML_HEADER         = '''<?xml version="1.0" encoding="UTF-8" ?>''' + "\n"

# xml namespaces
OGF_UR_NAMESPACE  = "http://schema.ogf.org/urf/2003/09/urf"
DEISA_NAMESPACE   = "http://rmis.deisa.org/acct"
SGAS_VO_NAMESPACE = "http://www.sgas.se/namespaces/2009/05/ur/vo"
SGAS_AT_NAMESPACE = "http://www.sgas.se/namespaces/2009/07/ur"
LOGGER_NAMESPACE  = "http://www.sgas.se/namespaces/2010/08/logger"

# job usage element/attribute names
JOB_USAGE_RECORD     = ET.QName("{%s}JobUsageRecord" % OGF_UR_NAMESPACE)

RECORD_IDENTITY      = ET.QName("{%s}RecordIdentity" % OGF_UR_NAMESPACE)
RECORD_ID            = ET.QName("{%s}recordId"       % OGF_UR_NAMESPACE)
CREATE_TIME          = ET.QName("{%s}createTime"     % OGF_UR_NAMESPACE)
JOB_IDENTITY         = ET.QName("{%s}JobIdentity"    % OGF_UR_NAMESPACE)
GLOBAL_JOB_ID        = ET.QName("{%s}GlobalJobId"    % OGF_UR_NAMESPACE)
LOCAL_JOB_ID         = ET.QName("{%s}LocalJobId"     % OGF_UR_NAMESPACE)
USER_IDENTITY        = ET.QName("{%s}UserIdentity"   % OGF_UR_NAMESPACE)
GLOBAL_USER_NAME     = ET.QName("{%s}GlobalUserName" % OGF_UR_NAMESPACE)
LOCAL_USER_ID        = ET.QName("{%s}LocalUserId"    % OGF_UR_NAMESPACE)
JOB_NAME             = ET.QName("{%s}JobName"        % OGF_UR_NAMESPACE)
STATUS               = ET.QName("{%s}Status"         % OGF_UR_NAMESPACE)
CHARGE               = ET.QName("{%s}Charge"         % OGF_UR_NAMESPACE)
WALL_DURATION        = ET.QName("{%s}WallDuration"   % OGF_UR_NAMESPACE)
CPU_DURATION         = ET.QName("{%s}CpuDuration"    % OGF_UR_NAMESPACE)
NODE_COUNT           = ET.QName("{%s}NodeCount"      % OGF_UR_NAMESPACE)
PROCESSORS           = ET.QName("{%s}Processors"     % OGF_UR_NAMESPACE)
START_TIME           = ET.QName("{%s}StartTime"      % OGF_UR_NAMESPACE)
END_TIME             = ET.QName("{%s}EndTime"        % OGF_UR_NAMESPACE)
PROJECT_NAME         = ET.QName("{%s}ProjectName"    % OGF_UR_NAMESPACE)
SUBMIT_HOST          = ET.QName("{%s}SubmitHost"     % OGF_UR_NAMESPACE)
MACHINE_NAME         = ET.QName("{%s}MachineName"    % OGF_UR_NAMESPACE)
HOST                 = ET.QName("{%s}Host"           % OGF_UR_NAMESPACE)
QUEUE                = ET.QName("{%s}Queue"          % OGF_UR_NAMESPACE)

SUBMIT_TIME   = ET.QName("{%s}SubmitTime" % DEISA_NAMESPACE)

VO            = ET.QName("{%s}VO"         % SGAS_VO_NAMESPACE)
VO_TYPE       = ET.QName("{%s}type"       % SGAS_VO_NAMESPACE)
VO_NAME       = ET.QName("{%s}Name"       % SGAS_VO_NAMESPACE)
VO_ISSUER     = ET.QName("{%s}Issuer"     % SGAS_VO_NAMESPACE)
VO_ATTRIBUTE  = ET.QName("{%s}Attribute"  % SGAS_VO_NAMESPACE)
VO_GROUP      = ET.QName("{%s}Group"      % SGAS_VO_NAMESPACE)
VO_ROLE       = ET.QName("{%s}Role"       % SGAS_VO_NAMESPACE)
VO_CAPABILITY = ET.QName("{%s}Capability" % SGAS_VO_NAMESPACE)

SGAS_USER_TIME           = ET.QName("{%s}UserTime"           % SGAS_AT_NAMESPACE)
SGAS_KERNEL_TIME         = ET.QName("{%s}KernelTime"         % SGAS_AT_NAMESPACE)
SGAS_EXIT_CODE           = ET.QName("{%s}ExitCode"           % SGAS_AT_NAMESPACE)
SGAS_MAJOR_PAGE_FAULTS   = ET.QName("{%s}MajorPageFaults"    % SGAS_AT_NAMESPACE)
SGAS_RUNTIME_ENVIRONMENT = ET.QName("{%s}RuntimeEnvironment" % SGAS_AT_NAMESPACE)

# logger elements and attributes
LOGGER_NAME         = ET.QName("{%s}LoggerName"         % LOGGER_NAMESPACE)
LOGGER_VERSION      = ET.QName("{%s}version"            % LOGGER_NAMESPACE)

# values for the logger name + version
LOGGER_NAME_VALUE    = 'SGAS-BaRT'
LOGGER_VERSION_VALUE = '001'

# register namespaces in element tree so we get more readable xml files
# the semantics of the xml files does not change due to this
try:
    register_namespace = ET.register_namespace
except AttributeError:
    def register_namespace(prefix, uri):
        ET._namespace_map[uri] = prefix

register_namespace('ur', OGF_UR_NAMESPACE)
register_namespace('deisa', DEISA_NAMESPACE)
register_namespace('vo', SGAS_VO_NAMESPACE)
register_namespace('sgas', SGAS_AT_NAMESPACE)
register_namespace('logger', LOGGER_NAMESPACE)



class VOInformation:

    def __init__(self):
        self.type = None
        self.name = None
        self.issuer = None
        self.attributes = [] # [group, role, capability]



class UsageRecord:

    def __init__(self):
        self.record_id          = None
        self.global_job_id      = None
        self.local_job_id       = None
        self.global_user_name   = None
        self.local_user_id      = None
        self.job_name           = None
        self.status             = None
        self.machine_name       = None
        self.queue              = None
        self.host               = None
        self.node_count         = None
        self.processors         = None
        self.submit_time        = None
        self.end_time           = None
        self.start_time         = None
        self.project_name       = None
        self.submit_host        = None
        self.wall_duration      = None
        self.cpu_duration       = None
        self.charge             = None
        self.vo_info            = [] # list of VOInformation
        # sgas attributes
        self.user_time          = None
        self.kernel_time        = None
        self.exit_code          = None
        self.major_page_faults  = None
        self.runtime_environments = []
        # logger attributes
        self.logger_name        = LOGGER_NAME
        self.logger_version     = LOGGER_VERSION


    def generateTree(self):
        """
        Generates the XML tree for usage record.
        """

        # utility function, very handy
        def setElement(parent, name, text):
            element = ET.SubElement(parent, name)
            element.text = str(text)

        # begin method

        ur = ET.Element(JOB_USAGE_RECORD)

        assert self.record_id is not None, "No recordId specified, cannot generate usage record"
        record_identity = ET.SubElement(ur, RECORD_IDENTITY)
        record_identity.set(RECORD_ID, self.record_id)
        record_identity.set(CREATE_TIME, time.strftime(ISO_TIME_FORMAT, time.gmtime()) + 'Z')

        if self.global_job_id is not None or self.local_job_id is not None:
            job_identity = ET.SubElement(ur, JOB_IDENTITY)
            if self.global_job_id is not None:
                setElement(job_identity, GLOBAL_JOB_ID, self.global_job_id)
            if self.local_job_id is not None:
                setElement(job_identity, LOCAL_JOB_ID, self.local_job_id)

        if self.global_user_name is not None or self.local_job_id is not None:
            user_identity = ET.SubElement(ur, USER_IDENTITY)
            if self.local_user_id is not None:
                setElement(user_identity, LOCAL_USER_ID, self.local_user_id)
            if self.global_user_name is not None:
                setElement(user_identity, GLOBAL_USER_NAME, self.global_user_name)

            # vo stuff belongs under user identity
            for voi in self.vo_info:

                vo = ET.SubElement(user_identity, VO)
                if voi.type is not None:
                    vo.attrib[VO_TYPE] = voi.type
                setElement(vo, VO_NAME, voi.name)
                if voi.issuer is not None:
                    setElement(vo, VO_ISSUER, voi.issuer)

                for attrs in voi.attributes:
                    group, role, capability = attrs
                    attr = ET.SubElement(vo, VO_ATTRIBUTE)
                    setElement(attr, VO_GROUP, group)
                    if role is not None:
                        setElement(attr, VO_ROLE, role)
                    if capability is not None:
                        setElement(attr, VO_CAPABILITY, capability)

        if self.job_name       is not None :  setElement(ur, JOB_NAME, self.job_name)
        if self.charge         is not None :  setElement(ur, CHARGE, self.charge)
        if self.status         is not None :  setElement(ur, STATUS, self.status)
        if self.machine_name   is not None :  setElement(ur, MACHINE_NAME, self.machine_name)
        if self.queue          is not None :  setElement(ur, QUEUE, self.queue)
        if self.host           is not None :  setElement(ur, HOST, self.host)
        if self.node_count     is not None :  setElement(ur, NODE_COUNT, self.node_count)
        if self.processors     is not None :  setElement(ur, PROCESSORS, self.processors)
        if self.submit_host    is not None :  setElement(ur, SUBMIT_HOST, self.submit_host)
        if self.project_name   is not None :  setElement(ur, PROJECT_NAME, self.project_name)
        if self.submit_time    is not None :  setElement(ur, SUBMIT_TIME, self.submit_time)
        if self.start_time     is not None :  setElement(ur, START_TIME, self.start_time)
        if self.end_time       is not None :  setElement(ur, END_TIME, self.end_time)
        if self.wall_duration  is not None :  setElement(ur, WALL_DURATION, "PT%fS" % self.wall_duration)
        if self.cpu_duration   is not None :  setElement(ur, CPU_DURATION, "PT%fS" % self.cpu_duration)
        # sgas attributes
        if self.user_time      is not None :  setElement(ur, SGAS_USER_TIME, "PT%fS" % self.user_time)
        if self.kernel_time    is not None :  setElement(ur, SGAS_KERNEL_TIME, "PT%fS" % self.kernel_time)
        if self.exit_code      is not None :  setElement(ur, SGAS_EXIT_CODE, self.exit_code)
        if self.major_page_faults is not None :
            setElement(ur, SGAS_MAJOR_PAGE_FAULTS, self.major_page_faults)
        for renv in self.runtime_environments:
            setElement(ur, SGAS_RUNTIME_ENVIRONMENT, renv)

        # set logger name and version
        logger_name = ET.SubElement(ur, LOGGER_NAME)
        logger_name.text = LOGGER_NAME_VALUE
        logger_name.set(LOGGER_VERSION, LOGGER_VERSION_VALUE)

        return ET.ElementTree(ur)


    def writeXML(self, filename):
        tree = self.generateTree()
        f = file(filename, 'w')
        f.write(XML_HEADER)
        tree.write(f, encoding='utf-8')


# ----

def gm2isoTime(gm_time):
    return time.strftime(ISO_TIME_FORMAT, gm_time) + "Z"


def epoch2isoTime(epoch_time):
    gmt = time.gmtime(epoch_time)
    return gm2isoTime(gmt)

