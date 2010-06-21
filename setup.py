from distutils.core import setup
from distutils.command.install_data import install_data

import os
import time



class InstallLRMSURGenerator(install_data):
    # this class is used to filter out data files which should not be overwritten
    # currently this is sgas.conf and sgas.authz

    def finalize_options(self):
        install_data.finalize_options(self)

        ETC_LRMSURGEN = '/etc/lrmsurgen'
        if self.root is not None:
            ETC_LRMSURGEN = os.path.join(self.root, ETC_LRMSURGEN[1:])

        if not os.path.exists(ETC_LRMSURGEN):
            os.makedirs(ETC_LRMSURGEN)

        lrmsurgen_conf = os.path.join(ETC_LRMSURGEN, 'lrmsurgen.conf')
        lrmsurgen_umap = os.path.join(ETC_LRMSURGEN, 'usermap')
        lrmsurgen_pmap = os.path.join(ETC_LRMSURGEN, 'projectmap')

        if os.path.exists(lrmsurgen_conf):
            print "Skipping installation of lrmsurgen.conf (already exists)"
            self.data_files.remove( ('/etc/lrmsurgen', ['datafiles/etc/lrmsurgen.conf']) )

        if os.path.exists(lrmsurgen_umap):
            print "Skipping installation of usermap (already exists)"
            self.data_files.remove( ('/etc/lrmsurgen', ['datafiles/etc/usermap']) )

        if os.path.exists(lrmsurgen_pmap):
            print "Skipping installation of projectmap (already exists)"
            self.data_files.remove( ('/etc/lrmsurgen', ['datafiles/etc/projectmap']) )



cmdclasses = {'install_data': InstallLRMSURGenerator} 

gmt = time.gmtime()
version = 'git-%04d%02d%02d' % (gmt.tm_year, gmt.tm_mon, gmt.tm_mday)

setup(name='lrmsurgen',
      version=version,
      description='LRMS UR Generator',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
      url='http://www.ndgf.org/',
      packages=['lrmsurgen'],
      scripts = ['lrms-ur-generator', 'lrms-ur-registrant'],
      cmdclass = cmdclasses,

      data_files = [
        ('/etc/lrmsurgen', ['datafiles/etc/lrmsurgen.conf']),
        ('/etc/lrmsurgen', ['datafiles/etc/usermap']),
        ('/etc/lrmsurgen', ['datafiles/etc/projectmap'])
      ]

)

