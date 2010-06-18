from distutils.core import setup

import time

gmt = time.gmtime()
version = 'git-%i%i%i' % (gmt.tm_year, gmt.tm_mon, gmt.tm_mday)

setup(name='lrmsurgen',
      version=version,
      description='LRMS UR Generator',
      author='Henrik Thostrup Jensen',
      author_email='htj@ndgf.org',
      url='http://www.ndgf.org/',
      packages=['lrmsurgen'],
      package_data={'examples': ['examples/*']},
      scripts = ['lrms-ur-generator', 'lrms-ur-registrant']
)

