import sys, time

JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

for j in JARS:
	if j not in sys.path:
		sys.path.append(j)

import datetime
from jipy import kernel



krn_proc = kernel.IPythonKernelProcess()

while krn_proc.connection is None:
	time.sleep(0.1)

krn = krn_proc.connection
krn.execute_request('import time, sys\n', listener=kernel.DebugKernelRequestListener('import'))
print '[import] Importing time'
krn.poll(-1)

krn.execute_request('time.sleep(1.0)\n', listener=kernel.DebugKernelRequestListener('sleep'))
print '[sleep] Sleeping...'
krn.poll(-1)

krn.execute_request('print "Hello world"\n', listener=kernel.DebugKernelRequestListener('say_hi'))
print '[say_hi] Printing something'
krn.poll(-1)

krn.execute_request('3.141\n', listener=kernel.DebugKernelRequestListener('pi'))
print '[pi] Getting the value of pi'
krn.poll(-1)

krn.execute_request('raise ValueError\n', listener=kernel.DebugKernelRequestListener('value_err'))
print '[value_err] Raising an exception'
krn.poll(-1)

N_POLLS = 1024
t1 = datetime.datetime.now()
for i in xrange(N_POLLS):
	krn.poll(0)
t2 = datetime.datetime.now()
print 'Polling {0} times took {1}'.format(N_POLLS, t2 - t1)

krn_proc.close()