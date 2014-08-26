import sys

JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

for j in JARS:
	if j not in sys.path:
		sys.path.append(j)

import datetime
from jipy import kernel

kernel_name = sys.argv[1]


def on_execute_reply_ok(execution_count, payload, user_expressions):
	print 'execute_reply OK: {0} payload={1} user_expressions={2}'.format(execution_count, payload,
									      user_expressions)


def on_execute_reply_error(ename, evalue, traceback):
	print 'execute_reply ERROR: ename={0} evalue={1} traceback={2}'.format(ename, evalue, traceback)


def on_execute_reply_abort():
	print 'execute_reply ABORT'.format()


def on_stream(stream_name, data):
	print '{0}: {1}'.format(stream_name, data)


def on_status(busy):
	print 'status: busy={0}'.format(busy)


def on_execute_input(execution_count, code):
	print 'execute_input: {0} code={1}'.format(execution_count, code)


kernel = kernel.KernelConnection(kernel_name)
kernel.on_stream = on_stream
kernel.on_status = on_status
kernel.on_execute_input = on_execute_input

print 'Importing time'
kernel.execute_request('import time, sys\n',
		       on_ok=on_execute_reply_ok, on_error=on_execute_reply_error, on_abort=on_execute_reply_abort)
kernel.poll(-1)
print 'Sleeping...'
kernel.execute_request('time.sleep(1.0)\n',
		       on_ok=on_execute_reply_ok, on_error=on_execute_reply_error, on_abort=on_execute_reply_abort)
kernel.poll(-1)
print 'Printing something'
kernel.execute_request('print "Hello world"\n',
		       on_ok=on_execute_reply_ok, on_error=on_execute_reply_error, on_abort=on_execute_reply_abort)
kernel.poll(-1)
print 'Raising an exception'
kernel.execute_request('raise ValueError\n',
		       on_ok=on_execute_reply_ok, on_error=on_execute_reply_error, on_abort=on_execute_reply_abort)
kernel.poll(-1)

N_POLLS = 1024
t1 = datetime.datetime.now()
for i in xrange(N_POLLS):
	kernel.poll(0)
t2 = datetime.datetime.now()
print 'Polling {0} times took {1}'.format(N_POLLS, t2 - t1)

kernel.close()