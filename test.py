import sys

JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

for j in JARS:
	if j not in sys.path:
		sys.path.append(j)

import datetime
from jipy import kernel

kernel_name = sys.argv[1]



class TestListener (kernel.KernelRequestListener):
	def __init__(self, name):
		super(TestListener, self).__init__()
		self.name = name


	def on_stream(self, stream_name, data):
		print '[{0}] {1}: {2}'.format(self.name, stream_name, data)

	def on_display_data(self, source, data, metadata):
		pass

	def on_status(self, busy):
		print '[{0}] status: busy={1}'.format(self.name, busy)

	def on_execute_input(self, execution_count, code):
		print '[{0}] execute_input: {1} code={2}'.format(self.name, execution_count, code)

	def on_input_request(self, prompt, password, reply_callback):
		'''
	    	'input_request' message on STDIN socket

		:param prompt: the prompt to the user
		:param password: if True, do not echo text back to the user
		:param reply_callback: function of the form f(value) that your code should invoke when input is available to send
		:return:
		'''
		pass


	def on_execute_ok(self, execution_count, payload, user_expressions):
		print '[{0}] execute_reply OK: {1} payload={2} user_expressions={3}'.format(self.name, execution_count, payload,
										      user_expressions)

	def on_execute_error(self, ename, evalue, traceback):
		print '[{0}] execute_reply ERROR: ename={1} evalue={2} traceback={3}'.format(self.name, ename, evalue, traceback)

	def on_execute_abort(self):
		print '[{0}] execute_reply ABORT'.format(self.name)


	def on_inspect_ok(self, data, metadata):
		pass

	def on_inspect_error(self, ename, evalue, traceback):
		pass


	def on_complete_ok(self, matches, cursor_start, cursor_end, metadata):
		pass

	def on_complete_error(self, ename, evalue, traceback):
		pass



kernel = kernel.KernelConnection(kernel_name)
msg_id = kernel.execute_request('import time, sys\n', listener=TestListener('1'))
print 'Importing time {0}'.format(1)
kernel.poll(-1)

msg_id = kernel.execute_request('time.sleep(1.0)\n', listener=TestListener('2'))
print 'Sleeping... {0}'.format(2)
kernel.poll(-1)

msg_id = kernel.execute_request('print "Hello world"\n', listener=TestListener('3'))
print 'Printing something {0}'.format(3)
kernel.poll(-1)

msg_id = kernel.execute_request('raise ValueError\n', listener=TestListener('4'))
print 'Raising an exception {0}'.format(4)
kernel.poll(-1)

N_POLLS = 1024
t1 = datetime.datetime.now()
for i in xrange(N_POLLS):
	kernel.poll(0)
t2 = datetime.datetime.now()
print 'Polling {0} times took {1}'.format(N_POLLS, t2 - t1)

kernel.close()