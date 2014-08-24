import sys

JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

for j in JARS:
    if j not in sys.path:
        sys.path.append(j)



from jipy import kernel

kernel_name = sys.argv[1]

class TestKernelConnection (kernel.KernelConnection):
    def on_execute_reply_ok(self, execution_count, payload, user_expressions):
        print 'execute_reply OK: {0} payload={1} user_expressions={2}'.format(execution_count, payload, user_expressions)

    def on_execute_reply_error(self, execution_count, ename, evalue, traceback):
        print 'execute_reply ERROR: {0} ename={1} evalue={2} traceback={3}'.format(execution_count,
                                                                                   ename, evalue, traceback)

    def on_execute_reply_abort(self, execution_count):
        print 'execute_reply ABORT: {0}'.format(execution_count)

    def on_stream(self, stream_name, data):
        print '{0}: {1}'.format(stream_name, data)

    def on_status(self, execution_state):
        print 'status: {0}'.format(execution_state)

    def on_execute_input(self, execution_count, code):
        print 'execute_input: {0} code={1}'.format(execution_count, code)



kernel = TestKernelConnection(kernel_name)



print 'Importing time'
kernel.execute_request('import time, sys\n')
kernel.poll(-1)
print 'Sleeping...'
kernel.execute_request('time.sleep(1.0)\n')
kernel.poll(-1)
print 'Printing something'
kernel.execute_request('print "Hello world"\n')
kernel.poll(-1)
print 'Done'



kernel.close()