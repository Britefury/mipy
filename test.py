import sys

JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

for j in JARS:
    if j not in sys.path:
        sys.path.append(j)



from jipy import kernel

kernel_name = sys.argv[1]

class TestKernelConnection (kernel.KernelConnection):
    def _handle_msg_shell_execute_reply(self, ident, msg):
        print 'execute_reply: {0}: {1}'.format(ident, msg['content'])

    def _handle_msg_iopub_stream(self, ident, msg):
        print 'stream: {0}: {1}'.format(ident, msg['content'])

    # def _handle_msg_iopub_status(self, ident, msg):
    #     print 'status: {0}: {1}'.format(ident, msg['content'])
    #
    # def _handle_msg_iopub_pyin(self, ident, msg):
    #     print 'pyin: {0}: {1}'.format(ident, msg['content'])



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