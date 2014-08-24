import os, sys, json, hmac, uuid, datetime, hashlib

from org.zeromq import ZMQ
from org.python.core.util import StringUtil



def load_connection_file(kernel_name):
    p = os.path.expanduser(os.path.join('~', '.ipython', 'profile_default', 'security',
                                        'kernel-{0}.json'.format(kernel_name)))

    if os.path.exists(p):
        with open(p, 'r') as f:
            return json.load(f)
    else:
        raise ValueError, 'Could not find connection file for kernel {0}'.format(kernel_name)

DELIM = StringUtil.toBytes("<IDS|MSG>")
KERNEL_PROTOCOL_VERSION = b'5.0'


def _unpack_ident(ident):
    return [StringUtil.fromBytes(x)   for x in ident]


class _MessageHandler (object):
    def __init__(self, instance, stream_name):
        self.__handlers = {}
        self.__instance = instance
        self.__stream_name = stream_name


    def method(self, msg_type):
        def decorated(unbound_method_fn):
            self.__handlers[msg_type] = unbound_method_fn
            return unbound_method_fn
        return decorated


    def handle(self, ident, msg):
        msg_type = msg['msg_type']
        try:
            bound_method = self.__handlers[msg_type]
        except KeyError:
            method_name = '_handle_msg_{0}_{1}'.format(self.__stream_name, msg_type)
            try:
                bound_method = getattr(self.__instance, method_name)
            except AttributeError:
                bound_method = None
            self.__handlers[msg_type] = bound_method

        if bound_method is not None:
            return bound_method(ident, msg)
        else:
            print '---   {0}: did not handle message {1}:{2}'.format(self.__stream_name, ident, msg_type)



class KernelConnection (object):
    def __init__(self, kernel_name, username=''):
        # Load the connection file and find out where we have to connect to
        connection = load_connection_file(kernel_name)

        key = connection['key'].encode('utf8')
        transport = connection['transport']
        address = connection['ip']

        shell_port = connection['shell_port']
        iopub_port = connection['iopub_port']
        stdin_port = connection['stdin_port']
        control_port = connection['control_port']

        # JeroMQ context
        self.__ctx = ZMQ.context(1)

        # Create the four IPython sockets; SHELL, IOPUB, STDIN and CONTROL
        self.shell = self.__ctx.socket(ZMQ.DEALER)
        self.iopub = self.__ctx.socket(ZMQ.SUB)
        self.stdin = self.__ctx.socket(ZMQ.DEALER)
        self.control = self.__ctx.socket(ZMQ.DEALER)
        # Connect
        self.shell.connect('{0}://{1}:{2}'.format(transport, address, shell_port))
        self.iopub.connect('{0}://{1}:{2}'.format(transport, address, iopub_port))
        self.stdin.connect('{0}://{1}:{2}'.format(transport, address, stdin_port))
        self.control.connect('{0}://{1}:{2}'.format(transport, address, control_port))
        # Subscribe IOPUB to everything
        self.iopub.subscribe(StringUtil.toBytes(''))

        # Create a poller to monitor the four sockets for incoming messages
        self.__poller = ZMQ.Poller(4)
        self.__shell_poll_index = self.__poller.register(self.shell, ZMQ.Poller.POLLIN)
        self.__iopub_poll_index = self.__poller.register(self.iopub, ZMQ.Poller.POLLIN)
        self.__stdin_poll_index = self.__poller.register(self.stdin, ZMQ.Poller.POLLIN)
        self.__control_poll_index = self.__poller.register(self.control, ZMQ.Poller.POLLIN)

        # Create a session for message packing and unpacking
        self.session = Session(key, username)

        # Create a message handler for each socket
        self._shell_handler = _MessageHandler(self, 'shell')
        self._iopub_handler = _MessageHandler(self, 'iopub')
        self._stdin_handler = _MessageHandler(self, 'stdio')
        self._control_handler = _MessageHandler(self, 'control')


    def close(self):
        '''
        Shutdown
        :return: None
        '''
        self.shell.close()
        self.iopub.close()
        self.stdin.close()
        self.control.close()
        self.__ctx.close()


    def poll(self, timeout=0):
        '''
        Poll input sockets for incoming messages

        :param timeout: The amount of time to wait for a message in milliseconds.
                -1 = wait indefinitely, 0 = return immediately,
        :return:
        '''
        n_events = self.__poller.poll(timeout)
        while n_events > 0:
            if self.__poller.pollin(self.__shell_poll_index):
                ident, msg = self.session.recv(self.shell)
                ident = _unpack_ident(ident)
                self._shell_handler.handle(ident, msg)

            if self.__poller.pollin(self.__iopub_poll_index):
                ident, msg = self.session.recv(self.iopub)
                ident = _unpack_ident(ident)
                self._iopub_handler.handle(ident, msg)

            if self.__poller.pollin(self.__stdin_poll_index):
                ident, msg = self.session.recv(self.stdin)
                ident = _unpack_ident(ident)
                self._stdin_handler.handle(ident, msg)

            if self.__poller.pollin(self.__control_poll_index):
                ident, msg = self.session.recv(self.control)
                ident = _unpack_ident(ident)
                self._control_handler.handle(ident, msg)

            n_events = self.__poller.poll(0)


    def execute_request(self, code, silent=False, store_history=True, user_expressions=None, allow_stdin=True):
        '''
        Send an execute request to the remote kernel via the SHELL socket

        :param code: the code to execute
        :param silent:
        :param store_history:
        :param user_expressions:
        :param allow_stdin:
        :return:
        '''
        self.session.send(self.shell, 'execute_request', {
            'code': code,
            'silent': silent,
            'store_history': store_history,
            'user_expressions': user_expressions   if user_expressions is not None   else {},
            'allow_stdin': allow_stdin
        })


    def inspect_request(self, code, cursor_pos, detail_level=0):
        '''
        Send an execute request to the remote kernel via the SHELL socket

        :param code: the code to execute
        :param cursor_pos: the position of the cursor (in unicode characters) where inspection is requested
        :param detail_level: 0 or 1
        :return:
        '''
        self.session.send(self.shell, 'inspect_request', {
            'code': code,
            'cursor_pos': cursor_pos,
            'detail_level': detail_level
        })


    def on_execute_reply_ok(self, execution_count, payload, user_expressions):
        pass

    def on_execute_reply_error(self, execution_count, ename, evalue, traceback):
        pass

    def on_execute_reply_abort(self, execution_count):
        pass

    def on_inspect_reply_ok(self, data, metadata):
        pass

    def on_inspect_reply_error(self, ename, evalue, traceback):
        pass

    def on_stream(self, stream_name, data):
        pass

    def on_status(self, execution_state):
        pass

    def on_execute_input(self, execution_count, code):
        pass


    def _handle_msg_shell_execute_reply(self, ident, msg):
        content = msg['content']
        status = content['status']
        if status == 'ok':
            return self.on_execute_reply_ok(content['execution_count'], content['payload'],
                                            content['user_expressions'])
        elif status == 'error':
            return self.on_execute_reply_error(content['execution_count'], content['ename'],
                                               content['evalue'], content['traceback'])
        elif status == 'abort':
            return self.on_execute_reply_abort(content['execution_count'])
        else:
            raise ValueError, 'Unknown execute_reply status'

    def _handle_msg_shell_inspect_reply(self, ident, msg):
        content = msg['content']
        status = content['status']
        if status == 'ok':
            return self.on_inspect_reply_ok(content['data'], content['metadata'])
        elif status == 'error':
            return self.on_inspect_reply_error(content['ename'],
                                               content['evalue'], content['traceback'])
        else:
            raise ValueError, 'Unknown inspect_reply status'

    def _handle_msg_iopub_stream(self, ident, msg):
        content = msg['content']
        return self.on_stream(content['name'], content['data'])

    def _handle_msg_iopub_status(self, ident, msg):
        content = msg['content']
        return self.on_status(content['execution_state'])

    def _handle_msg_iopub_pyin(self, ident, msg):
        content = msg['content']
        return self.on_execute_input(content['execution_count'], content['code'])

    def _handle_msg_iopub_execute_input(self, ident, msg):
        content = msg['content']
        return self.on_execute_input(content['execution_count'], content['code'])




class Session (object):
    def __init__(self, key, username=''):
        '''
        IPython session constructor

        :param key: message authentication key from connection file
        :param username: Username of user (or empty string)
        :return:
        '''
        self.__key = key.encode('utf8')

        self.auth = hmac.HMAC(self.__key, digestmod=hashlib.sha256)

        self.session = str(uuid.uuid4())
        self.username = username

        self.__none = self.pack({})


    def send(self, stream, msg_type, content=None, parent=None, metadata=None, ident=None, buffers=None):
        '''
        Build and sent a message on a JeroMQ stream

        :param stream: the JeroMQ stream over which the message is to be sent
        :param msg_type: the message type (see IPython docs for explanation of these)
        :param content: message content
        :param parent: message parent header
        :param metadata: message metadata
        :param ident: IDENT
        :param buffers: binary data buffers to append to message
        :return: the message as a list of serialised byte arrays
        '''
        msg = self.msg(msg_type, content, parent, metadata)
        to_send = self.serialize(msg, ident)
        if buffers is not None:
            to_send.extend(buffers)
        for part in to_send[:-1]:
            stream.sendMore(part)
        stream.send(to_send[-1])
        return msg

    def recv(self, stream):
        '''
        Receive a message from a stream
        :param stream: the JeroMQ stream from which to read the message
        :return: a tuple: (idents, msg) where msg is the deserialized message
        '''
        msg_list = [stream.recv()]
        while stream.hasReceiveMore():
            msg_list.append(stream.recv())

        # Extract identities
        pos = msg_list.index(DELIM)
        idents, msg_list = msg_list[:pos], msg_list[pos+1:]
        return idents, self.deserialize(msg_list)


    def serialize(self, msg, ident=None):
        '''
        Serialize a message into a list of byte arrays

        :param msg: the message to serialize
        :param ident: the ident
        :return: the serialize message in the form of a list of byte arrays
        '''
        content = msg.get('content', {})
        if content is None:
            content = self.__none
        else:
            content = self.pack(content)

        payload = [self.pack(msg['header']),
                   self.pack(msg['parent_header']),
                   self.pack(msg['metadata']),
                   content]

        serialized = []

        if isinstance(ident, list):
            serialized.extend(ident)
        elif ident is not None:
            serialized.append(ident)
        serialized.append(DELIM)

        signature = self.sign(payload)
        serialized.append(signature)
        serialized.extend(payload)

        return serialized


    def deserialize(self, msg_list):
        '''
        Deserialize a message, converting it from a list of byte arrays to a message structure (a dict)
        :param msg_list: serialized message in the form of a list of byte arrays
        :return: message structure
        '''
        min_len = 5
        if self.auth is not None:
            signature = msg_list[0]
            check = self.sign(msg_list[1:5])
            if signature != check:
                raise ValueError, 'Invalid signature'
        if len(msg_list) < min_len:
            raise ValueError, 'Message too short'
        header = self.unpack(msg_list[1])
        return {
            'header': header,
            'msg_id': header['msg_id'],
            'msg_type': header['msg_type'],
            'parent_header': self.unpack(msg_list[2]),
            'metadata': self.unpack(msg_list[3]),
            'content': self.unpack(msg_list[4]),
            'buffers': msg_list[5:]
        }



    def msg_header(self, msg_type):
        '''
        Build a header for a message of the given type
        :param msg_type: the message type
        :return: the message header
        '''
        msg_id = str(uuid.uuid4())
        return {
            'msg_id': msg_id,
            'msg_type': msg_type,
            'username': self.username,
            'session': self.session,
            'date': datetime.datetime.now().isoformat(),
            'version': KERNEL_PROTOCOL_VERSION
        }

    def msg(self, msg_type, content=None, parent=None, metadata=None):
        '''
        Build a message of the given type, with content, parent and metadata
        :param msg_type: the message type
        :param content: message content
        :param parent: message parent header
        :param metadata: metadata
        :return: the message structure
        '''
        header = self.msg_header(msg_type)
        return {
            'header': header,
            'msg_id': header['msg_id'],
            'msg_type': msg_type,
            'parent_header': {} if parent is None   else parent,
            'content': {} if content is None   else content,
            'metadata': {} if metadata is None   else metadata,
        }


    def sign(self, msg_payload_list):
        '''
        Sign a message payload

        :param msg_payload_list: the message payload (header, parent header, content, metadata)
        :return: signature hash hex digest
        '''
        if self.auth is None:
            return StringUtil.toBytes('')
        else:
            h = self.auth.copy()
            for m in msg_payload_list:
                h.update(m)
            return StringUtil.toBytes(h.hexdigest())



    def pack(self, x):
        '''
        Pack message data into a byte array

        :param x: message data to pack
        :return: byte array
        '''
        return StringUtil.toBytes(json.dumps(x))

    def unpack(self, x):
        '''
        Unpack byte array into message data

        :param x: byte array to unpack
        :return: message component
        '''
        return json.loads(StringUtil.fromBytes(x))


