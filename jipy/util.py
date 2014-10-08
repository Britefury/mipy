class MessageRouter(object):
	'''
	    Message router

	    Takes an incoming message and invokes a corresponding handler method on the attached object
	    '''

	def __init__(self, instance, socket_name):
		'''
		Message router constructor

		:param instance: the object on which handler methods can be found
		:param socket_name: the name of the socket that the router receives messages from
		'''
		self.__handler_method_cache = {}
		self.__instance = instance
		self.__socket_name = socket_name


	def handle(self, idents, msg):
		'''
		Handle a message

		Will look for a handler method on the attached instance (given as an argument to the constructor).
		Will look for a method called _handle_msg_<socket_name>_<msg_type>.
		Handler methods should be of the form:
		def _handle_msg_iopub_status(self, idents, msg)

		The message router for the 'iopub' socket will router messages whose msg_type is 'status' to
		the above method.

		:param idents: the ZeroMQ idents
		:param msg: the message to route
		'''

		msg_type = msg['msg_type']
		try:
			bound_method = self.__handler_method_cache[msg_type]
		except KeyError:
			method_name = '_handle_msg_{0}_{1}'.format(self.__socket_name, msg_type)
			try:
				bound_method = getattr(self.__instance, method_name)
			except AttributeError:
				bound_method = None
			self.__handler_method_cache[msg_type] = bound_method

		if bound_method is not None:
			return bound_method(idents, msg)
		else:
			print 'WARNING: socket {0} did not handle message of type {1} with ident {2}'.format(
				self.__socket_name, msg_type, idents)



