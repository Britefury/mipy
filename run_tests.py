##-*************************
##-* This software can be used, redistributed and/or modified under
##-* the terms of the BSD 2-clause license as found in the file
##-* 'License.txt' in this distribution.
##-* This source code is (C)copyright Geoffrey French 1999-2014.
##-*************************

import unittest
import sys
import os

if os.name == 'java':
	JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

	for j in JARS:
		if j not in sys.path:
			sys.path.append(j)



import mipy.kernel

testModules = [ mipy.kernel,
		]


if __name__ == '__main__':
	modulesToTest = []
	modulesToTest[:] = testModules
	
	if len( sys.argv ) > 1:
		modulesToTest = []
		for a in sys.argv[1:]:
			x = None
			for m in testModules:
				name = m.__name__
				if a == name:
					x = m
					break
			
			if x is None:
				for m in testModules:
					name = m.__name__
					if name.endswith( a ):
						x = m
						break

			if x is None:
				print 'No test module %s'  %  a
			else:
				modulesToTest.append( x )

	
	print 'Testing:'
	for m in modulesToTest:
		print '\t' + m.__name__
				

	loader = unittest.TestLoader()

	#print 'Testing the following modules:'
	#for m in testModules:
		#print m.__name__
	suites = [ loader.loadTestsFromModule( module )   for module in modulesToTest ]

	runner = unittest.TextTestRunner()

	results = unittest.TestResult()

	overallSuite = unittest.TestSuite()
	overallSuite.addTests( suites )

	runner.run( overallSuite )
