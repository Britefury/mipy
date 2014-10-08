mipy - a library for connecting to a remote IPython kernel from Jython, using JeroMQ or CPython using pyzmq.





Required libraries on Jython
===

To run on Jython will need:

* Guava v17; download guava-17.0.jar
* JeroMQ 0.3.4 or later; download or build a JAR

Place both JAR files in this directory. Modify the line:

    JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

in run_tests.py to match the names of the JARs you have.


Running the tests on Jython
===

> jython run_tests.py



Required libraries on CPython
===

To run on CPython will need:

* pyzmq



Running the tests on CPython
===

> python run_tests.py
