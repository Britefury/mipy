jipy - a library for connecting to a remote IPython kernel from Jython, using JeroMQ.





Required libraries
===

You will need:

* Guava v17; download guava-17.0.jar
* JeroMQ 0.3.4 or later; download or build a JAR

Place both JAR files in this directory. Modify the line:

    JARS = ['jeromq-0.3.5-SNAPSHOT.jar', 'guava-17.0.jar']

in test.py to match the names of the JARs you have.


Running the test
===

> jython test.py <kernel_id>
