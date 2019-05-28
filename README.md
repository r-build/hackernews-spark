# HackerNews posts exploration using Spark RDD (Big Data)

This project uses the Spark RDD (resilient distributed dataset ) API for data processing.

The dataset consists of nearly a million submitted HackerNews posts.

The Jupyter notebook walks through a number of exploratory tasks. The file `spark_rdd.py` has the same tasks defined as functions. 

# Requirements (March 2019)

You need to have JDK 8 installed. Other versions will not work.

https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

Make sure that the environment variable `JAVA_HOME` is correct and pointing to your JDK8 installation.

# Packages
Using conda:
`conda create -n hacknews-spark python=3 jupyter numpy pandas matplotlib seaborn pyspark=2.3.0 pyarrow`

`pip install pyspark==2.3.0 pyarrow`

### Getting it working on Windows 10 (March 2019)
This worked for me to set JAVA_HOME, and works for checking it too (taken from a stackoverflow answer):

1) Find the JDK installation directory.  On my windows 10, 64bit system, the Java 8 JDK is at:
`C:\Program Files\Java\jdk1.8.0_191`

- do not use the *jre* directory, which has an almost identical path,  (e.g. C:\Program Files\Java\jre1.8.0_191 )

- only use the path to the jdk directory, don't add any sub-folders such as \bin

- don't use any other versions you may have (e.g. I also have Java 11 at C:\Program Files\Java\jdk-11.0.1).

2) Set the `JAVA_HOME` Variable.
- Right-click 'My Computer' or 'This PC' icon in Windows Explorer and select Properties (or use some other way to get to System Properties).
- Click the Advanced tab (or Advance System Settings, depending on Windows version), then click the Environment Variables button.
- Under System Variables, click New.
- Enter the variable name as `JAVA_HOME`.
- Enter the JDK installation path as the variable value.
- Click OK, Apply, OK, etc

Note: You might need to restart Windows 

