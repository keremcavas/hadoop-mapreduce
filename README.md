# hadoop-mapreduce

Mapper and Reducer classes can be found at src/main/java/hadoop/MapReducer.java


Before running the program hadoop cluster have to be running.
In .../hadoop/sbin/ directory run ```./start-dfs.sh``` and ```./start-yarn.sh```


This project uses maven dependices.  
For compiling the code and generating jar with dependices go .../hadoop-mapreduce/ folder and run
```
mvn clean compile assembly:single
```


After compiling finishes go .../hadoop/bin/ directory and run
```
./hadoop jar .../hadoop-mapreduce/target/hadoop-1.0-jar-with-dependencies.jar
```


Mapreduce mathods are created for big text files.  
Statistical mapreduce methods requires words' counts have counted.  
Before running statictical functions you have to run counting mapreduce(```Start mapreduce``` in gui).

Descriptive statistical functions programmed in this project:
```
max
average
standard deviation
median
sum
```

Tested with openjdk version "1.8.0_292" and Apache Hadoop 3.3.0  
Tested on Ubuntu 20.04.2 with 3 virtual machines: 1 NameNode and 2 DataNodes
