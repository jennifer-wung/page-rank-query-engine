
#delete output folder
#hadoop fs -rmr output1M

# compile
javac -classpath ../../hadoop-core-1.2.1.jar -d classFolder/ *.java
jar -cvf  pageGraph.jar -C classFolder .

# execute
hadoop jar pageGraph.jar build-graph.GraphJob input1M output1M

# see result
hadoop fs -cat output1M/part-00000
