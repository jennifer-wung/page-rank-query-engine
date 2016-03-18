#output folder
#hadoop fs -rmr output1G_inverted

# compile
javac -classpath ../../hadoop-core-1.2.1.jar -d classFolder/ *.java
jar -cvf invertedIndex.jar -C classFolder .

# execute
hadoop jar invertedIndex.jar part3.InvertedIndexBasic /opt/input/1G/input-1G output1G_inverted

# see result
hadoop fs -cat output1G_inverted/part-00000

