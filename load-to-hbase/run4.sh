# compile
javac -classpath ../../hadoop-core-1.2.1.jar:../../hbase-0.94.18.jar -d classFolder/ *.java
jar -cvf  loadToHBase.jar -C classFolder .

# execute
#hadoop jar loadToHBase.jar part4.HBase.LoadPageRankToHBase HW2_output1G/result/part-00000
hadoop jar loadToHBase.jar load-to-hbase.HBase.LoadInvertedIndexToHBase output1G_inverted/part-00000
