#output folder
#hadoop fs -rmr output1G

# compile
javac -classpath ../../hadoop-core-1.2.1.jar -d classFolder/ *.java
jar -cvf  pageRank.jar -C classFolder .

# execute
hadoop jar pageRank.jar part2.PageRankingJob /opt/input/1G/input-1G output1G

# see result
hadoop fs -cat output1G/result/part-00000

