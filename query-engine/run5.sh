# compile
javac -classpath ../../hadoop-core-1.2.1.jar:../../hbase-0.94.18.jar -d classFolder/ *.java
jar -cvf queryEngine.jar -C classFolder .

# execute
hadoop jar queryEngine.jar part5.QueryEngine output1G/part-00000 master
