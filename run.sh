#!bin/bash
echo $1
echo $2
spark-submit --packages org.postgresql:postgresql:9.4-1201-jdbc41 jieba_spark.py $1 $2
