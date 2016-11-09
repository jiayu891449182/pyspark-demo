from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import jieba_cutword
import sys

reload(sys)
sys.setdefaultencoding( "utf-8" )
APP_NAME = "spark jieba application"

url = "jdbc:postgresql://ip/db"
properties = {"user": "......","password": "......",'driver': 'org.postgresql.Driver'}

def split(s):
    ss = s.split(',')
    if len(ss)<8:
        return ""
    comment = ss[3].replace(':','')
    cutword = jieba_cutword.split_jieba(comment)
    return (ss[0],ss[1],ss[2],cutword)

def main(sc):
    text =sc.textFile("file:///....../comment.csv")
    word = text.map(split).filter(lambda x: x != "")
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(word, ["url", "id", "name","comment"])
    df.show()
    df.write.jdbc(url=url, table="test", mode="append", properties=properties)

if __name__ =="__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local")
    sc = SparkContext(conf=conf)
    main(sc)
