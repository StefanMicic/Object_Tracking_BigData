import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

"""
    4.  Prosecno vreme ostajanja osobe po sobi
"""


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def main():
    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    conf = (
        SparkConf().setAppName("Step 1").setMaster("spark://spark-master:7077")
    )
    sc = SparkContext(conf=conf)
    spark = (
        SparkSession(sc)
        .builder.appName("Task2")
        .config(
            "spark.mongodb.output.uri",
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.task4?authSource=admin",
        )
        .getOrCreate()
    )

    quiet_logs(spark)

    df = spark.read.csv(HDFS_NAMENODE + "/home/data.csv")
    df = df.withColumnRenamed("_c0", "timestamp")
    df = df.withColumnRenamed("_c1", "person_id")
    df = df.withColumnRenamed("_c4", "room")
    df = df.withColumnRenamed("_c5", "day")

    df = df.groupBy("person_id", "room").count().orderBy("person_id")
    df.show()
    df = df.groupBy("room").agg(avg(col("count")))
    df.show()
    df.write.format("com.mongodb.spark.sql.DefaultSource").mode(
        "overwrite"
    ).save()


if __name__ == "__main__":
    main()
