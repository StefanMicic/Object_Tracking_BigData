import os

import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window

"""
    5. Izvlačenje procentualnog korišćenja unapred definisanih termina
"""


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def find_time(time):
    times = [
        "00:00:00",
        "03:00:00",
        "06:00:00",
        "09:00:00",
        "12:00:00",
        "15:00:00",
        "18:00:00",
        "21:00:00",
    ]
    for i, r in enumerate(times):
        if i == len(times) - 1:
            return i + 1
        elif time > r and time < times[i + 1]:
            return i + 1
    else:
        return 0


def main():

    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
    conf = (
        SparkConf().setAppName("Step 2").setMaster("spark://spark-master:7077")
    )
    sc = SparkContext(conf=conf)
    spark = (
        SparkSession(sc)
        .builder.appName("Task2")
        .config(
            "spark.mongodb.output.uri",
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.task5?authSource=admin",
        )
        .getOrCreate()
    )

    quiet_logs(spark)

    df = spark.read.csv(HDFS_NAMENODE + "/home/data.csv")
    df = df.withColumnRenamed("_c4", "room")

    times = df.rdd.map(
        lambda data: (
            data[4],
            data[0],
            find_time((data[0])),
        )
    )
    timeRdd = times.map(lambda w: Row(room=w[0], real_time=w[1], time=w[2]))

    timesDataFrame = spark.createDataFrame(timeRdd)

    timesDataFrame.show()
    timesDataFrame = timesDataFrame.groupBy("room", "time").count()

    timesDataFrame.show()

    timesDataFrame = timesDataFrame.withColumn(
        "percent",
        f.col("count") / f.sum("count").over(Window.partitionBy("room")),
    )
    timesDataFrame.show()
    timesDataFrame.write.format("com.mongodb.spark.sql.DefaultSource").mode(
        "overwrite"
    ).save()


if __name__ == "__main__":
    main()
