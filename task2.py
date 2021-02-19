import os

import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window

# from pyspark.sql.functions import countDistinct, lit

"""
    2. Izvlačenje procentualnog korišćenja unapred definisanih regiona
"""


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def find_region(t, center, room):
    region = t[room]["regions"]
    for i, r in enumerate(region):
        if center[0] < r[0] or center[0] > r[2]:
            continue
        elif center[1] < r[1] or center[1] > r[3]:
            continue
        else:
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
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.task2?authSource=admin",
        )
        .getOrCreate()
    )

    quiet_logs(spark)
    regions_config = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option(
            "uri",
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.regions?authSource=admin",
        )
        .load()
    )
    t = regions_config.first()
    df = spark.read.csv(HDFS_NAMENODE + "/home/data.csv")
    df = df.withColumnRenamed("_c4", "room")

    regions = df.rdd.map(
        lambda data: (
            data[4],
            find_region(t, (float(data[2]), float(data[3])), data[4]),
        )
    )
    rowRdd = regions.map(lambda w: Row(room=w[0], region=w[1]))

    regionsDataFrame = spark.createDataFrame(rowRdd)
    regionsDataFrame.show()

    regionsDataFrame = regionsDataFrame.groupBy("region", "room").count()

    regionsDataFrame.show()

    regionsDataFrame = regionsDataFrame.withColumn(
        "percent",
        f.col("count") / f.sum("count").over(Window.partitionBy("room")),
    )
    regionsDataFrame.show()

    regionsDataFrame.write.format("com.mongodb.spark.sql.DefaultSource").mode(
        "overwrite"
    ).save()


if __name__ == "__main__":
    main()
