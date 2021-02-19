import os

import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession, Window
from pyspark.sql.functions import col, size

# from pyspark.sql.functions import countDistinct, lit

"""
    3.  Najčešći šablon kretanja osobe po sobi
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
        SparkConf().setAppName("Step 3").setMaster("spark://spark-master:7077")
    )
    sc = SparkContext(conf=conf)
    spark = (
        SparkSession(sc)
        .builder.appName("Task2")
        .config(
            "spark.mongodb.output.uri",
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.task3?authSource=admin",
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
    # regions_config.show()
    rooms = regions_config.columns
    patterns = {}
    for r in rooms:
        if r == "_id":
            continue
        num = len(t["room-1"]["regions"])
        patterns[r] = []
        for i in range(1, num + 1):
            for j in range(1, i + 1):
                if i != j:
                    patterns[r].append([i, j])
                    patterns[r].append([j, i])

    df = spark.read.csv(HDFS_NAMENODE + "/home/data.csv")
    df = df.withColumnRenamed("_c4", "room")

    regions = df.rdd.map(
        lambda data: (
            data[4],
            data[1],
            find_region(t, (float(data[2]), float(data[3])), data[4]),
        )
    )
    rowRdd = regions.map(lambda w: Row(room=w[0], person_id=w[1], region=w[2]))

    regionsDataFrame = spark.createDataFrame(rowRdd)

    regionsDataFrame = regionsDataFrame.distinct()

    regionsDataFrame = regionsDataFrame.groupBy("person_id", "room").agg(
        f.collect_set("region")
    )

    # regionsDataFrame.show()
    templates = regionsDataFrame.groupBy("room", "collect_set(region)").count()
    templates = templates.where(size(col("collect_set(region)")) >= 2)
    templates = templates.filter(templates["count"] > 1)
    templates.show()
    w = Window.partitionBy("room")

    templates = (
        templates.withColumn("maxB", f.max("count").over(w))
        .where(f.col("count") == f.col("maxB"))
        .drop("maxB")
    )
    templates.show()
    templates.write.format("com.mongodb.spark.sql.DefaultSource").mode(
        "overwrite"
    ).save()


if __name__ == "__main__":
    main()
