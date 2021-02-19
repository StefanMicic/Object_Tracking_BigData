r"""
 Run the example
    
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/real_time.py zoo1:2181 room-1
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 $SPARK_HOME/real_time.py zoo1:2181 room-1
"""  # noqa W291
import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def getSparkSessionInstance(sparkConf):
    if "sparkSessionSingletonInstance" not in globals():
        globals()[
            "sparkSessionSingletonInstance"
        ] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


# regions = {
#     "room-1": {
#         "room_bbox": [1000, 400, 1600, 2000],  # xmin, ymin, xmax,ymax
#         "regions": [[1, 2, 3, 4], [1, 2, 3, 4]],
#     },
#     "room-2": {
#         "room_bbox": [1, 2, 3, 4],
#         "regions": [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]],
#     },
#     "room-3": {
#         "room_bbox": [1, 2, 3, 4],
#         "regions": [[1, 2, 3, 4], [1, 2, 3, 4]],
#     },
#     "room-4": {
#         "room_bbox": [1, 2, 3, 4],
#         "regions": [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]],
#     },
# }


def check_if_inside(t, center, room):
    region = t[room]["room_bbox"]
    if center[0] < region[0] or center[0] > region[2]:
        return False
    elif center[1] < region[1] or center[1] > region[3]:
        return False
    return True


if __name__ == "__main__":
    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic1>", file=sys.stderr)
        sys.exit(-1)

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
    quiet_logs(sc)

    regions_config = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option(
            "uri",
            "mongodb://asvsp:asvsp@mongo:27017/asvsp.regions?authSource=admin",
        )
        .load()
    )
    t = regions_config.first()

    ssc = StreamingContext(sc, 1)

    zooKeeper, topic1 = sys.argv[1:]

    kvs = KafkaUtils.createStream(
        ssc, zooKeeper, "spark-streaming-consumer", {topic1: 1}
    )

    lines = kvs.map(lambda x: x[1])

    boxes = lines.map(
        lambda line: (
            line.split()[0],
            line.split()[1],
            line.split()[2],
            line.split()[3],
            line.split()[4],
            line.split()[5],
        )
    )
    boxes.pprint()
    centers = boxes.map(
        lambda box: (
            box[0],
            (int(box[1]) + int(box[3])) / 2,
            (int(box[2]) + int(box[4])) / 2,
            box[5],
        ),
    ).filter(
        lambda center: check_if_inside(t, (center[1], center[2]), center[3])
    )

    centers.pprint()
    centers.saveAsTextFiles(HDFS_NAMENODE + "/output/centers.csv")

    def process(time, rdd):
        print("========= %s USAO =========" % str(time))

        try:
            spark = getSparkSessionInstance(rdd.context.getConf())
            rowRdd = rdd.map(
                lambda w: Row(
                    person_id=w[0], center_x=w[1], center_y=w[2], room=w[3]
                )
            )
            wordsDataFrame = spark.createDataFrame(rowRdd)
            wordsDataFrame.write.format("csv").mode("append").save(
                HDFS_NAMENODE + "/home/output.csv"
            )
            wordsDataFrame.createOrReplaceTempView("words")

            counts = wordsDataFrame.groupBy("room").count()
            counts.show()
            counts.write.format("csv").mode("append").save(
                HDFS_NAMENODE + "/home/counts.csv"
            )

        except Exception as e:
            print(e)

    centers.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
