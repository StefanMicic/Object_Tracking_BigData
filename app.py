# import os

# from loguru import logger as log
# from processing.data_joining import Reader
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

"""
Real time obrada:
    1.  Odbacivanje osoba koje se ne nalaze unutar unapred definisanog prostora
    2.  Brojanje ljudi koji se nalaze u određenoj prostoriji
    3.  Transformisanje bounding box-ova u centre istih
Batch obrada:
    1.  Izvlačenje procentualnog korišćenja unapred definisanih regiona
    2.  Broj ljudi koji se nalazio u prostoriji za određeni vremenski period
    3.  Najčešći šablon kretanja osobe po sobi
    4.  Prosecno vreme ostajanja osobe po sobi
"""


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def main():
    # HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    conf = (
        SparkConf()
        .setAppName("Application")
        .setMaster("spark://spark-master:7077")
    )
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    quiet_logs(spark)


if __name__ == "__main__":
    main()
