
import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\Nares\.jdks\corretto-1.8.0_442'
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(50, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print()
# # 🔴WITHCOLUMN SIMPLE
#
#
# data="""
#
# {
#     "id": 1,
#     "trainer": "sai",
#     "zeyoAddress": {
#             "permanentAddress": "hyderabad",
#             "temporaryAddress": "chennai"
#     }
# }
# """
#
# rdd = sc.parallelize([data])
#
# df = spark.read.option("multiline","true").json(rdd)
#
#
# df.show()
#
# df.printSchema()
#
#
# flatdata = df.select(
#
#     "id",
#     "trainer",
#     "zeyoAddress.permanentAddress",
#     "zeyoAddress.temporaryAddress"
# )
#
# flatdata.show()
# flatdata.printSchema()
#
#
# withflat = (
#     df.withColumn( "permanentAddress" , expr("zeyoAddress.permanentAddress"))
#     .withColumn("temporaryAddress", expr("zeyoAddress.temporaryAddress"))
#     .drop("zeyoAddress")
# )
#
# withflat.show()
# withflat.printSchema()

# 🔴WITHCOLUMN STRUCT INSIDE STRUCT


# data="""
#
# {
#     "id": 1,
#     "trainer": "sai",
#     "zeyoAddress": {
#         "user": {
#             "permanentAddress": "hyderabad",
#             "temporaryAddress": "chennai"
#         }
#     }
# }
#
#
# """
#
# rdd = sc.parallelize([data])
#
# df = spark.read.option("multiline","true").json(rdd)
#
#
# df.show()
#
# df.printSchema()
#
#
# withflat = (
#     df.withColumn("permanentAddress", expr("zeyoAddress.user.permanentAddress"))
#     .withColumn("temporaryAddress", expr("zeyoAddress.user.temporaryAddress"))
#     .drop("zeyoAddress")
# )
#
# withflat.show()
#
# withflat.printSchema()

# 🔴WITHCOLUMN IMAGE EXAMPLE


data="""

{
	"id": "000",
	"type": "donut",
	"name": "Non cream",
	"image": {
		"url": "images/0001.jpg",
		"width": 200,
		"height": 200
	},
	"thumbnail": {
		"url": "images/thumbnails/0001.jpg",
		"width": 33,
		"height": 33
	}
}


"""

rdd = sc.parallelize([data])

df = spark.read.option("multiline","true").json(rdd)


df.show()

df.printSchema()




withflat = (

    df.withColumn( "i_height" , expr("image.height") )
    .withColumn( "i_url" , expr("image.url") )
    .withColumn( "i_width" , expr("image.width") )
    .withColumn( "t_height" , expr("thumbnail.height") )
    .withColumn( "t_url" , expr("thumbnail.url") )
    .withColumn( "t_width" , expr("thumbnail.width") )
    .drop("image","thumbnail")




)

withflat.show()

withflat.printSchema()