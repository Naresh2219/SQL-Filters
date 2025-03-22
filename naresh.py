
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

# print()
# data="""
# {
#     "id": 1,
#     "trainer": "sai",
#         "zeyoStudents": [
#             "Ankita",
#             "Ajay"
#         ]
# }
# """
# data={
#     "id":1,
#     "trainer":"Sai",
#     "zeyostudents":[
#         "Naresh",
#         "Yashwanth"
#     ]
# }
#
# rdd = sc.parallelize([data])
#
# df = spark.read.option("multiline","true").json(rdd)
# df.show()
# df.printSchema()
# flatdata = df.selectExpr(
#     "id",
#     "trainer",
#     "explode(zeyostudents) as zeyoStudents"
# )
# flatdata.show();
# flatdata.printSchema();


#
# df.printSchema()
# data ={
#     "Name":"Naresh",
#     "mobile":79320833,
#     "Boolean":true,
#     "pets":["DOg,cat"]
#
# }
# rdd = sc.parallelize([data])
#
# df = spark.read.option("multiline","true").json(rdd)
#
#
# df.show()
#
# df.printSchema()

# ALONG WITH WITH COLUMN

data="""
{
    "id": 1,
    "trainer": "sai",
        "zeyoStudents": [
            "Ankita",
            "Ajay"
        ]
}
"""

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
# flatdata = df.selectExpr(
#
#     "id",
#     "trainer",
#     "explode(zeyoStudents) as zeyoStudents"
#
# )
#
# flatdata.show()
# flatdata.printSchema()
#
#
#
#
#
# withColexp = df.withColumn("zeyoStudents",expr("explode(zeyoStudents)"))
#
# withColexp.show()
# withColexp.printSchema()
# 🔴 *FULL CODE-STRUCT INSIDE ARRAY*

data="""
{
	"org": "zeyobron",
	"trainer": "zeyobron",
	"location": "Pune",
	"users": [{
			"userId": 1,
			"firstName": "Krish",
			"lastName": "Lee",
			"phoneNumber": 123456,
			"emailAddress": "krish.lee@learningcontainer.com"
		},
		{
			"userId": 2,
			"firstName": "racks",
			"lastName": "jacson",
			"phoneNumber": 123456,
			"emailAddress": "racks.jacson@learningcontainer.com"
		}
	]
}
"""

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
# exploddf = df.withColumn("users",expr("explode(users)"))
# exploddf.show()
# exploddf.printSchema()
#
#
# finaldf = exploddf.select(
#     "location",
#     "org",
#     "trainer",
#     "users.emailAddress",
#     "users.firstName",
#     "users.lastName",
#     "users.phoneNumber",
#     "users.userId"
# )
#
# finaldf.show()
# finaldf.printSchema()
# FULL URL CODE

# FULL URL CODE





import urllib.request

import ssl

urldata = (

    urllib.request

    .urlopen("https://randomuser.me/api/0.8/?results=10",context=ssl._create_unverified_context())

    .read()

    .decode("utf-8")

)



print(urldata)

rdd = sc.parallelize([urldata])



df = spark.read.json(rdd)



df.show()

df.printSchema()

explodedf = df.withColumn("results",expr("explode(results)"))

explodedf.show()

explodedf.printSchema()
finalexplode =explodedf.select(
    "nationality",
    "results.user.cell",
    "results.user.dob"

)
finalexplode.show();
finalexplode.printSchema();

# explodedf.printSchema()
#
# finalexplode =  explodedf.select(
#
#
#
#     "nationality",
#
#     "results.user.cell",
#
#     "results.user.dob",
#
#     "results.user.email",
#
#     "results.user.gender",
#
#     "results.user.location.city",
#
#     "results.user.location.state",
#
#     "results.user.location.street",
#
#     "results.user.location.zip",
#
#     "results.user.md5",
#
#     "results.user.name.first",
#
#     "results.user.name.last",
#
#     "results.user.name.title",
#
#     "results.user.password",
#
#     "results.user.phone",
#
#     "results.user.picture.large",
#
#     "results.user.picture.medium",
#
#     "results.user.picture.thumbnail",
#
#     "results.user.registered",
#
#     "results.user.salt",
#
#     "results.user.sha1",
#
#     "results.user.sha256",
#
#     "results.user.username",
#
#     "seed",
#
#     "version"
#
#
#
# )
#
#
#
# finalexplode.show()
#
#
#
# finalexplode.printSchema()