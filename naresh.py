
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
# 🔴 ALL MOST ALL FILTER


data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash"),
    ("00006", "03-02-2025", 300, "Food", "Biryani","debit"),
    ("00007", "02-15-2025", 500, "Food", "Pizza", "debit"),
    ("00008", "01-20-2023", 150, "Clothing", "T-shirt", "credit"),
    ("00009", "11-30-2022", 400, "Electronics", "Laptop", "credit"),
    ("00010", "07-04-2024", 250, "Entertainment", "Movie Tickets", "debit"),
    ("00011", "09-19-2024", 350, "Groceries", "Vegetables", "paytm"),
    ("00012", "04-15-2025", 600, "Health", "Supplements", "cash"),
    ("00013", "08-10-2022", 150, "Food", "Sandwich", "cash"),
    ("00014", "05-15-2023", 250, "Electronics", "Phone", "credit"),
    ("00015", "04-10-2023", 600, "Entertainment", "Movie Tickets", "debit"),
    ("00016", "06-25-2024", 200, "Groceries", "Vegetables", "credit"),
    ("00017", "08-11-2024", 400, "Health", "Vitamins", "debit"),
    ("00018", "03-01-2022", 300, "Team Sports", "Football", "cash"),
    ("00019", "01-01-2022", 180, "Food", "Burger", "cash"),
]
df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])

df1 = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
#cust.createOrReplaceTempView("cust")
#prod.createOrReplaceTempView("prod")

print("Single cloumn data")
spark.sql("select * from df where category = 'Exercise'").show()
print("Multi cloumn data")
spark.sql("select * from df where category in ('Exercise' ,'Electronics')").show()
print("Null data")
spark.sql("select * from df where product is null").show()
print("Max ID")
spark.sql("select max(id) from df").show()
print("count")
spark.sql("select count(1) from df").show()
print("Status of payments")
spark.sql("select *, case when spendby = 'cash' then 'green' when spendby = 'paytm' then 'NA' else 'blue' end as status from df").show()
print("Concat two columns")
spark.sql("select id,category,concat(id,'-',category) from df").show()
spark.sql("select id,category,concat(id,'-',category) as condata from df").show()
spark.sql("select id,category,concat(id,'-',category,'-',product) as condata from df").show()
print("lower case")
spark.sql("select category, lower(category) as lower from df").show()
print("UPPER CASE")
spark.sql("select category, upper(category) as upper from df").show()
print("CEIL operation")
spark.sql("select amount,ceil(amount) as ceil from df").show()
print("Round operation")
spark.sql("select amount,round(amount) as round from df").show()
print("Replace Nulls")
spark.sql("select  product,coalesce(product, 'NA') as nullrep from df").show()
print("Trim Space")
spark.sql("select trim(product) from df").show()
print("Distinct")
spark.sql("select distinct category, spendby from df").show()
print("Substring")
spark.sql("select substring(product,1,10) as sub from df").show()
print("Split")
spark.sql("select product,split(product,' ')[0] as split from df").show()

UNION
spark.sql("select * from df union all select * from df1").show()

Total amount col for Each catogery
spark.sql("select category,sum(amount) as sum from df group by category").show()

Total amount category spendby
spark.sql("select category,spendby,sum(amount) as sum,count(amount) as count from df group by category,spendby").show()


what is MAX amount of every category
spark.sql("select category, max(amount) as MAX from df group by category order by category desc").show()

Window Row Number
spark.sql("SELECT category, amount, densc_rank() OVER (PARTITION BY category ORDER BY amount DESC) AS row_number FROM df").show()

#