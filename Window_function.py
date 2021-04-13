from pyspark.sql import SparkSession , Window
from pyspark.sql import functions as f
from pyspark.sql import *

spark = SparkSession.builder.appName("Window_function").master("local[*]").getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
col = ["EName","Dept","Salary"]

df = spark.createDataFrame(data=simpleData,schema=col)

df.printSchema()
df.show()
window_spec_Agg = Window.partitionBy("Dept")

df1 = df.withColumn('avg',f.avg(f.col('Salary')).over(window_spec_Agg)).\
    withColumn("salaries",f.collect_list('Salary').over(window_spec_Agg)).\
    withColumn('sum',f.sum(f.col('Salary')).over(window_spec_Agg)).\
    withColumn('max',f.max(f.col('Salary')).over(window_spec_Agg)).\
    withColumn('min',f.min(f.col('Salary')).over(window_spec_Agg))

df1.show(truncate=False)

