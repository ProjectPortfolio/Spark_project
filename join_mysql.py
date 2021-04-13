from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("join-rdbms").master("local[4]").config("spark.driver.extraClassPath","/Users/admin/Downloads/mysql-connector-java-8.0.23/mysql-connector-java-8.0.23.jar").getOrCreate()

df1 = spark.read\
    .format("jdbc")\
    .option("url","jdbc:mysql://localhost:3306/emp_db")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","product")\
    .option("user","root")\
    .option("password",'cloudera').load()

df2 = spark.read\
    .format("jdbc")\
    .option("url","jdbc:mysql://localhost:3306/emp_db")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","customer")\
    .option("user","root")\
    .option("password",'cloudera').load()

df1.show()
df2.show()

print("-------inner join---------")
inner_join = df1.join(df2, df1.prod_id == df2.customer_prod_id)
inner_join.show()



print("--------right join-----------")
right_join = df1.join(df2,df1.prod_id == df2.customer_prod_id, how='right')
right_join.show()

print("--------full join-----------")
full_join = df1.join(df2,df1.prod_id == df2.customer_prod_id, how='full')
full_join.show()
