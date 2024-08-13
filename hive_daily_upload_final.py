from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("hive_data_upload").enableHiveSupport().getOrCreate()

filepath='file:////home/talentum/shared/Project/Project_codes/source/*.json'
stock_data=spark.read.option("multiline","true").json(filepath)

from pyspark.sql.functions import explode
stock_data = stock_data.select("symbol", explode("historical").alias("historical"))
stock_data = stock_data.select("symbol", "historical.*")
stock_data.write.mode("overwrite").saveAsTable("STOCKS")
