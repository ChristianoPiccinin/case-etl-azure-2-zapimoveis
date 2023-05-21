# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import current_timestamp, col


# mount raw
dbutils.fs.mount(
  source = "wasbs://raw@stcasezap001.blob.core.windows.net",
  mount_point = "/mnt/raw",
  extra_configs = {"fs.azure.account.key.stcasezap001.blob.core.windows.net":""})

# mount bronze
dbutils.fs.mount(
 source='wasbs://bronze@stcasezap001.blob.core.windows.net',
 mount_point = '/mnt/bronze',
 extra_configs = {"fs.azure.account.key.stcasezap001.blob.core.windows.net":""})

# mount silver
dbutils.fs.mount(
 source='wasbs://silver@stcasezap001.blob.core.windows.net',
 mount_point = '/mnt/silver',
 extra_configs = {"fs.azure.account.key.stcasezap001.blob.core.windows.net":""})

# mount gold
dbutils.fs.mount(
 source='wasbs://gold@stcasezap001.blob.core.windows.net',
 mount_point = '/mnt/gold',
 extra_configs = {"fs.azure.account.key.stcasezap001.blob.core.windows.net":""})

# get raw files
get_raw_file = "/mnt/raw/zap_imoveis/*.json"
df_raw = spark.read \
    .format("json") \
    .option("inferSchema", "true")\
    .option("header", "true") \
    .json(get_raw_file)
	
	
# save raw data in parquet format
write_delta_mode = "overwrite"
delta_bronze_zone = "/mnt/bronze/"
df_raw.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/zap_imoveis/")	

# load parquet file from bronze.
get_bronze_file = "/mnt/bronze/zap_imoveis"
df_bronze = spark.read.format("delta").load(get_bronze_file)


# select columns I will used
df_bronze_select = df_bronze.select(
    col("id").alias("id"),
    col("bathrooms").alias("bathrooms"),
    col("bedrooms").alias("bedrooms"),
    col("description").alias("description"),
    col("listingStatus").alias("listingStatus"),
    col("pricingInfos.businessType").alias("businessType"),
    col("pricingInfos.monthlyCondoFee").alias("monthlyCondoFee"),
    col("pricingInfos.period").alias("period"),
    col("pricingInfos.price").alias("price"),
    col("pricingInfos.rentalTotalPrice").alias("rentalTotalPrice"),
    col("pricingInfos.yearlyIptu").alias("yearlyIptu"),
    col("publicationType").alias("publicationType"),
    col("publisherId").alias("publisherId"),
    col("suites").alias("suites"),
    col("title").alias("title"),
    col("totalAreas").alias("totalAreas"),
    col("unitTypes").alias("unitTypes"),
    col("updatedAt") .alias("updatedAt"),
    col("usableAreas").alias("usableAreas"),
    col("createdAt").alias("createdAt"),
    col("address.country").alias("country"),
    col("address.state").alias("state"),
    col("address.city").alias("city"),
    col("address.neighborhood").alias("neighborhood"),
    col("address.street").alias("street"),
    col("address.streetNumber").alias("streetNumber"),
    col("address.zipCode").alias("zipCode"),
    col("address.zone").alias("zone"),
    col("address.district").alias("district"),
    col("address.geoLocation.location.lat").alias("lat"),
    col("address.geoLocation.location.lon").alias("lon"),
    lit(current_timestamp()).alias("event_time")
)

# register as a spark sql object
df_bronze_select.createOrReplaceTempView("vw_houses")


# save the sql into a dataframe
df_bronze_select_vw_houses = spark.sql("""
SELECT *  FROM vw_houses
""")

# save silver zone area
write_delta_mode = "overwrite"
delta_silver_zone = "/mnt/silver/"
df_bronze_select_vw_houses.write.mode(write_delta_mode).format("delta").save(delta_silver_zone + "/zap_imoveis/")               
