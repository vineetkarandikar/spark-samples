from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField, StructType ,StringType,FloatType,IntegerType
from pyspark.sql.functions import col, lit
from pyspark import SparkContext
from pyspark.sql import SparkSession
#To avoid CRC and Success file creation
sqlContext = SQLContext(sc)

sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

#create single row for museum by the name and it's subsequent categoies by columns
museum_categories_df = spark.read.option("inferSchema",True).option("header",True).json("s3a://data-lake-demo-vineet/raw-zone/museum_categories_world.json")
names = museum_categories_df.schema.names
museum_categories_transformed_data = []
na = "NA"
for name in names:
    if "." in name:
      name = "`"+ name + "`"
    my_list = museum_categories_df.select(name).rdd.flatMap(lambda x: x).collect()
    length = len(my_list[0])
    #my_list = museum_categories_df.select(f.collect_list(name)).first()[0]    
    if length==1:
       data = {"Name": name, "category_1": my_list[0][0], "category_2": na, "category_3": na,"category_4":na,"category_5":na }
    elif length==2:  
         data = {"Name": name, "category_1": my_list[0][0], "category_2": my_list[0][1], "category_3": na,"category_4":na,"category_5":na}
    elif length==3:
         data = {"Name": name, "category_1": my_list[0][0], "category_2": my_list[0][1], "category_3": my_list[0][2],"category_4":na,"category_5":na }
    elif length==4:
         data = {"Name": name, "category_1": my_list[0][0], "category_2": my_list[0][1], "category_3": my_list[0][2],"category_4":my_list[0][3],"category_5":na}
    elif length==5:
         data = {"Name": name, "category_1": my_list[0][0], "category_2": my_list[0][1], "category_3": my_list[0][2],"category_4":my_list[0][3],"category_5":my_list[0][4]}
    museum_categories_transformed_data.append(data)

museum_category_schema = StructType([StructField("Name", StringType(), False), StructField("category_1", StringType(), False), StructField("category_2", StringType(), True), StructField("category_3", StringType(),True),StructField("category_4", StringType(),True),StructField("category_5", StringType(),True)])
museum_category_rdd_one = sc.parallelize(museum_categories_transformed_data)
museum_category_df_two = sqlContext.createDataFrame(museum_category_rdd_one,museum_category_schema)
museum_category_df_two.show(5)
museum_category_df_two.write.mode('append').format('json').save('s3a://data-lake-demo-vineet/demo-curated-zone/museum_category')

#create single row for museum by the name and it's subsequent categoies by columns
museum_reviews_df = spark.read.option("inferSchema",True).option("header",True).json("s3a://data-lake-demo-vineet/raw-zone/review_quote_world.json")
names = museum_reviews_df.schema.names
museum_reviews_df_transformed_data = []
na = "NA"
for name in names:
    if "." in name:
      name = "`"+ name + "`"
    my_list = museum_reviews_df.select(name).rdd.flatMap(lambda x: x).collect()
    data = {"Name": name, "reviews":','.join(my_list[0])}
    museum_reviews_df_transformed_data.append(data)
museum_reviews_schema = StructType([StructField("Name", StringType(), False), StructField("reviews", StringType(), False)])
museum_reviews_rdd_one = sc.parallelize(museum_reviews_df_transformed_data)
museum_reviews_df_two = sqlContext.createDataFrame(museum_reviews_rdd_one,museum_reviews_schema)
museum_reviews_df_two.show(5)
museum_reviews_df_two.write.mode('append').format('json').save('s3a://data-lake-demo-vineet/demo-curated-zone/museum_reviews')


#create single row for museum by the name and it's subsequent categoies by columns
tag_clouds_df = spark.read.option("inferSchema",True).option("header",True).json("s3a://data-lake-demo-vineet/raw-zone/tag_clouds_world.json")
names = tag_clouds_df.schema.names
tag_clouds_df_transformed_data = []
na = "NA"
for name in names:
    if "." in name:
      name = "`"+ name + "`"
    my_list = tag_clouds_df.select(name).rdd.flatMap(lambda x: x).collect()
    data = {"Name": name, "tags":','.join(my_list[0])}
    tag_clouds_df_transformed_data.append(data)
tag_clouds_schema = StructType([StructField("Name", StringType(), False), StructField("tags", StringType(), False)])
tag_clouds_rdd_one = sc.parallelize(tag_clouds_df_transformed_data)
tag_clouds_df_two = sqlContext.createDataFrame(tag_clouds_rdd_one,tag_clouds_schema)
tag_clouds_df_two.show(5)
tag_clouds_df_two.write.mode('append').format('json').save('s3a://data-lake-demo-vineet/demo-curated-zone/tag_clouds')

tripadvisor_museum_world_df = spark.read.option("inferSchema",True).option("header",True).csv("s3a://data-lake-demo-vineet/raw-zone/tripadvisor_museum_world.csv")
tripadvisor_museum_world_df = tripadvisor_museum_world_df.dropDuplicates()
tripadvisor_museum_world_df = tripadvisor_museum_world_df.drop('FeatureCount','_c0','Description',)
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn('ReviewCount', regexp_replace('ReviewCount', ',', ''))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("Langtitude", tripadvisor_museum_world_df["Langtitude"].cast(FloatType()))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("Latitude", tripadvisor_museum_world_df["Latitude"].cast(FloatType()))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("Rank", tripadvisor_museum_world_df["Rank"].cast(IntegerType()))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("ReviewCount", tripadvisor_museum_world_df["ReviewCount"].cast(IntegerType()))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("TotalThingsToDo", tripadvisor_museum_world_df["TotalThingsToDo"].cast(IntegerType()))
tripadvisor_museum_world_df = tripadvisor_museum_world_df.withColumn("Rating", tripadvisor_museum_world_df["Rating"].cast(FloatType()))
tripadvisor_museum_world_df.show(5)
tripadvisor_museum_world_df.write.mode('append').format('json').save('s3a://data-lake-demo-vineet/demo-curated-zone/trip_advisor_museum_world')