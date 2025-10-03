# train.py

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.sql import SQLContext

import paths

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("session").getOrCreate()
# Create SQL context
sqlContext = SQLContext(spark)

# Load the dataset
ratings = sqlContext.read.csv(paths.username_rating_ds, inferSchema=True, header=True)

# Data preprocessing
ratings = ratings.withColumn("rating", ratings["rating"].cast(IntegerType()))
ratings = ratings.na.drop()
ratings = ratings.filter(ratings.rating != -1)

# Split the data
(training, _) = ratings.randomSplit([0.8, 0.2])

# ALS Model
als = ALS(maxIter=100, regParam=0.01, userCol="user_id", itemCol="anime_id", ratingCol="rating", coldStartStrategy="drop", rank=5)
model = als.fit(training)

# Save the trained model
model_path = "ALS_5_Model"  # Modify as needed
model.save(model_path)
print("model trained")
