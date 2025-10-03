import pandas as pd
import infer
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, SQLContext

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("session").getOrCreate()
    df = pd.read_csv("datasets/users-details-2023.csv", usecols=['Mal ID'])
    samples = int(input("how many samples:"))
    model_path = "ALS_5_Model"
    model = ALSModel.load(model_path)
    for i in range(samples):
        result = infer.recommend(model, df.sample(1).iloc[0][0])
        print(result)

