from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, to_json
from pyspark.sql.types import DoubleType, StructType, StructField, FloatType, StringType
from pyspark.ml.classification import RandomForestClassificationModel
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder \
    .appName("DiabetesClassification") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.driver.host", "192.168.100.23") \
    .getOrCreate()

columns = ["HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
                   "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
                   "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
                   "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income"]
model = RandomForestClassificationModel.load("random_forest_tree")
scaler = StandardScalerModel.load("scaler")

schema = StructType([StructField(column_name, DoubleType()) for column_name in columns])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "diabetes_data") \
    .option("startingOffsets", "latest") \
    .load()

health_data_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")


@pandas_udf(FloatType())
def predict_udf(*cols):
    input_data = pd.concat(cols, axis=1)
    input_data.columns = columns
    assembler = VectorAssembler(inputCols=columns, outputCol="features")
    input_transformed = assembler.transform(input_data)
    scaled_data = scaler.transform(input_transformed)

    predictions = model.predict(scaled_data)
    return pd.Series(predictions)


predictions_df = health_data_df.withColumn("predicted_class", predict_udf(*columns))

output_df = predictions_df.select(
    F.to_json(F.struct("*")).alias("value")
)

query = output_df \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "health_data_predicted") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()


