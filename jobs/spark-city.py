from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

def main():
    spark = SparkSession.builder.appName('SmartCityStreaming')\
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk:1.11.469')\
            .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')\
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))\
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))\
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
            .getOrCreate()
    
    ## Adjust the loglev to minimize the output
    spark.sparkContext.setLogLevel('WARN')

    ## VehicleSchema
    vehicleSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceid', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fueltype', StringType(), True),
    ])

    ## gpsSchema
    gpsSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceid', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicleType', StringType(), True)
    ])

    ## traffic schema
    trafficSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceid', StringType(), True),
        StructField('cameraId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('snapshot', StringType(), True),
    ])

    ## weather schema
    weatherSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceid', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('weatherCondition', DoubleType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('windspeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True),
    ])

    ## emergency data schema
    emergencySchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceid', StringType(), True),
        StructField('incidentId', StringType(), True),
        StructField('type', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('status', StringType(), True),
        StructField('description', StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream.
                format('kafka').
                option('kafka.bootstrap.servers', 'broker:29092').
                option('subscribe', topic).
                option('startingOffsets', 'earliest').
                load().
                selectExpr('CAST(value AS STRING)').
                select(from_json(col('value'),schema).alias('data')).
                select('data.*').
                withWatermark('timestamp', '2 minutes')
                )

    def streamWriter(input, checkpointFolder, output):
        return (input.writeStream.format('parquet').option('checkpointLocation', checkpointFolder)\
                .option('path', output).outputMode('append').start())

    vehicleDF = read_kafka_topic(topic='vehicle_data', schema=vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic(topic='gps_data', schema=gpsSchema).alias('gps')
    trafficDF = read_kafka_topic(topic='traffic_data', schema=trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic(topic='weather_data', schema=weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic(topic='emergency_data', schema=emergencySchema).alias('emergency')

    q1=streamWriter(input=vehicleDF,
                checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/vehicle_data',
                output='s3a://spark-streaming-data-smart-city-project/data/vehicle_data')
    q2=streamWriter(input=gpsDF,
                checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/gps_data',
                output='s3a://spark-streaming-data-smart-city-project/data/gps_data')
    q3=streamWriter(input=trafficDF,
                checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/traffic_data',
                output='s3a://spark-streaming-data-smart-city-project/data/traffic_data')
    q4=streamWriter(input=weatherDF,
                checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/weather_data',
                output='s3a://spark-streaming-data-smart-city-project/data/weather_data')
    q5=streamWriter(input=emergencyDF,
                checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/emergency_data',
                output='s3a://spark-streaming-data-smart-city-project/data/emergency_data')
    q5.awaitTermination()

if __name__ == '__main__':
    main()