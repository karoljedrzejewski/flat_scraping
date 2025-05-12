from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import avg, count, col, round as spark_round

def analyse_with_pyspark(data, filename):

    spark = SparkSession.builder.appName('FlatScraper').getOrCreate()

    schema = StructType([
        StructField('Location', StringType(), True),
        StructField('Price', DoubleType(), True),
        StructField('m2', DoubleType(), True),
        StructField('p/m2', DoubleType(), True),
        StructField('Date', StringType(), True),
        StructField('Text', StringType(), True),
        StructField('hl', StringType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)

    print(f"Dataframe count: {df.count()}")
    print(f"Dataframe schema: {df.printSchema()}")
    print(f"Dataframe columns: {df.columns}")
    print(f"Dataframe data: {df.show(5)}")
    print(f"Dataframe data types: {df.dtypes}")
    print(f"Dataframe first row: {df.first()}")
    print(f"Dataframe columns: {df.columns}")

    # Average price per m2 by location
    m2byloc = df.groupBy('Location').agg(spark_round(avg('p/m2'), 2).alias('avg_price_per_m2'))
    m2byloc.orderBy(col('avg_price_per_m2')).coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_avg_price_per_m2', header=True)

    # Average price by location
    pbyloc = df.groupBy('Location').agg(spark_round(avg('Price'), 2).alias('avg_price'))
    pbyloc.orderBy(col('avg_price')).coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_avg_price', header=True)

    # Offers count per location
    offcount = df.groupBy('Location').agg(count('*').alias('offer_count'))
    offcount.orderBy(col('offer_count').desc()).coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_offer_count', header=True)

    # Top 5 largest flats
    df.orderBy(col('m2').desc()).limit(5).coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_top5_largest_flats', header=True)

    # Top 5 smallest flats
    df.orderBy(col('m2').asc()).limit(5).coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_top5_smallest_flats', header=True)

    # Calculating median and mean price per m2
    approx_median = df.approxQuantile("p/m2", [0.5], 0.05)[0]
    mean = df.select(avg("p/m2")).first()[0]

    stats_df = spark.createDataFrame([(round(mean, 2), round(approx_median, 2))], ["mean_price_per_m2", "median_price_per_m2"])
    stats_df.coalesce(1).write.mode('overwrite').csv(f'/opt/airflow/data/{filename}_stats', header=True)

    spark.stop()
