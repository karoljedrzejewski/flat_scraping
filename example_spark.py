from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import avg, count, min, max, col, round

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

    # Average price per m2 by location
    print('Average price per m2 by location:')
    m2byloc = df.groupBy('Location').agg(round(avg('p/m2'), 2).alias('avg_price_per_m2'))
    m2byloc.orderBy(col('avg_price_per_m2')).show(m2byloc.count(), truncate=False)

    # Average price by location
    print('Average price by location:')
    pbyloc = df.groupBy('Location').agg(round(avg('Price'), 2).alias('avg_price'))
    pbyloc.orderBy(col('avg_price')).show(pbyloc.count(), truncate=False)

    # Offers count per location
    print('Offers count per location:')
    offcount = df.groupBy('Location').agg(count('*').alias('offer_count'))
    offcount.orderBy(col('offer_count').desc()).show(offcount.count(), truncate=False)

    # Top 5 most expensive flats by total size (m2)
    print('Top 5 largest flats:')
    df.orderBy(col('m2').desc()).show(5)

    # Top 5 smallest flats by total size (m2)
    print('Top 5 smallest flats:')
    df.orderBy(col('m2').asc()).show(5)

    # Calculating median and mean price per m2
    approx_median = df.approxQuantile("p/m2", [0.5], 0.05)[0]
    mean = df.select(avg("p/m2")).first()[0]


    print(f"Average price/m2: {mean:.2f} zł")
    print(f"Median price/m2: {approx_median:.2f} zł")

    df.write.mode('overwrite').csv(filename)