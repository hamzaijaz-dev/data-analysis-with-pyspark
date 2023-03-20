from pyspark.sql import SparkSession


def init_spark():
    spark = SparkSession.builder.appName("Test Projects").getOrCreate()
    # config MySQL db connector in case loading into DB directly
    # .config("spark.driver.extraClassPath", "/usr/local/spark/jars/mysql-connector-java-8.0.16.jar")
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()

    # TODO TASK simple
    # nums = sc.parallelize([1, 2, 3, 4])
    # print(nums.map(lambda x: x * x).collect())

    # TODO TASK read data
    data_file_csv = 'datasource/used_car_prices*.csv'
    sdfData = spark.read.csv(data_file_csv, header=True, sep=",").cache()

    # data_file_json = 'datasource/used_car_prices*.json'
    # sdfData = spark.read.json(data_file_json).cache()

    fuel = sdfData.groupBy('fuel').count()
    print(fuel.show())

    # print('Total Records = {}'.format(sdfData.count()))
    # sdfData.show()

    # TODO TASK database query
    # sdfData.createOrReplaceTempView("used_cars")

    # output = spark.sql('SELECT * from used_cars')
    # output = spark.sql('SELECT * from used_cars ORDER BY year_of_manufacture DESC ')
    # output = spark.sql('SELECT * from used_cars WHERE year_of_manufacture > 2015 ')

    # output.show()

    # create multiple files - each work is involved in the operation of writing in the file
    # output.write.format('json').save('pyspark_filtered_data.json')

    # collects and reduces the data from all partitions to a single dataframe
    # output.coalesce(1).write.format('json').save('pyspark_filtered_data.json')

    # load data in MySQL
    # output.write.format('jdbc').options(
    #     url='jdbc:mysql://localhost/spark',
    #     driver='com.mysql.cj.jdbc.Driver',
    #     dbtable='cars_info',
    #     user='root',
    #     password='root').mode('append').save()


if __name__ == '__main__':
    main()
