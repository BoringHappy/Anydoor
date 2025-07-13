from pyspark.sql import SparkSession


def init_spark(executor_memory="4g"):
    packages = [
        "io.delta:delta-spark_2.12:3.2.1",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.0",
    ]

    spark = (
        SparkSession.builder.config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultsSize", "0")
        .getOrCreate()
    )
    return spark
