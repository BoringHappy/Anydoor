from typing import Any, Dict, List


def init_spark(
    driver_memory: str = "4g",
    executor_memory: str = "4g",
    warehouse_location: str = "/data/lake",
    ivy_location: str = "/data/lake/.ivy",
    spark_jars_packages: List[str] = [
        "io.delta:delta-spark_2.12:3.2.1",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.0",
    ],
    spark_config: Dict[str, str] = {},
) -> Any:
    from pyspark import SparkConf  # type: ignore[import-not-found]
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    # Create SparkConf object
    conf = SparkConf()

    # Set all configurations
    conf.set("spark.jars.packages", ",".join(spark_jars_packages))
    conf.set("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
    conf.set("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    conf.set("spark.driver.memory", driver_memory)
    conf.set("spark.executor.memory", executor_memory)
    conf.set("spark.driver.maxResultsSize", "0")
    conf.set("spark.sql.warehouse.dir", warehouse_location)
    conf.set("spark.jars.ivy", ivy_location)

    # Set additional configurations from spark_config parameter
    for key, value in spark_config.items():
        conf.set(key, value)

    # Create SparkSession with the configuration
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark
