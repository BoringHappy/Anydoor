from typing import Any, List


def init_spark(
    driver_memory: str = "4g",
    executor_memory: str = "4g",
    warehouse_location: str = "/data/lake",
    ivy_location: str = "/data/lake/.ivy",
    spark_jars_packages: List[str] = [
        "io.delta:delta-spark_2.12:3.2.1",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.0",
    ],
) -> Any:
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    spark = (
        SparkSession.builder.config(
            "spark.jars.packages", ",".join(spark_jars_packages)
        )
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultsSize", "0")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.jars.ivy", ivy_location)
        .getOrCreate()
    )
    return spark
