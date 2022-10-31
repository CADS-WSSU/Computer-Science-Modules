

from pyspark.sql.session import SparkSession
df = spark.read.json("employee.json")
    # Displays the content of the DataFrame to stdout
df.show()