# Databricks notebook source
data_path = "aws_prod.default.employee_salary"

df = spark.read.table(data_path)

# Display the ingested data
display(df)

# COMMAND ----------

# Remove null values and incorrect values
df_cleaned = df.dropna().filter(df.salary > 0)

display(df_cleaned)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as fx


# COMMAND ----------

window_func = Window.partitionBy("employee_id").orderBy("effective_date")

#using lag function to add old salary
df_transform = df_cleaned.withColumn("previous_salary", fx.lag("salary").over(window_func))

#salary change 
df_transform = df_transform.withColumn("salary_change", fx.col("salary") - fx.col("previous_salary"))

# Compute running total of salaries
df_transform = df_transform.withColumn("running_total", fx.sum("salary").over(window_func))

display(df_transform)


# COMMAND ----------

df_max_salary = df_transform.groupBy("employee_id").agg(fx.max("salary").alias("max_salary"))

# Creating a window for percentile rank cal based on max_salary
percentile_window = Window.orderBy(fx.col("max_salary"))

# Calculating salary percent rank
df_salary_band = df_max_salary.withColumn("salary_percent_rank", fx.percent_rank().over(percentile_window))

# Classify salary bands with %
df_salary_band = df_salary_band.withColumn("salary_band",fx.when(fx.col("salary_percent_rank") < 0.25, "Low").when((fx.col("salary_percent_rank") >= 0.25) & (fx.col("salary_percent_rank") < 0.75), "Medium").otherwise("High"))


display(df_salary_band)

# COMMAND ----------

#give path 
output_path = "/Workspace/Users/regattesiddartha@gmail.com"

#asc order
df_salary_band_sort = df_salary_band.orderBy("employee_id")

# Write sorted data into Delta table
df_salary_band_sort.write.format("delta").mode("overwrite").save(output_path)

# Verify  Delta table
df_delta_sort= spark.read.format("delta").load(output_path).orderBy("employee_id")
display(df_delta_sort)