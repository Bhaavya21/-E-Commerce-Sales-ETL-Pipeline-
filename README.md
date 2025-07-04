# -E-Commerce-Sales-ETL-Pipeline-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Define file paths
customers_file = "MultipleFiles/customers.csv"
sales_file = "MultipleFiles/sales.csv"
returns_file = "MultipleFiles/returns.csv"

# Read CSV files into DataFrames
customers_df = spark.read.csv(customers_file, header=True, inferSchema=True)
sales_df = spark.read.csv(sales_file, header=True, inferSchema=True)
returns_df = spark.read.csv(returns_file, header=True, inferSchema=True)

# --- Data Cleaning and Joining ---

# 1. Clean 'sales_df': Ensure 'price' and 'quantity' are numeric
sales_df = sales_df.withColumn("price", col("price").cast("float")) \
                   .withColumn("quantity", col("quantity").cast("integer"))

# 2. Join sales with customers on 'customer_id'
# Using a left join to keep all sales records, even if customer info is missing (though unlikely here)
joined_df = sales_df.join(customers_df, on="customer_id", how="left")

# 3. Join the result with returns on 'sale_id'
# Using a left join to include sales that have no corresponding return
final_df = joined_df.join(returns_df, on="sale_id", how="left")

# Select and reorder columns for the final DataFrame to be loaded into MySQL
# Assuming we want all relevant information in a single flat table
mysql_ready_df = final_df.select(
    col("sale_id"),
    col("customer_id"),
    col("name").alias("customer_name"),
    col("email").alias("customer_email"),
    col("region").alias("customer_region"),
    col("product_id"),
    col("quantity"),
    col("price"),
    col("sale_date"),
    col("return_date"),
    col("reason").alias("return_reason")
)

# Show schema and some data to verify
mysql_ready_df.printSchema()
mysql_ready_df.show(5, truncate=False)

# --- Prepare for MySQL Load (Conceptual) ---
# To load into MySQL, you would typically use the JDBC connector.
# This part is conceptual as it requires a running MySQL instance and JDBC driver.

# Example of how you would write to MySQL (uncomment and configure if needed)
# jdbc_url = "jdbc:mysql://localhost:3306/your_database"
# connection_properties = {
#     "user": "your_username",
#     "password": "your_password",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }
#
# mysql_ready_df.write.jdbc(url=jdbc_url, table="sales_returns_data", mode="overwrite", properties=connection_properties)
#
# print("DataFrame prepared and ready for MySQL load.")

# Stop Spark Session
spark.stop()

