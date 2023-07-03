import spark

# Create a Spark session
spark = spark.sql.SparkSession.builder.appName("Extract Data").getOrCreate()

# Read the CSV file into a Spark DataFrame
df = spark.read.csv("customers.csv")

# Display the first few rows of the DataFrame
df.show(5)

# Remove duplicate rows
df = df.dropDuplicates()

# Convert the Order History column to a JSON format
df = df.withColumn("Order History", to_json(df["Order History"]))

# Split the Order History column into a separate DataFrame for each order
order_df = df.select("Customer ID", "Order History").rdd.flatMap(lambda x: json.loads(x["Order History"]).items())

# Remove any orders that do not have any products
order_df = order_df.filter(lambda x: x[1] != [])

# Remove any products that are not in stock
order_df = order_df.filter(lambda x: x[1]["In Stock"] == True)

# Calculate the total order amount for each order
order_df = order_df.withColumn("Total Order Amount", x["1"]["Order Total"])

import mysql.connector

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="customers"
)

# Write the DataFrame to the MySQL database
order_df.write.jdbc(
    url="jdbc:mysql://localhost:3306/customers",
    table="orders",
    mode="overwrite"
)