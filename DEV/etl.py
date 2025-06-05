from pyspark.sql import SparkSession
import pandas as pd
import requests
import mysql.connector
from config import MYSQL_USER, MYSQL_PASSWORD

# Initialize Spark session
spark = SparkSession.builder.appName("CreditCardCapstone").getOrCreate()

# MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
connection_properties = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Create database
conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host="localhost")
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
conn.close()

# Req 1.1 & 1.2: Load Credit Card JSON data
def load_credit_card_data():
    # Read JSON files
    branch_df = spark.read.json("data/CDW_SAPP_BRANCH.JSON")
    credit_card_df = spark.read.json("data/CDW_SAPP_CREDITCARD.JSON")
    customer_df = spark.read.json("data/CDW_SAPP_CUSTOMER.JSON")

    # Transformations (example: per Mapping Document)
    customer_df = customer_df.withColumnRenamed("SSN", "CUST_SSN") \
                            .na.fill({"MIDDLE_NAME": ""})
    branch_df = branch_df.na.fill({"BRANCH_PHONE": "Unknown"})
    credit_card_df = credit_card_df.withColumnRenamed("TRANSACTION_DATE", "TIMEID")

    # Write to MySQL
    customer_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=connection_properties)
    branch_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_BRANCH", mode="overwrite", properties=connection_properties)
    credit_card_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="overwrite", properties=connection_properties)
    print("Credit Card data loaded successfully.")

# Req 4.1, 4.2, 4.3: Load Loan Application data
def load_loan_data():
    url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    response = requests.get(url)
    print(f"Loan API Status Code: {response.status_code}")

    if response.status_code == 200:
        loan_data = response.json()
        loan_df = spark.createDataFrame(pd.DataFrame(loan_data))
        loan_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_loan_application", mode="overwrite", properties=connection_properties)
        print("Loan data loaded successfully.")
    else:
        print("Failed to fetch Loan data.")

if __name__ == "__main__":
    load_credit_card_data()
    load_loan_data()
    spark.stop()