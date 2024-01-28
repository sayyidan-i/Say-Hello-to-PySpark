# Say Hello to PySpark
Welcome to "Say Hello to PySpark," your go-to resource for diving into the world of PySpark and making sense of sales data! Whether you're a beginner or looking to brush up on your skills, this repository guides you through the basics of PySpark and demonstrates how to analyze simple sales data.

## Key Features:

- Getting Started: Jumpstart your PySpark journey with easy-to-follow tutorials that walk you through the setup and essentials.
- Hands-On Examples: Explore practical examples that unravel the power of PySpark in analyzing and interpreting sales data.
- Data Insights: Learn how to leverage PySpark to gain valuable insights from your sales datasets, turning raw numbers into actionable information.

## What You'll Find:

- Step-by-step guides for PySpark installation and configuration using Databrick.
- Simple scripts for sales data analysis.

---
## 1. Get to Know with Data
This project will guide you through gaining insights from data using PySpark, with a focus on the data below.

![Data Diagram](https://github.com/sayyidan-i/Say-Hello-to-PySpark/blob/main/img/say%20hello%20to%20pyspark.png)


## 2. Use Databricks as our Tool
Let's streamline the process by utilizing Databricks as our tool of choice. Installing Spark on a local machine can be complex, so we'll leverage Databricks for a seamless start. This tool comes pre-configured, allowing you to dive into your work immediately

_Step by step to start with Databricks_
- Navigate to Databricks [Community Edition](https://www.databricks.com/try-databricks) and sign up—it's free! Despite limitations, it provides ample resources for our project.
- After entering your information, click, make sure to click `Get Started with Community Edition`

![Register Databricks](https://github.com/sayyidan-i/Say-Hello-to-PySpark/blob/main/img/databricks%20register.png)

![Get Started with Community Edition](https://github.com/sayyidan-i/Say-Hello-to-PySpark/blob/main/img/get%20started%20with%20community%20edition.jpg)

- Your Databricks workspace is now set up and ready to go. Proceed to upload our [data](https://github.com/sayyidan-i/Say-Hello-to-PySpark/tree/main/data) by selecting catalog > create table > write directory that you want, for example `first_project` and drop our data.

- Now our data is saved in `/FileStore/tables/first_project, create a new notebook and run the following command to list them.
'''
%fs ls /FileStore/tables/first_project
'''

## 2. Starting PySpark
You must be wondering
