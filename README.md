- [Say Hello to PySpark](#say-hello-to-pyspark)
  - [Key Features:](#key-features)
  - [What You'll Find:](#what-youll-find)
  - [1. Get to Know with Data](#1-get-to-know-with-data)
  - [2. Use Databricks as our Tool](#2-use-databricks-as-our-tool)
  - [2. Starting PySpark](#2-starting-pyspark)
    - [Import all necessary libraries](#import-all-necessary-libraries)
    - [create a dictionary for our file location](#create-a-dictionary-for-our-file-location)
    - [Take a look at our data](#take-a-look-at-our-data)
    - [Specify our Data Schema](#specify-our-data-schema)
    - [Create PySpark DataFrame for our data](#create-pyspark-dataframe-for-our-data)
    - [Answer Questions using PySpark](#answer-questions-using-pyspark)
      - [1. How much does each customer spend in the restaurant?](#1-how-much-does-each-customer-spend-in-the-restaurant)
      - [2. How many days has each client visited the restaurant](#2-how-many-days-has-each-client-visited-the-restaurant)
      - [3. Which was the first item each client bought?](#3-which-was-the-first-item-each-client-bought)
      - [4. Which was the most popular plate on the menu? How many times did the customer buy it?](#4-which-was-the-most-popular-plate-on-the-menu-how-many-times-did-the-customer-buy-it)
      - [5. Which was the most preferred plate by each customer?](#5-which-was-the-most-preferred-plate-by-each-customer)
      - [6. What was the first item after the client became a member?](#6-what-was-the-first-item-after-the-client-became-a-member)
      - [7. What was the item before the client became a member?](#7-what-was-the-item-before-the-client-became-a-member)
      - [8. How much money and articles did a customer buy before becoming a member?](#8-how-much-money-and-articles-did-a-customer-buy-before-becoming-a-member)
      - [9. If each $1 is worth 10 points and when is sushi you get double points? How many points each customer has?](#9-if-each-1-is-worth-10-points-and-when-is-sushi-you-get-double-points-how-many-points-each-customer-has)
      - [10. On the first week after a client is a member (including the join date), it earns double points, not only on Sushi. How many points will customers A and B have at the end of January?](#10-on-the-first-week-after-a-client-is-a-member-including-the-join-date-it-earns-double-points-not-only-on-sushi-how-many-points-will-customers-a-and-b-have-at-the-end-of-january)


***
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

```
%fs ls /FileStore/tables/first_project
```

## 2. Starting PySpark

### Import all necessary libraries 
```
# import sql types to define our schemas
from pyspark.sql.types import *

# import all SQL functions as F to clean and transform data
import pyspark.sql.functions as F

# to do windows functions
from pyspark.sql.window import Window

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
```

### create a dictionary for our file location
```
files = {
    'menu': '/FileStore/tables/first_project/menu.csv',
    'members': '/FileStore/tables/first_project/members.csv',
    'sales': '/FileStore/tables/first_project/sales.csv'
}
```
### Take a look at our data
```
df_menu = spark.read.csv(files['menu'], header=True, inferSchema=True)
display(df_menu)
```
```
df_sales = spark.read.csv(files['sales'], header=True, inferSchema=True)
display(df_sales)
```
```
df_members = spark.read.csv(files['members'], header=True, inferSchema=True)
display(df_members)
```
- header=True indicates that the first row in the data contains the header, which will be used as column names.
- inferSchema=True instructs PySpark to automatically infer the data types of each column.

### Specify our Data Schema
If you don't specify a schema, PySpark will attempt to infer it dynamically based on the data in the source, which is convenient but may result in longer job startup times.
- We use 'StructType' to specify the overall structure of our DataFrame by defining a list of 'StructField' objects
- 'StructField' is used to define a single field (column) within a 'StructType'
- Each 'StructField' contains three main parameters: column name, and data type. If we are concerned about performance and data quality, better if we specify our table schema.

```
# Define the proper schema
menu_schema = StructType([
    StructField('product_id', IntegerType(), False),
    StructField('product_name', StringType(), False),
    StructField('price', IntegerType(), False)
])

sales_schema = StructType ([
    StructField('customer_id', StringType(), False),
    StructField('order_date', DateType(), False),
    StructField('product_id', IntegerType(), False)
])

members_schema = StructType ([
    StructField('customer_id', StringType(), False),
    StructField('join_date', DateType(), False)
])
```

### Create PySpark DataFrame for our data
```
menu_df = (
    spark
    .read.format('csv')
    .options(header=True)
    .schema(menu_schema)
    .load(files['menu'])
    )
display(menu_df)
```
```
sales_df = (
    spark
    .read.format('csv')
    .options(header=True)
    .schema(sales_schema)
    .load(files['sales'])
    )
display(sales_df)
```
```
members_df = (
    spark
    .read.format('csv')
    .options(header=True)
    .schema(members_schema)
    .load(files['members'])
    )
display(members_df)
```

### Answer Questions using PySpark
#### 1. How much does each customer spend in the restaurant?
To answer this question, we need to join sales df and menu df first and then we calculate total sales by summing the values based on the price and then grouping by customer_id
```
Q1= (
    sales_df
    .join(
        menu_df,
        sales_df.product_id == menu_df.product_id,
        "inner")
    .groupBy('customer_id')
    .agg(
        F.sum('price').alias('Total Spend'))
    .orderBy(F.col('Total Spend').desc())
)
display(Q1)
```
Now you can see each customer with their total spend. If you are using Databricks, you can use the `display(var)` function to visualize the data. Alternatively, you can convert the data to a pandas DataFrame using the `toPandas()` method and visualize it sns or matplotlib.
```
Q1_pandas = Q1.toPandas()
sns.set('notebook')
sns.set_style('white')
fig, ax = plt.subplots()

sns.barplot(
    data=Q1_pandas,
    x='customer_id',
    y='Total Spend',
    ax=ax
)

fig.suptitle('Total Spend by each customer', fontsize=20)
plt.xlabel('Customer', fontsize=18),
plt.ylabel('Total Spend', fontsize=16)

plt.show
```
```
import matplotlib.pyplot as plt
import seaborn as sns

customer_ids = Q1_pandas['customer_id']
total_spend = Q1_pandas['Total Spend']

# Set the color palette
colors = sns.color_palette('pastel')[0:len(customer_ids)]

# Create a pie chart
plt.figure(figsize=(8, 8))
plt.title('Total Spending Distribution by Customer')
plt.pie(total_spend, labels=customer_ids, autopct='%1.1f%%', colors=colors, startangle=90)
plt.show()

```

#### 2. How many days has each client visited the restaurant
To address this question, we can concentrate on the sales table. By grouping customer IDs and counting distinct order dates, we can obtain the necessary information.
```
Q2 = (
    sales_df
    .groupBy('customer_id')
    .agg(
        F.countDistinct('order_date').alias('Days')
    )
    .orderBy(F.col('Days').desc())
)
display(Q2)
```

#### 3. Which was the first item each client bought?
To address this question, we can join the sales and menu tables, create a new column to establish a new index based on customer_id, and then arrange the data by order date. Subsequently, we can retrieve the required information by filtering rows where the row number is equal to 1 and selecting only the desired columns.
```
Q3 = (
    sales_df
    .join(
        menu_df,
        sales_df.product_id == menu_df.product_id,
        "inner"
    )

    # get the row_number using partition by customer, ordering by order_Date
    .withColumn(
        'row_number', F.row_number().over(
            Window
            .partitionBy('customer_id')
            .orderBy('order_date')
        )
    )
    # only return the data we need
    .filter('row_number==1')
    .select('customer_id', 'order_date', 'product_name')

    #rename the column name so it will more understandable
    .withColumnRenamed('product_name', 'first item')
    .withColumnRenamed('order_date', 'date of first order')
)
display(Q3)
```

#### 4. Which was the most popular plate on the menu? How many times did the customer buy it?
Looks like we need to look at the sales table. But don't forget to join the sales table with the menu table first to get information about the product name. Subsequently, we can group the table by product_name, count occurrences of each product_name, and then arrange the results in descending order based on total sales. Finally, we can limit the output to just one result.
```
Q4 = (
    sales_df
    .join(
        menu_df,
        sales_df.product_id == menu_df.product_id,
        'inner'
    )
    .groupBy('product_name')
    .agg(
        F.count('product_name').alias('Total Sales')
    )
    .orderBy(F.col('Total Sales').desc())
    .limit(1)
)

display(Q4)
```

#### 5. Which was the most preferred plate by each customer?
Looks like we need to make a new column called rank using partition based on customer_id and order it by total sales. Don't forget to filter only the most preferred plate
```
Q5 = (
    sales_df
    .join(
        menu_df,
        sales_df.product_id == menu_df.product_id,
        'inner'
    )
    .groupBy('customer_id','product_name')
    .agg(
        F.count('product_name').alias('Total Sales')
    )

    #get rank number using partition by customer id and order by total sales 
    .withColumn('plate_rank', F.rank().over(
        Window
        .partitionBy('customer_id')
        .orderBy(F.col('Total Sales').desc())
    ))
    #.orderBy(F.col('plate_rank').asc())
    .filter('plate_rank==1')
)

display(Q5)
```

#### 6. What was the first item after the client became a member?
We need to join members table with the sales table with some condition which is when their customer_id is the same and when members_df.join_date is less or equal to sales_df.order_date. After that, we can make the column rank like before


```
Q6 = (
    members_df
    .join(
        sales_df,
        (members_df.customer_id == sales_df.customer_id)
            & (members_df.join_date <= sales_df.order_date)
    ).join (
        menu_df,
        "product_id"
    )
    .withColumn('order_rank', F.rank().over(
        Window
        .partitionBy(members_df.customer_id)
        .orderBy(F.col('order_date'))
    ))
    .filter('order_rank==1')
    .select([sales_df.customer_id, "join_date", "order_date", "product_name"] )
    
)

Q6.show()
```

#### 7. What was the item before the client became a member?
This question is similar to before, try to understand it alone

```
Q7 = (
    members_df
    .join(
        sales_df,
        (members_df.customer_id == sales_df.customer_id)
            & (members_df.join_date > sales_df.order_date)
    ).join (
        menu_df,
        "product_id"
    )
    .withColumn('order_rank', F.rank().over(
        Window
        .partitionBy(members_df.customer_id)
        .orderBy(F.col('order_date').desc())
    ))
    .filter('order_rank==1')
    .select([sales_df.customer_id, "join_date", "order_date", "product_name"] )
    
)

Q7.show()
```

#### 8. How much money and articles did a customer buy before becoming a member?
This question is similar, but you need to group based on customer_id and count the sum of the price

```
Q8 = (
    members_df
    .join(
        sales_df,
        (members_df.customer_id == sales_df.customer_id)
            & (members_df.join_date > sales_df.order_date)
    ).join (
        menu_df,
        "product_id"
    )
    .withColumn('order_rank', F.rank().over(
        Window
        .partitionBy(members_df.customer_id)
        .orderBy(F.col('order_date').desc())
    ))
    .groupBy (
        members_df.customer_id
    )
    .agg (
        F.sum('price')
    )
    
)

display(Q8)
```


#### 9. If each $1 is worth 10 points and when is sushi you get double points? How many points each customer has?
To conquer this question, we can join our sales table with the menu. After that, we can create a new column called "points" with the product name sushi to get double points. Finally, we can group data by customer_id, sum the points table and order by points column.

```
Q9 = (
    sales_df
    .join(
        menu_df,
        "product_id"
    )
    .withColumn(
        "points",
        F.when(menu_df.product_name == 'sushi', (menu_df.price * 10 * 2)).otherwise(menu_df.price*10)
    )
    .groupBy(
        "customer_id"
    )
    .agg(
        F.sum("points").alias("Total points")
    )
    .orderBy(
        F.col("Total points").desc())
)

display(Q9)
```

#### 10. On the first week after a client is a member (including the join date), it earns double points, not only on Sushi. How many points will customers A and B have at the end of January?

This question is a bit challenging because there will be a few conditions. You can reimplement the code for question 9 and add a few conditions
```
Q10 = (
    sales_df
    .join(
        menu_df,
        "product_id"
    )
    .join(
        members_df,
        "customer_id"
    )
    .withColumn(
        "points",
        F.when( (sales_df.order_date >= members_df.join_date) & (sales_df.order_date <= members_df.join_date + 7), menu_df.price*10*2)
        .when(((sales_df.order_date < members_df.join_date) | (sales_df.order_date > members_df.join_date +7)) & (menu_df.product_name == 'sushi'), menu_df.price*10*2 )
        .otherwise(menu_df.price*10)
    )
    .filter(sales_df.order_date < '2021-02-01')
    .groupBy(
          "customer_id"
      )
      .agg(
          F.sum("points").alias("Total points")
      )
      .orderBy(
          F.col("Total points").desc())
)

Q10.show()
```