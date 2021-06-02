# PySpark Mongo Queries
## General Information
The goal of the project is to import the given __.xlsx__ file into MongoDB and then perform some queries on the data using PySpark and MongoDB Connector. The questions are as follows:

1.  Group all transactions by invoice
1. Which product sold the most?
1. Which customer spent the most money?
1. A chart showing the distribution of each product for each of the available countries 
1. What is the average unit price?
1. Give us a chart showing the distribution of prices.
1. Give us the ratio between price and quantity for each invoice.

The code contains two files, each of them contains a single class which handles the part of the task. 

* mongoimport.py provides a class __MongoImportDF__ which is used to import the panda dataframe into the MongoDB
* spark_queries.py provides a class __QuestionAnswerer__ which is used to perform queries on the Spark Dataframe


## Requirements

The main requirements are MongoDB and Spark with all respective dependencies installed and configured. I worked using conda environment, the __requirements.txt__ is provided. 

## Usage

You can test the functionality by either launching main.py which provides a basic-level interface or just copying the highlighted part of the part in the same file and testing the functionality yourself. The following part of the code needs to be edited to suit your needs
```python
mongo_uri = "mongodb://localhost:27017/"
db_name = "final_db"
collection_name = "final_collection"
mongodb_input_uri = "mongodb://127.0.0.1/" + db_name + '.' + collection_name
mongodb_output_uri = "mongodb://127.0.0.1/" + db_name + '.' + collection_name
xslx_name = 'Online Retail.xlsx'
save_directory = 'Output'
```
* __mongo_uri__ - used to connect to MongoDB
* __db_name__ - name of the database in MongoDB
* __collection_name__ - name of the collection in MongoDB
* __mongodb_input_uri__, __mongodb_output_uri__ - used to create SparkSession
* __xslx_name__  - name of __.xlsx__ file containing dataset
* __save_directory__ - directory used to save charts for some of the queries 
