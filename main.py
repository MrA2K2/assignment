import pandas as pd
# Due to specifics of installation on my machine
# these lines need to be executed before importing Spark
# Feel free to comment them out or delete them
#################
import findspark
findspark.init()
#################
from pyspark.sql import SparkSession
from src.spark_queries import QuestionAnswerer
from src.mongoimport import MongoImportDF
from pymongo import MongoClient

mongo_uri = "mongodb://localhost:27017/"
collection_name = "final_collection"
db_name = "final_db"
mongodb_input_uri = "mongodb://127.0.0.1/" + db_name + '.' + collection_name
mongodb_output_uri = "mongodb://127.0.0.1/" + db_name + '.' + collection_name
xslx_name = 'Online Retail.xlsx'
save_directory = 'Output'

# Utility functions used to generate interactive interface
# for each of the questions


def message():
    print('Press question number, \'l\' to list questions or \'q\' to quit\n')


def list_question():
    print('Question 1: Group all transactions by invoice \n')
    print('Question 2: Which product sold the most? \n')
    print('Question 3: Which customer spent the most money? \n')
    print('Question 4: A chart showing the distribution of each product\
          for each of the available countries')
    print('Question 5: What is the average unit price?')
    print('Question 6: A chart showing the distribution of prices')
    print('Question 7: The ratio between price and quantity for each invoice')


def question_1(qa):
    print('Question 1: Group all transactions by invoice \n')
    print('Cant display grouped data :(')
    print(qa.group_by_invoice())


def question_2(qa):
    print('Question 2: Which product sold the most? \n')
    print('The product(s) with StockCode(s) {} sold the most'
          .format(qa.the_most_sold_product()))


def question_3(qa):
    print('Question 3: Which customer spent the most money? \n')
    print('The customer(s) with CustomerID(s) {} spent the most money'
          .format(qa.customer_spent_the_most))


def question_4(qa, save=False, show=False):
    print('Question 4: A chart showing the distribution of each product\
        for each of the available countries')
    print('Description (d) or StockCode? (s)\n')
    answer = input()
    if answer not in ['d', 's']:
        raise Exception('Invalid Answer!\n')
    if answer == 'd':
        column = 'Description'
    else:
        column = 'StockCode'
    print('Input {}\n'.format(column))
    product = input()
    print('Save(s), show(w) the chart or both (sw)?\n')
    answer = input()
    if 's' in answer:
        save = True
    if 'w' in answer:
        show = True
    qa.product_by_country_distribution(product, column, save, show)
    print('Done')


def question_5(qa):
    print('Question 5: What is the average unit price?')
    print('Description (d) or StockCode? (s)\n')
    answer = input()
    if answer not in ['d', 's']:
        raise Exception('Invalid Answer!\n')
    if answer == 'd':
        column = 'Description'
    else:
        column = 'StockCode'
    qa.avg_unit_price(column).show()


def question_6(qa, save=False, show=False):
    print('Question 6: A chart showing the distribution of prices')
    column = input()
    print('Description, InvoiceNo, Country or StockCode?\n')
    print('Input {}\n'.format(column))
    name = input()
    print('Save(s), show(w) the chart or both (sw)?\n')
    answer = input()
    if 's' in answer:
        save = True
    if 'w' in answer:
        show = True
    qa.price_distribution(column, name, save, show)


def question_7(qa, save=False, show=False):
    print('Question 7: The ratio between price and \
            quantity for each invoice')
    print('Input InvoiceNo\n')
    invoice_no = int(input())
    print('Save(s), show(w) the chart or both (sw)?\n')
    answer = input()
    if 's' in answer:
        save = True
    if 'w' in answer:
        show = True
    qa.price_quantity_ratio(invoice_no, save, show)


if __name__ == "__main__":
    #The MongoDB import part starts here
    print('Connecting to MongoDB...\n')
    my_client = MongoClient(mongo_uri)
    print('Connected\n')
    print('Reading xslx...')
    df = pd.read_excel(xslx_name, engine='openpyxl')
    print('Read!')
    midf = MongoImportDF(df, my_client, db_name, collection_name)
    print('Started import of dataframe into MongoDB...')
    midf.mongo_import()
    print('Import finished, database "{}", collection "{}"\n'
          .format(db_name, collection_name))
    # The MongoDB import part starts here
    # The Spark Part starts here
    print('Creating Spark Session...\n')
    my_spark = SparkSession \
        .builder \
        .appName("OnlineRetail") \
        .config("spark.mongodb.input.uri",
                mongodb_input_uri) \
        .config("spark.mongodb.output.uri",
                mongodb_output_uri) \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
        .getOrCreate()
    print('Spark session created\n')
    print('Creating Spark dataframe from MongoDB...\n')
    spark_df = my_spark.read.format("mongo").load()
    print('Spark dataframe created\n')
    qa = QuestionAnswerer(spark_df)
    # The Spark Part ends here. The queries can be executed by directly calling
    # the methods of QuestionAnswerer
    command = 'l'

    switcher = {
        '1': question_1,
        '2': question_2,
        '3': question_3,
        '4': question_4,
        '5': question_5,
        '6': question_6,
        '7': question_7
    }

    while command != 'q':
        message()
        command = input()
        if command == 'l':
            list_question()
        else:
            switcher.get(command, 'Wrong Input!')(qa)
