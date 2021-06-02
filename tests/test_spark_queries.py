import pytest
import numpy as np
import pandas as pd
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from src.spark_queries import QuestionAnswerer
import findspark


@pytest.fixture(scope="session")
def spark_session(request):
    findspark.init()
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


@pytest.fixture
def spark_df(spark_session):
    test_dict = {'Country': ['EU', 'UK', 'EU', 'FRANCE'],
                 'Description': ['F', None, 'F', 'F'],
                 'CustomerID': [0, 2, 1, 0],
                 'InvoiceDate': [pd._libs.tslibs.timestamps.Timestamp(
                                '2010-12-01 08:26:00')] * 4,
                 'InvoiceNo': ['0', '1', '1', 'W'],
                 'Quantity': [10, 1, 1, 11],
                 'StockCode': ['4', '4', '3', '5'],
                 'UnitPrice': [10.5, 50.0, 149.0, 4.0]}
    test_df = pd.DataFrame(test_dict)
    test_spark_df = spark_session.createDataFrame(test_df)
    return test_spark_df


@pytest.fixture()
def question_answerer(spark_df):
    test_question_answerer = QuestionAnswerer(spark_df)
    return test_question_answerer


@pytest.mark.parametrize("invoice_no, expected_output", [
    ('0', 1),
    ('1', 2),
    ('W', 1),
])


def test_group_by_invoice(question_answerer, invoice_no, expected_output):
    n = question_answerer.group_by_invoice().count().filter(col('InvoiceNo') == invoice_no).collect()[0][1]
    assert(n == expected_output)


def test_the_most_sold_product(question_answerer):
    answer = question_answerer.the_most_sold_product()
    assert(set(answer) == set(['4', '5']))


def test_customer_spent_the_most(question_answerer):
    answer = question_answerer.customer_spent_the_most()
    assert(set(answer) == set([0, 1]))


@pytest.mark.parametrize("product, countries, quantities, column_name", [
    ('F', ['EU', 'FRANCE'], [11, 11], 'Description'),
    ('4', ['UK', 'EU'], [1, 10], 'StockCode'),
])


def test_product_by_country_distribution(question_answerer, product, countries,
                                         quantities, column_name):
    country_list, quantity_list = question_answerer.product_by_country_distribution(product, column_name)
    assert((country_list, quantity_list) == (countries, quantities))


@pytest.mark.parametrize("name, by_column, price_list, count_list", [
    (None, None, [4.0, 10.5, 50.0, 149.0], [1, 1, 1, 1]),
    ('EU', 'Country', [10.5, 149.0], [1, 1]),
])


def test_price_distribution(question_answerer, name, by_column, price_list, count_list):
    p_list, c_list = question_answerer.price_distribution(by_column, name)
    assert((p_list, c_list) == (price_list, count_list))

@pytest.mark.parametrize("column_name, product, avg_price", [
    ('StockCode', '4', (10.5 * 10 + 1 * 50.0) /11),
    ('Description', 'F', (10.5 * 10 + 1 * 149.0 + 11 * 4.0) /22),
])


def test_avg_unit_price(question_answerer, column_name, product, avg_price):
    answer = question_answerer.avg_unit_price(column_name)\
                .filter(col(column_name) == product)\
                .collect()[0]['AverageUnitPrice']
    assert(answer == avg_price)

@pytest.mark.parametrize("invoice_no, price_list, quantity_list", [
    ('1', [50.0, 149.0], [1, 1]),
    ('0', [10.5], [10]),
])


def test_price_quantity_ration(question_answerer, invoice_no, price_list, quantity_list):
    p_list, q_list = question_answerer.price_quantity_ratio(invoice_no)
    assert((p_list, q_list) == (price_list, quantity_list))
