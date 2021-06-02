import pytest
import pandas as pd
import numpy as np
import mongomock
from src.mongoimport import MongoImportDF


@pytest.fixture
def create_test_df():
    test_dict = {'Country': ['UK', 'EU', 'EU', 'UK', 'FRANCE'],
                 'Description': ['F', 'S', np.nan, 'F', 4],
                 'CustomerID': [0, None, 1, 0.0, 0],
                 'InvoiceDate': [pd._libs.tslibs.timestamps.Timestamp(
                                '2010-12-01 08:26:00')]*5,
                 'InvoiceNo': ['0', 1, 10.0, '0', 'W'],
                 'Quantity': [10, 0, 1, 10, 5],
                 'StockCode': ['4', '4', '3', '4', '5'],
                 'UnitPrice': [10.5, 5.0, 6.0, 10.5, 4.0]}
    test_df = pd.DataFrame(test_dict)
    return test_df


@pytest.fixture
def create_test_midf(create_test_df):
    test_df = create_test_df
    mongo_client = mongomock.MongoClient().db.collection
    db_name = 'test_db'
    collection_name = 'test_collection'
    midf = MongoImportDF(test_df, mongo_client, db_name, collection_name)
    return midf


@pytest.mark.parametrize("row_index, expected_output", [
    (0, {'Country': 'UK', 'Description': 'F',
         'CustomerID': 0,
         'InvoiceDate':
         pd._libs.tslibs.timestamps.Timestamp('2010-12-01 08:26:00'),
         'InvoiceNo': '0', 'Quantity': 10,
         'StockCode': '4', 'UnitPrice': 10.5}),
    (1, {'Country': 'EU', 'Description': 'S',
         'InvoiceDate':
         pd._libs.tslibs.timestamps.Timestamp('2010-12-01 08:26:00'),
         'InvoiceNo': 1, 'Quantity': 0,
         'StockCode': '4', 'UnitPrice': 5.0}),
    (2, {'Country': 'EU', 'CustomerID': 1,
         'InvoiceDate':
         pd._libs.tslibs.timestamps.Timestamp('2010-12-01 08:26:00'),
         'InvoiceNo': 10.0, 'Quantity': 1,
         'StockCode': '3', 'UnitPrice': 6.0}),
    (4, {'Country': 'FRANCE', 'Description': '4',
         'CustomerID': 0,
         'InvoiceDate':
         pd._libs.tslibs.timestamps.Timestamp('2010-12-01 08:26:00'),
         'InvoiceNo': 'W', 'Quantity': 5,
         'StockCode': '5', 'UnitPrice': 4.0}),
])


def test_row_to_dict(create_test_midf, row_index, expected_output):
    midf = create_test_midf
    d = midf.row_to_dict(midf.df.iloc[row_index])
    assert (d == expected_output)


def test_mongo_import(create_test_midf):
    midf = create_test_midf
    midf.mongo_import()
    print(midf.collection.find({'Country': 'UK'}))
    assert (midf.collection.count_documents({}) == 4)
