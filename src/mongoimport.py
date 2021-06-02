from tqdm import tqdm
import numpy as np
# This line is used only to have the ability to see the progress of
# import to MongoDB
tqdm.pandas()



class MongoImportDF():
    """
    A class used to import a dataframe into MongoDB with some cleaning before
    Attributes
    ----------
    df : pandas.DataFrame
        a dataframe to import
    mongo_client: pymongo.mongo_client.MongoClient
        a client used to connect to MongoDB
    db_name : the name of database where the dataframe is going to be imported
    collection_name : the name of collection where the dataframe is going to be imported

    Methods
    -------
    row_to_dict(row)
        converts the dataframe row to dictionary modifying some of the entries,
        such as deleting the missing values in CustomerID and Description, and
        converting the non-string entries in Description to string

    mongo_import()
        Removes the duplicates from dataframe and imports each row as a document to
        defined database collection previously modifying the with row_to_dict
    """
    def __init__(self, df, mongo_client, db_name, collection_name):
        self.df = df
        self.client = mongo_client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def row_to_dict(self, row):
        row_dict = dict(row)
        if np.isnan(row_dict['CustomerID']):
            del row_dict['CustomerID']
        else:
            row_dict['CustomerID'] = int(row_dict['CustomerID'])
        if row_dict['Description'] == row_dict['Description']:
            row_dict['Description'] = str(row_dict['Description']).upper()
        else:
            del row_dict['Description']
        return row_dict

    def mongo_import(self):
        self.df = self.df.drop_duplicates()
        self.df.progress_apply(
            lambda x: self.collection.insert_one(self.row_to_dict(x)), axis=1)
        print('Import Finished')
