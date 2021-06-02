from pyspark.sql.functions import desc
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import os


class QuestionAnswerer():
    """
    A class used to answer the queries using Spark on a dataframe containing
    information about transactions


    Attributes
    ----------
    df : pyspark.sql.DataFrame
        a dataframe on which the queries are executed
    save_dir: str
        a path to a directory where the charts from some of executed queries
        are saved

    Methods
    -------
    group_by_invoice()
        groups transactions by invoice

    the_most_sold_product()
        returns the StockCode of the most sold product

    customer_spent_the_most()
        returns the CustomerID of the customer who spent the most money

    product_country_distribution(product, by_column, save=False, show=True)
        for a given product returns the list of countries and the quantity of
        the product sold in the respective country, display and/or saves the
        pie chart with this information

    price_distribution(by_column=None, name=None, save=False, show=True)
        returns price distribution in the form of list of prices and list of
        their respective quantity, display and/or saves the chart with
        this information

    avg_unit_price(self, by_column)
        returns the average price of the unit

    price_quantity_ratio(invoice_no, save=False, show=True)
        returns tje price quantity ratio for a given invoice in a form of list
        of prices and their quantities, display and/or saves the chart with
        this information
    """

    def __init__(self, df, save_dir=None):
        """
        Parameters
        ----------
        df : pyspark.sql.DataFrame
            a dataframe on which the queries are executed
        save_dir: str, optional
            a path to a directory where the charts from some of
            executed queries are saved
        """

        plt.ioff()
        self.df = df
        self.save_dir = save_dir
        if save_dir is not None:
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)

    def group_by_invoice(self):
        """
        Groups transactions by InvoiceNo, returns GroupedData
        """
        return self.df.groupby('InvoiceNo')

    def the_most_sold_product(self):
        """
        Returns the most sold product StockCode, meaning the highest number of
        units among the trasactions
        """

        sorted_by_tq_df = self.df.groupby('StockCode')\
                                 .sum('Quantity')\
                                 .withColumnRenamed('sum(Quantity)',
                                                    'TotalQuantity')\
                                 .sort(desc('TotalQuantity'))
        max_tq = sorted_by_tq_df.collect()[0]['TotalQuantity']
        all_the_max_tq_df = sorted_by_tq_df.filter(
                                            col('TotalQuantity') == max_tq)
        answer = all_the_max_tq_df.select("StockCode").rdd\
            .flatMap(lambda x: x).collect()
        return answer

    def customer_spent_the_most(self):
        """
        Returns the CustomerID who spent the most money, meaning for all the
        transaction the highest number of sum(UnitPrice * Quantity)
        """
        sorted_by_tms_df = self.df.where(col('CustomerID').isNotNull())\
                                  .withColumn('MoneySpent',
                                              self.df['UnitPrice']
                                              * self.df['Quantity'])\
                                  .groupby('CustomerID')\
                                  .sum('MoneySpent')\
                                  .withColumnRenamed('sum(MoneySpent)',
                                                     'TotalMoneySpent')\
                                  .sort(desc('sum(MoneySpent)'))
        max_tms = sorted_by_tms_df.collect()[0]['TotalMoneySpent']
        all_the_max_tms_df = \
            sorted_by_tms_df.filter(col('TotalMoneySpent') == max_tms)
        answer = all_the_max_tms_df.select("CustomerID").rdd\
            .flatMap(lambda x: x).collect()
        return answer

    def product_by_country_distribution(self, product, by_column,
                                        save=False, show=False):
        """
        For a given product returns the list of countries and the quantity of
        the product sold in the respective country, display and/or saves the
        pie chart with this information

        Parameters
        ----------
        by_column : str
            which column to use for identifying the product, the available
            options are 'Description' and 'StockCode'
        product : str
            the Description or StockCode of the product
        save : bool
            determines if the generated chart is saved
        show : bool
            determines if the generated chart is shown
        """
        if by_column not in ['Description', 'StockCode']:
            raise Exception('Impossible to filter by {}'.format(by_column))
        df_country_desc = self.df.filter(col(by_column).isNotNull())\
                                 .filter(col('Country') != 'Unspecified')\
                                 .groupby(by_column, 'Country')\
                                 .sum('Quantity')\
                                 .withColumnRenamed('sum(Quantity)',
                                                    'TotalQuantity')
        filtered_df = df_country_desc.filter(col(by_column) == product)\
                                     .sort(col('TotalQuantity'))
        country_collect = filtered_df.select('Country').collect()
        quantity_collect = filtered_df.select('TotalQuantity').collect()
        country_list = [row['Country'] for row in country_collect]
        quantity_list = [row['TotalQuantity'] for row in quantity_collect]
        plt.figure(figsize=(10, 10))
        patches, texts = plt.pie(quantity_list, shadow=True, startangle=90)
        plt.title('Product by Country Distribution for {} {}'
                  .format(by_column, product))
        plt.legend(patches, country_list, loc="best")
        if show:
            plt.show()
        if save:
            plt.savefig(os.path.join(self.save_dir,
                                     'product_by_country_distribution_'
                                     + str(product) + '.png'))
        return country_list, quantity_list

    def price_distribution(self, by_column=None, name=None,
                           save=False, show=True):
        """
        Returns price distribution in the form of list of prices and list of
        their respective quantity, display and/or saves the pie chart with
        this information

        Parameters
        ----------
        by_column : str
            which column to use for identifying the product, the available
            options are 'Description', 'Country', 'InvoiceNo' and 'StockCode'
        name : str
            the name which is searched for a by_column in dataframe
        save : bool
            determines if the generated chart is saved
        show : bool
            determines if the generated chart is shown
        """
        df_price_count = self.df
        if by_column is not None:
            if by_column not in ['Description', 'StockCode',
                                 'InvoiceNo', 'Country']:
                raise Exception('Impossible to filter by {}'.format(by_column))
            else:
                df_price_count = self.df.filter(col(by_column) == name)
        df_price_count = df_price_count.groupby('UnitPrice')\
                                       .count()\
                                       .withColumnRenamed('count',
                                                          'TotalCount')\
                                       .sort('UnitPrice')
        price_list = [row.UnitPrice for row in
                      df_price_count.select('UnitPrice').collect()]
        count_list = [row.TotalCount for row in
                      df_price_count.select('TotalCount').collect()]
        plt.figure(figsize=(10, 10))
        plt.title('Price Distribution' +
                  (by_column is not None) * ' for {} {}'
                  .format(by_column, name))
        plt.hist(price_list, weights=count_list)
        if show:
            plt.show()
        if save:
            plt.savefig(os.path.join(self.save_dir,
                                     'price_distribution_ '
                                     + str(by_column) + '.png'))
        return price_list, count_list

    def avg_unit_price(self, by_column):
        """
        Returns the average price of the unit
        Parameters
        ----------
        by_column : str
            which column to use for identifying the product, the available
            options are 'Description' and 'StockCode'
        """
        if by_column not in ['Description', 'StockCode']:
            raise Exception('Impossible to filter by {}'.format(by_column))
        grp_by_column = self.df.filter(col(by_column).isNotNull())\
                               .withColumn('MoneySpent',
                                           self.df['UnitPrice'] *
                                           self.df['Quantity'])\
                               .groupby(by_column)\
                               .sum('Quantity', 'MoneySpent')\
                               .withColumnRenamed('sum(Quantity)',
                                                  'TotalQuantity')\
                               .withColumnRenamed('sum(MoneySpent)',
                                                  'TotalMoneySpent')\

        answer = grp_by_column.withColumn('AverageUnitPrice',
                                          grp_by_column['TotalMoneySpent'] /
                                          grp_by_column['TotalQuantity'])
        return answer

    def price_quantity_ratio(self, invoice_no, save=False, show=True):
        """
        Returns the price quantity ratio for a given invoice in a form of list
        of prices and their quantities, display and/or saves the chart with
        this information
        Parameters
        ----------
        invoice_no : int
            InvoiceNo for which the information is given
        save : bool
            determines if the generated chart is saved
        show : bool
            determines if the generated chart is shown
        """
        invoice_df = self.df.filter(col('InvoiceNo') == invoice_no)\
            .groupby('UnitPrice')\
            .sum('Quantity')\
            .withColumnRenamed('sum(Quantity)', 'TotalQuantity')\
            .sort('UnitPrice')
        price_list = [row['UnitPrice'] for row in
                      invoice_df.select('UnitPrice').collect()]
        quantity_list = [row['TotalQuantity'] for row in
                         invoice_df.select('TotalQuantity').collect()]
        plt.figure(figsize=(10, 10))
        plt.title('Price-Quantity Ratio for InvoiceNo {}'.format(invoice_no))
        plt.xlabel('Price')
        plt.ylabel('Quantity')
        plt.scatter(price_list, quantity_list)
        if show:
            plt.show()
        if save:
            plt.savefig(os.path.join(self.save_dir,
                                     'price_quantity_ratio '
                                     + str(invoice_no) + '.png'))
        return price_list, quantity_list
