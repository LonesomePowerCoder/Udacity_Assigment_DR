import numpy as np
import pandas as pd
import re
from sqlalchemy import create_engine

class data_processor():
    """ Data processing class for ETL of desaster response data

    Attributes:
        -
        -
        -

    """
    def __init__(self):
        self.data = []
        self.data_merged = []

    def data_loader(self, path = None):
        """

        :param path:
        :return:
        """
        try:
            self.data = pd.read_csv(path)
        except:
            raise Exception('Only csv files accepted')

        return self.data

    def data_merger(self,messages,categories):
        """

        :param dataset1:
        :param dataset2:
        :return:
        """
        try:
            df_messages = self.data_loader(messages)
            df_categories = self.data_loader(categories)
            self.data_merged = df_messages.merge(df_categories,on = 'id')
        except:
            raise Exception('Cannot merge both files')
        return self.data_merged

    def split_category(self):
        """

        :return:
        """


        return

    def clean_labels(self,df):
        """

        :param data:
        :return:
        """
        categories = df['categories'].str.split(';', expand=True).add_prefix('column')
        categories.columns = categories.iloc[0, :]
        categories.columns = categories.columns.str.replace('-1', '').str.replace('-0', '')
        for column in categories.columns:
            categories[column] = categories[column].str.replace(column+'-','')
            categories[column] = pd.to_numeric(categories[column])
        df_merged = df.join(categories).fillna('na')
        data_cleaned = df_merged.drop(columns=['categories'])

        return data_cleaned

    def setup_sqlite_DB(self,df,db_name,table_name):
        """

        :param df:
        :return:
        """
        path = 'sqlite:///'+ db_name + '.db'
        engine = create_engine(path)
        df.to_sql(table_name, engine, index=False)

    def start_processing(self,messages,categories,db_name='DB1',table_name='Table1'):
        """

        :return:
        """
        data_merged = self.data_merger(messages,categories)
        data_cleaned = self.clean_labels(data_merged)
        data_cleaned = data_cleaned.drop_duplicates()

        self.setup_sqlite_DB(data_cleaned,db_name,table_name)
        print("SQLite table {} inside DB {} ready to use".format(table_name, db_name))