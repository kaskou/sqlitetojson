import sqlite3
import pandas as pd
import os
import logging
import json


class JsonExport:
    def __init__(self, db_path):
        """
        Initializes the Export class with the sqlite db
        :param db_path: sqlite db path, ex.: "global.db"
        """
        self.path = db_path

    def db_connect(self):
        """
        Creates the connection with initialized sqlite db
        :return: db connection
        """
        con = sqlite3.connect(self.path)
        return con

    def execute_sql(self, query):
        """
        Execute the sql query on the db and returns the results in the form of dataframe
        :param query: sql query
        :return: dataframe
        """
        con = self.db_connect()
        curr = con.cursor()
        # Select table and display
        curr.execute(query)

        # Fetches all the rows from the result of the query
        rows = curr.fetchall()

        # Gets the column names for the table
        colnames = [desc[0] for desc in curr.description]

        # Converts into readable pandas data frame
        df_result = pd.DataFrame(rows, columns=colnames)
        print(df_result)
        return df_result

    def jsonwriter(self, df_data, filename):
        """
        Writes the dataframe data into json file
        :param df_data:
        :param filename:
        :return:
        """
        # writes the pandas data frame to json file
        folder_name = self.path.strip('.db')
        try:
            os.mkdir(folder_name)
        except OSError:
            logging.info("Creation of the directory %s failed" % folder_name)
        logging.info("Successfully created the directory %s " % folder_name)
        file_name = folder_name + '/' + filename + '.json'
        df_data.to_json(file_name, orient="records")

    def alltables(self):
        """
        Exports all the tables in the database
        :return:
        """
        # Export all the tables information to the json files
        query = 'SELECT name FROM sqlite_master WHERE type = "table";'
        df_data = self.execute_sql(query)
        tablenames = df_data['name'].values.tolist()

        for i in tablenames:
            print(i)
            query = 'SELECT * FROM ' + i + ';'
            df_data = self.execute_sql(query)
            self.jsonwriter(df_data, i)

    def singletable(self, tablename):
        """
        Exports the required table to json from the db
        :param tablename:
        :return:
        """
        # Export particular table information to the json file
        query = 'SELECT * FROM ' + tablename + ';'
        df_data = self.execute_sql(query)
        self.jsonwriter(df_data, tablename)
