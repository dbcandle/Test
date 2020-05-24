"""SQL Server Support Functions
"""

#
# *** Based on: https://towardsdatascience.com/supercharging-ms-sql-server-with-python-e3335d11fa17
#

# *** pandas is a fast, powerful, flexible and easy to use open source data analysis 
# *** and manipulation tool, built on top of the Python programming language.
# *** https://pandas.pydata.org/
import pandas as pd


# *** pyodbc is an open source Python module that makes accessing ODBC databases simple. 
# *** It implements the DB API 2.0 specification but is packed with even more Pythonic convenience.
# *** https://pypi.org/project/pyodbc/
import pyodbc


from datetime import datetime


# ***
# *** Connect to SQL Server:
# ***
class Sql:
    def __init__(self, database, server="XXV,5000"):

        # here we are telling python what to connect to (our SQL Server)
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server="+server+";"
                                   "Database="+database+";"
                                   "Trusted_Connection=yes;")

        # initialise query attribute
        self.query = "-- {}\n\n-- Made in Python".format(datetime.now()
                                                         .strftime("%d/%m/%Y"))


    # *** 
    # *** Function for SQL read-table:
    # *** 
    def manual(self, query, response=False, comment="manual query",
            verbose=False):
        """Enter a manual statement/query.

        Keyword arguments:
        query -- SQL query to run on SQL connection
        response -- Boolean value stating whether a response/table
                    should be returned (default False)
        comment -- string input that translates into a comment in the
                self.query string (default "manual query")
        verbose -- Boolean value indicating whether to print extra detail to
                the terminal or not (default True)

        Returns:
        if (response=True): a dataframe returned from the query sent
        if (response=False): a string to notify user manual query complete
        """
        cursor = self.cnxn.cursor()  # create execution cursor

        # append query to our SQL code logger
        self.query += ("\n\n-- "+str(comment)+"\n" + query)

        print("Executing query.")  # inform user
        if verbose:
            # print comment and query if user wants
            print(comment)
            print(query)

        if response:
            return pd.read_sql(query, self.cnxn)  # get sql query
        try:
            cursor.execute(query)  # execute
        except pyodbc.ProgrammingError as error:
            if verbose:
                print("Warning:\n{}".format(error))  # print error as a warning
        self.cnxn.commit()  # commit query to SQL Server
        return "Query complete."

    # *** 
    # *** Function for SQL write-table:
    # *** 
    def push_dataframe(self, data, table="raw_data", batchsize=500):
        # create execution cursor
        cursor = self.cnxn.cursor()
        # activate fast execute
        cursor.fast_executemany = True

        # create create table statement
        query = "CREATE TABLE [" + table + "] (\n"

        # iterate through each column to be included in create table statement
        for i in range(len(list(data))):
            query += "\t[{}] varchar(255)".format(list(data)[i])  # add column (everything is varchar for now)
            # append correct connection/end statement code
            if i != len(list(data))-1:
                query += ",\n"
            else:
                query += "\n);"

        cursor.execute(query)  # execute the create table statement
        self.cnxn.commit()  # commit changes

        # append query to our SQL code logger
        self.query += ("\n\n-- create table\n" + query)

        # insert the data in batches
        query = ("INSERT INTO [{}] ({})\n".format(table,
                                                '['+'], ['  # get columns
                                                .join(list(data)) + ']') +
                "VALUES\n(?{})".format(", ?"*(len(list(data))-1)))

        # insert data into target table in batches of 'batchsize'
        for i in range(0, len(data), batchsize):
            if i+batchsize > len(data):
                batch = data[i: len(data)].values.tolist()
            else:
                batch = data[i: i+batchsize].values.tolist()
            # execute batch insert
            cursor.executemany(query, batch)
            # commit insert to SQL Server
            self.cnxn.commit()