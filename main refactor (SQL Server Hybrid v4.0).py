# *** 
# *** Name: main refactor (SQL Server Hybrid v4.0).py
# *** 
# *** Version: 2020-05-16 - Combines SQL Server functionality with string_grouper.
# ***


# ***********************
# *** Imports Section ***
# ***********************

from string_grouper import match_strings, match_most_similar, group_similar_strings, StringGrouper

# *** Import my custom SQL Server functionality:
from sql_server import Sql

# *** os — Miscellaneous operating system interfaces
# *** https://docs.python.org/3/library/os.html
import os

# *** re — Regular expression operations
# *** https://docs.python.org/3/library/re.html
import re

# *** time — Time access and conversions
# *** https://docs.python.org/3/library/time.html
import time

# *** openpyxl - A Python library to read/write Excel 2010 xlsx/xlsm files
# *** https://openpyxl.readthedocs.io/en/stable/
import openpyxl

# *** pandas is a fast, powerful, flexible and easy to use open source data analysis
# *** and manipulation tool, built on top of the Python programming language.
# *** https://pandas.pydata.org/
import pandas as pd

# *** scikit-learn - Machine Learning in Python
# *** https://scikit-learn.org/stable/
# *** https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.cosine_similarity.html?highlight=cosine_sim
from sklearn.metrics.pairwise import cosine_similarity
# *** https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html?highlight=tfidfvectorizer#sklearn.feature_extraction.text.TfidfVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
# *** https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.NearestNeighbors.html?highlight=nearestneighbors#sklearn.neighbors.NearestNeighbors
from sklearn.neighbors import NearestNeighbors

# *** ftfy: fixes text for you
# *** https://ftfy.readthedocs.io/en/latest/
from ftfy import fix_text


# ********************
# *** Defs Section ***
# ********************

# *** fnExit - Exit from python
def fnExit():
    import sys
    print ("*** Exiting via sys.exit() ***")
    sys.exit()

# *** fnListNone - Check list for None values:
def fnListNone(p_list):
    res = [i for i in range(len(p_list)) if p_list[i] == None]
    #print("The None indices list is : " + str(res))
    #print("The len(res) is: ", len(res))
    #print("res is: ", res)
    if len(res) == 0:
        return False
    else:
        return True

# *** Function to execute SQL statement and save results to Excel:
def SQLtoExcel(database, server, sql_statement, filename):
    sql = Sql(database, server)
    SQL_Result = sql.manual(sql_statement, response=True)
    if os.path.exists(filename):
        os.remove(filename)
    with pd.ExcelWriter(filename) as writer:
        SQL_Result.to_excel(writer)
        print ("Generated: ", filename)

# *** Function to execute SQL statement and return dataframe:
def SQLtoDataframe(database, server, sql_statement):
    sql = Sql(database, server)
    return sql.manual(sql_statement, response=True)

# *** Function to execute SQL statement and return dataframe:
def DataframetoSQL(database, server, df, table):
    sql = Sql(database, server)
    # push_dataframe(self, data, table="raw_data", batchsize=500):
    return sql.push_dataframe(df, table)


# **************************
# *** Executable Section ***
# **************************

print("Select SQL Server Database:")
print("  1 - ZEUS")
print("  2 - HERCULES")
print("  3 - Silver")
selection = input("Enter your value: ")
if selection == "1":
    global_database = "ZEUS"
elif selection == "2":
    global_database = "HERCULES"
elif selection == "3":
    global_database = "Silver"
else:
    print("Invalid selection - exiting!")
    fnExit()

print("Select Data to Analyze:")
print("  1 - Original")
print("  2 - Technopedia")
print("  3 - Simple data")
selection = input("Enter your value: ")
if selection == "1":
    global_data_source = "Original"
elif selection == "2":
    global_data_source = "Technopedia"
elif selection == "3":
    global_data_source = "Simple data"
else:
    print("Invalid selection - exiting!")
    fnExit()

print ("Configured for data source: ", global_data_source)

# *** Load data:
if global_data_source == "Technopedia":
    dfSUBJECT = SQLtoDataframe('Test',
                               global_database,
                               "SELECT isnull(Manufacturer,'') + ' ' + isnull(Product,'') + ' ' + isnull(Version,'') AS 'LU' \
                                  FROM maintain_mat_list$")
    dfSUBJECT_KeyColumn = "LU"
    dfREFERENCE = SQLtoDataframe('Technopedia',
                                 global_database,
                                 'SELECT [LU] \
                                    FROM [Technopedia_software_extended_lu_view]')
    dfREFERENCE_KeyColumn = "LU"
elif global_data_source == "Original":
    dfSUBJECT = SQLtoDataframe('Test',
                               global_database,
                               "SELECT [LU] \
                                  FROM [SUBJECT_test]")
    dfSUBJECT_KeyColumn = "LU"
    dfREFERENCE = SQLtoDataframe('Test',
                                 global_database,
                                 'SELECT [LU] \
                                    FROM [REFERENCE_test]')
    dfREFERENCE_KeyColumn = "LU"
elif global_data_source == "Simple data":
    dfSUBJECT = SQLtoDataframe('Test',
                               global_database,
                               "SELECT [LU], \
                                       [nxt1], \
                                       [nxt2] \
                                  FROM [SUBJECT_test3]")
    dfSUBJECT_KeyColumn = "LU"
    dfREFERENCE = SQLtoDataframe('Test',
                                 global_database,
                                 'SELECT [LU], \
                                         [cola], \
                                         [colb], \
                                         [colc] \
                                    FROM [REFERENCE_test3]')
    dfREFERENCE_KeyColumn = "LU"

# *******************************
# *** The StringGrouper class ***
# *******************************


# *** Create StringGrouper:
start_time = time.time()
string_grouper = StringGrouper(dfREFERENCE[dfREFERENCE_KeyColumn], dfSUBJECT[dfSUBJECT_KeyColumn], min_similarity=0.4 )
print("Fitting the StringGrouper - this will take a while...")
string_grouper = string_grouper.fit()
elapsed_time = time.time() - start_time
print(time.strftime("Time to create StringGrouper: %H:%M:%S", time.gmtime(elapsed_time)))


# *** Generate a DataFrame with all the matches and their cosine similarity:
# *** _matches_list contains:
# ***   master_side = Index into dfREFERENCE
# ***   dupe_side = Index into dfSUBJECT
# ***   similarity = How similar LU values in dfREFERENCE and dfSUBJECT are
# *** For indexing (iloc): https://thispointer.com/select-rows-columns-by-name-or-index-in-dataframe-using-loc-iloc-python-pandas/
# *** For concatenation of series: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
start_time = time.time()
print("Generating output DataFrame from string_grouper._matches_list - this will take a while...")
l_matches_list = string_grouper._matches_list
total_matches = len(l_matches_list)
current_match = 0
df_empty = True
for row in l_matches_list.itertuples():
    # Print status:
    current_match += 1
    if (current_match % 100 == 0):
        print("Total: ", total_matches, "  Current: ", current_match)

    # Isolate dfREFERENCE as dataframe:
    dfREFERENCE_as_series = dfREFERENCE.iloc[row[1], :]
    dfREFERENCE_as_series = dfREFERENCE_as_series.add_prefix('R_')
    dfREFERENCE_as_dataframe = pd.DataFrame(dfREFERENCE_as_series)
    dfREFERENCE_as_dataframe = dfREFERENCE_as_dataframe.T
    dfREFERENCE_as_dataframe = dfREFERENCE_as_dataframe.reset_index(drop=True)

    # Isolate similarity as dataframe:
    similarity_as_list = [row[3]]
    similarity_as_series = pd.Series(similarity_as_list, index =['Similarity'])
    similarity_as_dataframe = pd.DataFrame(similarity_as_series)
    similarity_as_dataframe = similarity_as_dataframe.T
    similarity_as_dataframe = similarity_as_dataframe.reset_index(drop=True)

    # Isolate dfSUBJECT as dataframe:
    dfSUBJECT_as_series = dfSUBJECT.iloc[row[2], :]
    dfSUBJECT_as_series = dfSUBJECT_as_series.add_prefix('S_')
    dfSUBJECT_as_dataframe = pd.DataFrame(dfSUBJECT_as_series)
    dfSUBJECT_as_dataframe = dfSUBJECT_as_dataframe.T
    dfSUBJECT_as_dataframe = dfSUBJECT_as_dataframe.reset_index(drop=True)

    # Concatenate the 3 series:
    tmp_dataframe = dfREFERENCE_as_dataframe.join(similarity_as_dataframe)
    tmp_dataframe = tmp_dataframe.join(dfSUBJECT_as_dataframe)

    if df_empty:
        df_empty = False
        df = tmp_dataframe
    else:
         df = df.append(tmp_dataframe)

elapsed_time = time.time() - start_time
print(time.strftime("Time to generate output DataFrame: %H:%M:%S", time.gmtime(elapsed_time)))

# *** Save resulting match between SUBJECT and REFERENCE to Excel:
ExcelOutputFile = 'TEST_EXCEL.xlsx'
if os.path.exists(ExcelOutputFile):
    os.remove(ExcelOutputFile)
with pd.ExcelWriter(ExcelOutputFile) as writer:
    df.to_excel(writer)
    print("Wrote file: ", ExcelOutputFile)


print('Execution Complete')
# ***********
# *** EOF ***
# ***********
