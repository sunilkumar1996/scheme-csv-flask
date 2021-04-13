# Task 3 solution

import sqlite3, csv
import pandas as pd
    import pymysql
    import mysql.connector
    from mysql.connector import Error


    # def create_server_connection(host_name, user_name, user_password, db_name):
    #     connection = None
    #     try:
    #         connection = mysql.connector.connect(
    #             host=host_name,
    #             user=user_name,
    #             passwd=user_password,
    #             database=db_name
    #         )
    #         df=pd.read_sql('/home/sunil/workspace/pph/mustfa/bristol-air-quality-data.sql', connection)
    #         print(df)
    #         print("MySQL Database connection successful")
    #     except Error as err:
    #         print(f"Error: '{err}'")

    #     return connection



def generate_sql_from_csv(csv_filename):

    """
        Generate sql file from input csv file

        param: csv_filename is the file name of csv file to be read

        returns the file name of the sql file.
    """

    # Start building the sql query by statement: DROP TABLE IF EXISTS `bristol-air-quality`;

    # and this to prevent errors in loading the table if it was imported before into database.

    # then we create the table `bristol-air-quality` and specifying the columns and their types

    # as specified in paper, then save the statemnts into the variable sql_text

    sql_text = '''DROP TABLE IF EXISTS `bristol_air_quality`; CREATE TABLE `bristol_air_quality` (`Date Time` text, NOx real, NO2 real, NO real, SiteID INTEGER, PM10 real, NVPM10 real, VPM10 real, `NVPM2.5` real, `PM2.5` real, `VPM2.5` real, CO real, O3 real, SO2 real, Temperature real, RH real, `Air Pressure` real, Location text, geo_point_2d text, DateStart text, DateEnd text, Current text, `Instrument Type` text);'''

    # Read the csv file into csv reader object

    with open(csv_filename) as csv_file:

        reader = csv.reader(csv_file, delimiter=';')

        # pass the header row

        next(reader)

        # Iterate over each row in the reader object

        for row in reader:

            # For each row we will insert the row data into the sql_text varaible

            sql_text += f"INSERT INTO `bristo_air_quality` VALUES {tuple(row)};"

    # generate the file name of the output sql file

    output_sql_filename = f'{csv_filename.split(".")[0]}.sql'

    # After reading all the rows into sql_text variable,

    # save the variable value into output_sql_filename

    with open(output_sql_filename, 'w') as sql_file:

        sql_file.write(sql_text)

    # Finally return the file name of the sql file

    return output_sql_filename


# Now we can generate read the data in 'bristol-air-quality-data[cleaned part 1-c].csv' file

# into bristol-air-quality-data[cleaned part 1-c].sql file by calling the function generate_sql_from_csv

# and we will save the sql file name into air_quality_sql for populating the database

# air_quality_sql = generate_sql_from_csv('bristol-air-quality-data[cleaned part 1-c].csv')
air_quality_sql = generate_sql_from_csv('/home/sunil/workspace/pph/mustfa/bristol-air-quality-data.csv')

# query = None
# Next step is to populate the sql file into database

# First, open the sql file into sql object

with open(air_quality_sql, 'r') as sql:

# df=pd.read_sql('/home/sunil/workspace/pph/mustfa/bristol-air-quality-data.sql', connection)
# print(df)

# def execute_query(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         # cursor.executescript(sql.read())
#         connection.commit()
#         print("Query successful")
#     except Error as err:
#         print(f"Error: '{err}'")

    # Make a new sqlite3 connection and create the database in RAM
    # con = sqlite3.connect("one.db")
    con = mysql.connector.connect(user='root', password="root", host="localhost", database='testdb')
    # start a cursor to point to rows
    cur = con.cursor()
    # Read the sql file contents by executing cur.execute script method
    cur.executescript(sql.read())  
    # Finally commit the changes into database
    con.commit()
    # Let's validate there are no errors in the records
    # by reading the database into a dataframe and print info of df
    df = pd.read_sql("select * from `bristol_air_quality`;", con)
    print(df.info)

# connection = create_server_connection("localhost", "root", "root", "testdb")
# execute_query(connection, query)
