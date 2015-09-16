#!/usr/bin/env python

"""

Filename:           gp-ddl-maker.py
Customer:           Beach
Project:            Beach
Created:            May 21st, 2015
Updated:
Contact:            jvawdrey@pivotal.io
Description:        Script build a very basic DDL as a starting template
To Do:              Look at more than second row to determine column type
                    Add more column type (currently limited to TEXT, FLOAT, INTEGER)
"""

# 3rd party dependencies
import psycopg2     # postgres client
import sys          # System-specific parameters and function
import csv          # CSV File Reading and Writing
import os           # Miscellaneous operating system interfaces
import re           # Regular expression operations
import datetime     # Basic date and time types

__author__ = "Jarrod Vawdrey (jvawdrey@pivotal.io)"
__version__ = "0.0.1"
__status__ = "Development"

# postgres/greenplum/hawq connection
DBNAME="postgres"      # database name
DBUSER="postgres"      # database username
DBPASSWORD="postgres"  # database password
DBHOST="localhost"     # database host
SCHEMANAME="public"    # database schema
TABLENAME="public.ddl_import_test" # database tablename

# location of abs data
FILENAME="/Users/jvawdrey/code/gp-ddl-maker/test.csv"

# connect to database (database name, database user, database password, database host)
def connect_db(dbname, dbuser, dbpassword, dbhost):
    try:
        conn = psycopg2.connect("dbname=" + dbname + " user=" + dbuser + " host=" + dbhost + " password=" + dbpassword)
        cur = conn.cursor()
        e = None
        print "Connected to database " + dbname
        return (conn, cur, e)
    except psycopg2.Error as e:
        conn = None
        cur = None
        print "Unable to connect to database " + dbname
        return (conn, cur, e.diag.message_primary)

# disconnect from database
def disconnect_db(connection, cursor):
    try:
        if connection:
            connection.rollback()
        cursor.close()
        connection.close()
        print "Disconnected from database"
    except:
        print "Error disconnecting from database"

# drop database table
def dropTable(connection, cursor, tableName):
    try:
        cursor.execute('DROP TABLE IF EXISTS ' + tableName)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

# create database table
def createTable(connection, cursor, tableName, cols, colTypes):
    # number of columns
    N = len(cols)
    if (N ==0):
        return("Error no columns found")

    # build create table string
    sqlString = "CREATE TABLE " + tableName + " (%s)" % ", ".join("%s %s" % (n,t) for n,t in zip(cols, colTypes))

    try:
        # execute and commit query
        cursor.execute(sqlString)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

# insert csv table into database table
def insertCSVIntoTable(connection, cursor, csvName, tableName, cols):
    try:
        # can not use copy_from due to presence of header
        sqlString="COPY " + tableName + " FROM stdin DELIMITER \',\' CSV header";
        cursor.copy_expert(sqlString,open(csvName))
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)


# grab header from csv file
def getColumnNamesFromCSV(csvName):
    try:
        contents = csv.reader(open(csvName))
        header=contents.next()
        return (header,None)
    except:
        return (None,"Error getting header from file: " + csvName)

# guess column types based on 2nd record (only text and float types)
def guessColumnTypes(csvName):
    types=[]
    try:
        contents = csv.reader(open(csvName))
        contents.next()
        secondRecord=contents.next()
        for i in range(0, len(secondRecord)):
            if(re.search("[a-zA-Z\.]", secondRecord[i]) == None):
                types.append(" INTEGER")
            elif(re.search("[a-zA-Z]", secondRecord[i]) == None):
                types.append(" FLOAT")
            else:
                types.append(" TEXT")
        return (types,None)
    except:
        return (None,"Error guessing types from file: " + csvName)

# main driver (calls above function in sequence)
def main():

    print TABLENAME

    # Connect to database
    conn, cur, e = connect_db(DBNAME, DBUSER, DBPASSWORD, DBHOST)
    # If database connection not made exit
    if (e is not None):
        print "Exiting: Unable to connect to database \n" + e
        sys.exit()

    # grab column names (assumed first record of csv is header)
    header, e = getColumnNamesFromCSV(FILENAME);
    # If error grabing column names
    if (e is not None):
        print "Exiting: " + e
        sys.exit()

    # grab column types
    colTypes, e =  guessColumnTypes(FILENAME);
    # If error guessing column types
    if (e is not None):
        print "Exiting: " + e
        sys.exit()

    # drop table if exists
    e = dropTable(conn, cur, TABLENAME);
    # If error with dropping table then exit
    if (e is not None):
        print "Exiting: Unable to drop table \n" + e
        sys.exit()

    # create table
    e = createTable(conn, cur, TABLENAME, header, colTypes);
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Unable to create table \n" + e
        sys.exit()

    # insert data into table
    e = insertCSVIntoTable(conn, cur, FILENAME, TABLENAME, header);
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Unable to insert CSV into database table \n" + e
        sys.exit()

    # Disconnect from database
    disconnect_db(conn, cur)

    # system exit
    sys.exit()

# call driver
main()
