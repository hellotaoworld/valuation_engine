from dotenv import load_dotenv
load_dotenv()
import os
import mysql.connector

# Establish a connection to the MySQL database
cloud_config = {
  'host': os.getenv("DB_HOST"),
  'user':os.getenv("DB_USERNAME"),
  'passwd': os.getenv("DB_PASSWORD"),
  'db': os.getenv("DB_NAME"),
  'autocommit': True
}

local_config = {
  'host': os.getenv("LOCALDB_HOST"),
  'user':os.getenv("LOCALDB_USERNAME"),
  'passwd': os.getenv("LOCALDB_PASSWORD"),
  'db': os.getenv("LOCALDB_NAME"),
  'autocommit': True
}

def establish_local_database():
    connection = mysql.connector.connect(**local_config)
    if connection.is_connected():
        return connection
    else:
        return "error:database connection faied"
      
def establish_cloud_database():
    connection = mysql.connector.connect(**cloud_config)
    if connection.is_connected():
        return connection
    else:
        return "error:database connection faied"