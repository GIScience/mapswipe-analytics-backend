#!/usr/bin/python3
#
# Author: B. Herfort, M. Reinmuth, 2017
############################################

import pyrebase
import psycopg2  # handle postgres
import pymysql #handle mysql
import json
import sys


# Configuration of the firebase database
def firebase_admin_auth():
    try:
        with open('../cfg/config.cfg') as json_data_file:
            data = json.load(json_data_file)
            api_key = data['firebase']['api_key']
            auth_domain = data['firebase']['auth_domain']
            database_url = data['firebase']['database_url']
            storage_bucket = data['firebase']['storage_bucket']
            service_account = data['firebase']['service_account']
            # print('use configuration for psql as provided by config.json')
    except:
        # Default Configuration
        sys.exit('please provide a firebase config')

    # adapt this to your firebase setting
    config = {
        "apiKey": api_key,
        "authDomain": auth_domain,
        "databaseURL": database_url,
        "storageBucket": storage_bucket,
        # this is important if you want to login as admin
        "serviceAccount": service_account
    }
    firebase = pyrebase.initialize_app(config)
    return firebase

def geoserver_config():
    # general configuration of the geoserver
    try:
        with open('../cfg/config.cfg') as json_data_file:
            data = json.load(json_data_file)
            geoserver_config = {
                "username": data['geoserver']['username'],
                "password": data['geoserver']['password'],
                "baseurl": data['geoserver']['baseurl'],
                "workspace": data['geoserver']['workspace'],
                "datastore": data['geoserver']['datastore'],
                "namespace": data['geoserver']['namespace'],
                "styles": data['geoserver']['styles']
            }
    except:
        sys.exit('please provide a geoserver config.')
        # Default configuration

    return geoserver_config

# # Configuration of the psql database
class psqlDB(object):
    _db_connection = None
    _db_cur = None

    def __init__(self):
        # try to load configuration from config file
        try:
            with open('../cfg/config.cfg') as json_data_file:
                data = json.load(json_data_file)
                dbname = data['psql']['database']
                user = data['psql']['username']
                password = data['psql']['password']
                host = data['psql']['host']
                port = data['psql']['port']
                #print('use configuration for psql as provided by config.json')
        except:
            sys.exit('please provide a psql config')

        # adapt this to your psql setting
        self._db_connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            # for testing on your local computer, change the port for different purpose, on the server we use port 5432
            port=port)

    def query(self, query, data):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query, data)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def retr_query(self, retr_query, data):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(retr_query, data)
        content = self._db_cur.fetchall()
        self._db_connection.commit()
        self._db_cur.close()
        return content

    def copy_from(self, file, table, sep='\t', null='\\N', size=8192, columns=None):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.copy_from(file, table, sep=sep, null='\\N', size=8192, columns=columns)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def close(self):
        self._db_connection.close()

    def __del__(self):
        #self._db_cur.close()
        self._db_connection.close()

# Configuration of the default psql database 'postgres'
class default_psqlDB(object):
    _db_connection = None
    _db_cur = None

    def __init__(self):
        # try to load configuration from config file
        try:
            with open('../cfg/config.cfg') as json_data_file:
                data = json.load(json_data_file)
                dbname = 'postgres'
                user = data['psql']['username']
                password = data['psql']['password']
                host = data['psql']['host']
                port = data['psql']['port']
                #print('use configuration for psql as provided by config.json')
        except:
            sys.exit('please provide a psql config')

        # adapt this to your psql setting
        self._db_connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            # for testing on your local computer, change the port for different purpose, on the server we use port 5432
            port=port)
        self._db_connection.set_isolation_level(0)

    def query(self, query, data):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query, data)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def close(self):
        self._db_connection.close()

    def __del__(self):
        #self._db_cur.close()
        self._db_connection.close()


class mapswipe_psqlDB(object):
    _db_connection = None
    _db_cur = None

    def __init__(self):
        # try to load configuration from config file
        try:
            with open('../cfg/config.cfg') as json_data_file:
                data = json.load(json_data_file)
                dbname = data['mapswipe_psql']['database']
                user = data['mapswipe_psql']['username']
                password = data['mapswipe_psql']['password']
                host = data['mapswipe_psql']['host']
                port = data['mapswipe_psql']['port']
                #print('use configuration for psql as provided by config.json')
        except:
            sys.exit('please provide a psql config')

        # adapt this to your psql setting
        self._db_connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            # for testing on your local computer, change the port for different purpose, on the server we use port 5432
            port=port)

    def query(self, query, data):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query, data)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def retr_query(self, retr_query, data):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(retr_query, data)
        content = self._db_cur.fetchall()
        self._db_connection.commit()
        self._db_cur.close()
        return content

    def copy_from(self, file, table, sep='\t', null='\\N', size=8192, columns=None):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.copy_from(file, table, sep=sep, null='\\N', size=8192, columns=columns)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def close(self):
        self._db_connection.close()

    def __del__(self):
        #self._db_cur.close()
        self._db_connection.close()
