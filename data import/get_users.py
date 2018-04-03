#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')

import os
import time
import logging

from psycopg2 import sql

from auth import firebase_admin_auth
from auth import psqlDB

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-u', '--users_table_name', required=False, default='users', type=str,
                    help='the name of the users table in your database')


def download_users():
    ### this functions loads the user data from firebase

    firebase = firebase_admin_auth()
    fb_db = firebase.database()

    users_filename = 'users.csv'
    users_file = open(users_filename, 'w')
    # header = 'taskId,projectId,groupId,geo\n'
    # task_file.write(header)

    users = fb_db.child("users").get()
    for user_a in users.each():

        user = {}

        key = user_a.key()
        val = user_a.val()
        # we need to check for empty groups
        user["id"] = key
        user["distance"] = val["distance"]
        user["contributions"] = val["contributions"]
        user["name"] = str(val["username"].encode('utf-8)'))

        outline = '%s;%i;%i;%s\n' % (user["id"], user["distance"], user["contributions"], str(user["name"]))
        users_file.write(outline)

    users_file.close()
    del fb_db
    return users_filename

def save_users_psql(users_filename, users_table_name):
    ### this functions loads data from csv to psql and updates the group table

    # Open CSV file
    users_file = open(users_filename, 'r')
    columns = ('userid', 'distance', 'contributions', 'username')

    # create table for user data
    p_con = psqlDB()
    sql_insert = '''
        DROP TABLE IF EXISTS {} CASCADE; 
        CREATE TABLE {} (
          userid character varying,
          distance integer DEFAULT 0,
          contributions integer DEFAULT 0,
          username character varying,
          CONSTRAINT pk_user_id PRIMARY KEY (userid)
        )
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(users_table_name),
                                            sql.Identifier(users_table_name))
    p_con.query(sql_insert, None)

    # second copy data from csv file to psql table
    p_con.copy_from(users_file, users_table_name, sep=';', columns=columns)
    users_file.close()
    os.remove(users_filename)
    p_con.close()

    return

def get_users(users_table_name):

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # record time
    starttime = time.time()

    ## complete workflow to Import User Information ##
    users_filename = download_users()
    print('returned user information')
    logging.warning('returned user information')
    save_users_psql(users_filename, users_table_name)
    print('saved user information in psql')
    logging.warning('saved user information in psql')
    # calc process time
    endtime = time.time() - starttime
    print('finished users import, %f sec.' % endtime)
    logging.warning('finished users import, %f sec.' % endtime)
    return

########################################################################################################################
if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    get_users(args.users_table_name)
