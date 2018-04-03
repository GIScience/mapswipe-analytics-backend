#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

import logging
import os
import threading
import time
from queue import Queue

import requests
from psycopg2 import sql

from get_tasks import check_tasks

from auth import firebase_admin_auth
from auth import psqlDB


import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--task_table_name', required=False, default='tasks', type=str,
                    help='the name of the tasks table in your database')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')



def get_completed_count(q):

    while not q.empty():
        # get the values from the q object
        fb_db, completed_count_file, project_id, group_id = q.get()

        ### this functions downloads only the completed count per group from firebase
        completed_count = fb_db.child("groups").child(project_id).child(group_id).child("completedCount").get().val()

        # we check whether the completed_count is defined. if the connection fails etc. we don't write None
        # writing None would cause an error during the upload to psql
        if completed_count is None:
            print(completed_count)
            print('completed count for group %s of project %s is not defined' % (group_id, project_id))
        else:
            outline = '%s;%s;%s\n' % (group_id, project_id, completed_count)
            completed_count_file.write(outline)


        q.task_done()

def download_tasks_completed_count(project_id):
    ### this functions uses threading to get the completed counts of all groups per project

    firebase = firebase_admin_auth()
    fb_db = firebase.database()

    # this tries to set the max pool connections to 100
    adapter = requests.adapters.HTTPAdapter(max_retries=10, pool_connections=100, pool_maxsize=100)
    for scheme in ('http://', 'https://'):
        fb_db.requests.mount(scheme, adapter)

    completed_count_filename = 'completed_count.csv'
    completed_count_file = open(completed_count_filename, 'w')

    # we will use a queue to limit the number of threads running in parallel
    q = Queue(maxsize=0)
    num_threads = 25

    # it is important to use the shallow option, only keys will be loaded and not the complete json
    all_groups = fb_db.child("groups").child(project_id).shallow().get().val()
    for group_id in all_groups:
        q.put([fb_db, completed_count_file, project_id, group_id])



    for i in range(num_threads):
        worker = threading.Thread(
            target=get_completed_count,
            args=(q,))
        #worker.setDaemon(True)
        worker.start()

    q.join()

    completed_count_file.close()
    del fb_db
    return completed_count_filename

def update_completed_count_psql(completed_count_filename, project_id, task_table_name):
    ### this functions loads data from csv to psql and updates the group table

    # Open CSV file
    completed_count_file = open(completed_count_filename, 'r')
    columns = ('groupid', 'projectid', 'completedcount')
    raw_group_table_name = 'groups_' + task_table_name + '_{}'.format(project_id)
    task_table_name = task_table_name + '_{}'.format(project_id)

    p_con = psqlDB()
    # first, create table with group id and completed count
    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
          groupid integer
          ,projectid integer
          ,completedcount integer
        )
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(raw_group_table_name),
                                            sql.Identifier(raw_group_table_name))

    p_con.query(sql_insert, None)

    # copy completed count data to psql
    p_con.copy_from(completed_count_file, raw_group_table_name, sep=';', columns=columns)

    completed_count_file.close()
    os.remove(completed_count_filename)
    del completed_count_file

    sql_insert = '''
        UPDATE {} as b
        SET
          completedcount = a.completedcount
        FROM {} as a
        WHERE
          a.groupid = b.groupid
          AND
          a.projectid = b.projectid;
        DROP TABLE IF EXISTS {}
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(task_table_name),
                                            sql.Identifier(raw_group_table_name),
                                            sql.Identifier(raw_group_table_name))
    p_con.query(sql_insert, None)
    p_con.close()

    return

def get_tasks_completed_count(updated_projects, task_table_name):

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # get groups completed counts for selected projects from firebase

    # record time
    starttime = time.time()

    for project_id in updated_projects:
        print('perform updated for project: %s' % project_id)
        logging.warning('perform updated for project: %s' % project_id)

        # check if we have the tasks in our database
        # if not there is no meaning in performing the update
        if check_tasks(project_id, task_table_name):
            # download group completed count data from firebase
            completed_count_filename = download_tasks_completed_count(project_id)
            print('downloaded tasks completed count from firebase for project: ', project_id)
            logging.warning('downloaded tasks completed count from firebase for project: %s' % project_id)
            # save group completed count in psql database
            update_completed_count_psql(completed_count_filename, project_id, task_table_name)
            print('updated tasks completed count in psql for project: ', project_id)
            logging.warning('updated tasks completed count in psql for project: %s' % project_id)
        else:
            print('tasks have not been imported. please run get_tasks() first')
            logging.warning('tasks have not been imported. please run get_tasks() first')

    # calc process time
    endtime = time.time() - starttime
    print('finished tasks completed count update, %f sec.' % endtime)
    logging.warning('finished tasks completed count update, %f sec.' % endtime)
    return

########################################################################################################################
if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    get_tasks_completed_count(args.projects, args.task_table_name)

