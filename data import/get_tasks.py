#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

# add some files in different folders to sys.
# these files can than be loaded directly
import sys
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

import os
import logging

import time
import threading
from queue import Queue
import requests

from psycopg2 import sql

from auth import firebase_admin_auth
from auth import psqlDB
from geometry_from_tile_coords import geometry_from_tile_coords

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--task_table_name', required=False, default='tasks', type=str,
                    help='the name of the tasks table in your database')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')



def check_tasks(project_id, task_table_name):
    task_table_name = task_table_name + '_{}'.format(project_id)

    p_con = psqlDB()
    sql_insert = '''
        SELECT
          taskid
        FROM
          {}
        WHERE
          projectid = %s
        LIMIT 1
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(task_table_name))
    data = [project_id]
    try:
        task = p_con.retr_query(sql_insert, data)
        p_con.close()

        if len(task) == 1:
            return True
        else:
            return False
    except:
        return False

def get_tasks_geometry(q):
    while not q.empty():

        project_id, group_id, task_file = q.get()

        firebase = firebase_admin_auth()
        fb_db = firebase.database()

        completed_count = fb_db.child("groups").child(project_id).child(group_id).child("completedCount").shallow().get().val()
        group_tasks = fb_db.child("groups").child(project_id).child(group_id).child("tasks").shallow().get().val()
        print(group_tasks)

        if len(group_tasks) > 0:
            for task_id in group_tasks:

                # get TileX, TileY, TileZ and convert to integer
                tile_z, tile_x, tile_y = map(int, task_id.split('-'))

                task_geom = geometry_from_tile_coords(tile_x, tile_y, tile_z)

                outline = '%s;%i;%i;%i;%s\n' % (task_id, int(project_id), int(group_id),
                                                int(completed_count), task_geom)
                task_file.write(outline)

        q.task_done()

def download_tasks(project_id):
    ### this function downloads all group info from firebase
    ### in the second step all tasks are generated

    task_filename = 'tasks.csv'
    task_file = open(task_filename, 'w')

    firebase = firebase_admin_auth()
    fb_db = firebase.database()

    group_ids = fb_db.child("groups").child(project_id).shallow().get().val()
    print('got group ids data from firebase')

    # this tries to set the max pool connections to 100
    adapter = requests.adapters.HTTPAdapter(max_retries=5, pool_connections=100, pool_maxsize=100)
    for scheme in ('http://', 'https://'):
        fb_db.requests.mount(scheme, adapter)

    # we will use a queue to limit the number of threads running in parallel
    q = Queue(maxsize=0)
    num_threads = 8

    for group_id in group_ids:
        q.put([project_id, group_id, task_file])

    for i in range(num_threads):
        worker = threading.Thread(
            target=get_tasks_geometry,
            args=(q,))
        #worker.setDaemon(True)
        worker.start()

    q.join()


    task_file.close()
    print('Saved tasks file')

    del fb_db
    return task_filename


def save_tasks_psql(project_id, task_filename, task_table_name):
    ### this function saves the task information to the psql database

    # Open CSV file
    task_file = open(task_filename, 'r')
    columns = ('taskid', 'projectid', 'groupid', 'completedcount', 'geo')
    raw_task_table_name = 'raw_' + task_table_name
    task_table_name = task_table_name + '_{}'.format(project_id)

    # first import to a table where we store the geom as text
    p_con = psqlDB()
    sql_insert = '''
        DROP TABLE IF EXISTS {} CASCADE;
        CREATE TABLE {} (
            taskId varchar NOT NULL
            ,projectId int NOT NULL
            ,groupId int
            ,completedcount int
            ,geo varchar
            --,CONSTRAINT pk_task_id_raw PRIMARY KEY (taskId, projectId, groupId)
        );
        '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(raw_task_table_name),
                                            sql.Identifier(raw_task_table_name))
    p_con.query(sql_insert, None)

    # copy data to the new table
    p_con.copy_from(task_file, raw_task_table_name, sep=';', columns=columns)
    task_file.close()
    os.remove(task_filename)
    print('copied task information to psql')

    # second import all entries into the task table and convert into psql geometry
    sql_insert = '''
        CREATE TABLE {} AS
        SELECT
          taskid
          ,projectid
          ,groupid
          ,completedcount
          ,ST_GeomFromText(geo, 4326)
        FROM {};
        DROP TABLE IF EXISTS {} CASCADE;
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(task_table_name),
                                            sql.Identifier(raw_task_table_name),
                                            sql.Identifier(raw_task_table_name) )
    try:
        p_con.query(sql_insert, None)
        print('inserted task information in tasks table')
    except BaseException:
        # we catch duplicates that are already in the pgsql database
        tb = sys.exc_info()
        error_class = tb[0]
        error_detail = str(tb[1]).split('\n')[0]
        if str(error_class) == "<class 'psycopg2.IntegrityError'>" and error_detail == 'duplicate key value violates unique constraint "pk_task_id"':
            print('tasks already imported')
        else:
            print(error_class, error_detail)

    p_con.close()
    return



def get_tasks(new_projects, task_table_name):

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # if there are new projects, create tables for tasks
    # for testing project id = 5519, bijagos islands with small projects

    # record time
    starttime = time.time()

    for project_id in new_projects:
        print('get tasks for new project:', project_id)
        logging.warning('get tasks for new project: %s' % project_id)
        if not check_tasks(project_id, task_table_name):
            new_tasks = download_tasks(project_id)
            print('got tasks from firebase')
            logging.warning('got tasks from firebase')
            save_tasks_psql(project_id, new_tasks, task_table_name)
            print('saved tasks to local mapswipe db')
            logging.warning('saved tasks to local mapswipe db')
        else:
            print('tasks have already been imported. please have a look at your database')
            logging.warning('tasks have already been imported. please have a look at your database')

    # calc process time
    endtime = time.time() - starttime
    print('finished tasks import, %f sec.' % endtime)
    logging.warning('finished tasks import, %f sec.' % endtime)

    return


########################################################################################################################
if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    get_tasks(args.projects, args.task_table_name)
