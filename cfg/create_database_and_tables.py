#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
import json
import time

from auth import psqlDB
from auth import default_psqlDB
from psycopg2 import sql

def create_db(database_name):
    # to create a new database, we need to connect to the default postgres database
    # from this connection we can create a new database

    p_con = default_psqlDB()

    sql_insert = '''
        CREATE DATABASE {};
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(database_name))
    p_con.query(sql_insert, None)
    print('created database: %s' % database_name)
    p_con.close()

def drop_db(database_name):

    p_con = default_psqlDB()

    sql_insert = '''
        DROP DATABASE IF EXISTS {};
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(database_name))
    p_con.query(sql_insert, None)
    print('droped database: %s' % database_name)
    p_con.close()


def create_projects_table():

    p_con = psqlDB()

    sql_insert = '''
    CREATE EXTENSION postgis;

    CREATE TABLE projects (
      id INT NOT NULL
      ,contributors INT NOT NULL
      ,groupAverage DOUBLE PRECISION NOT NULL
      ,image CHARACTER VARYING NOT NULL
      ,importKey CHARACTER VARYING NOT NULL
      ,isFeatured BOOLEAN NOT NULL
      ,lookFor CHARACTER VARYING NOT NULL
      ,name CHARACTER VARYING NOT NULL
      ,progress INT NOT NULL
      ,projectDetails CHARACTER VARYING NOT NULL
      ,state INT NOT NULL
      ,verificationCount INT NOT NULL
      ,corrupt BOOLEAN NOT NULL
      ,lastCheck TIMESTAMP WITHOUT TIME ZONE
      ,extent geometry
      ,centroid geometry
      ,CONSTRAINT pk_project_id PRIMARY KEY (id)
      );
    '''

    p_con.query(sql_insert, None)
    print('created table: projects')
    p_con.close()

def create_results_table():

    p_con = psqlDB()

    sql_insert = '''
        CREATE TABLE results (
          taskId VARCHAR NOT NULL
          ,userId VARCHAR NOT NULL
          ,projectId INT NOT NULL
          ,timestamp BIGINT NOT NULL
          ,result INT NOT NULL
          ,duplicates INT NOT NULL
          ,CONSTRAINT pk_result_id PRIMARY KEY (taskId, userId, projectId)
        );
        
        CREATE INDEX results_taskId_index
          ON public.results
          USING BTREE
          (taskId);
        
        CREATE INDEX results_timestamp_index
          ON public.results
          USING BTREE
          (timestamp);
        
        CREATE INDEX results_projectId_index
          ON public.results
          USING BTREE
          (projectId);
        
        CREATE INDEX results_index
          ON public.results
          USING BTREE
          (result);
    '''
    p_con.query(sql_insert, None)
    print('crated table: results')
    p_con.close()


def create_tasks_table():
    p_con = psqlDB()

    sql_insert = '''
        CREATE TABLE tasks (
          taskId VARCHAR NOT NULL
          ,projectId INT NOT NULL
          ,groupId INT
          ,completedCount INT NOT NULL 
          ,geo GEOMETRY
          ,CONSTRAINT pk_task_id PRIMARY KEY (taskId, projectId, groupId)
        );
        
        CREATE INDEX tasks_taskId_index
          ON public.tasks
          USING BTREE
          (taskId);
        
        CREATE INDEX tasks_projectId_index
          ON public.tasks
          USING BTREE
          (projectId);
        
        
        CREATE INDEX tasks_groupId_index
          ON public.tasks
          USING BTREE
          (groupId);
        
        
        CREATE INDEX tasks_geo_index
          ON public.tasks
          USING GIST
          (geo);
    '''
    p_con.query(sql_insert, None)
    print('created table: tasks')
    p_con.close()


def create_database_and_tables():

    try:
        # this functions looks for the name of the database in the configuration file
        with open('../cfg/config.cfg') as json_data_file:
            data = json.load(json_data_file)
            database_name = data['psql']['database']
            print('use configuration provided by config.cfg')
    except:
        # Default configuration
        database_name = 'your_mapswipe_db'
        print('Use default configuration. Please verify if this is in your interest. If not provide information in the config.cfg file.')


    # first step: drop existing database
    drop_db(database_name)

    # second step: create database
    create_db(database_name)

    # third step: create tables
    create_projects_table()
    create_results_table()
    create_tasks_table()


    # return the names of the tables created
    return ['projects', 'results', 'tasks']


########################################################################################################################
if __name__ == '__main__':
    # record time
    starttime = time.time()

    create_database_and_tables()

    # calc process time
    endtime = time.time() - starttime
    print('finished get projects, %f sec.' % endtime)


