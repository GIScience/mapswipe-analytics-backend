#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################


# add some files in different folders to sys.
# these files can than be loaded directly
import sys
sys.path.insert(0, '../cfg')

import csv
import os
import logging


import threading
import time
from queue import Queue

from psycopg2 import sql

from auth import mapswipe_psqlDB
from auth import psqlDB

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')

# default timestamp is set to the last hour
parser.add_argument('-t', '--timestamp', required=False, default=int((time.time() - 3600)*1000), type=int,
                    help='the timestamp to extract the results')
parser.add_argument('-l', '--limit', required=False, default=500000, type=int,
                    help='the maximum amount of results that will be downloaded within one transaction. this is to handle memory consumption.')
parser.add_argument('-r', '--results_table_name', required=False, default='results', type=str,
                    help='the name of the results table in your database')


def get_last_timestamp(results_table_name):
    # create db object
    p_con = psqlDB()
    # query last timestamp from psql
    timestamp_query = '''
        SELECT
          timestamp
        FROM
          {}
        ORDER BY 
          timestamp DESC
        LIMIT 1
    '''
    timestamp_query = sql.SQL(timestamp_query).format(sql.Identifier(results_table_name))

    data = p_con.retr_query(timestamp_query, None)

    # check if there is no last timestamp in db in order to start a complete import
    if len(data) == 0:
        timestamp_psql = 0
    else:
        timestamp_psql = data[0][0]
    # delete/close db connection
    p_con.close()
    return timestamp_psql

def save_in_csv(data):
    # create temporary csv; prepare header
    results_csv_filename = 'results_temp.csv'
    if os.path.isfile(os.getcwd() + '/' + results_csv_filename):
        os.remove(os.getcwd() + '/' + results_csv_filename)
    # in python 3 one must use w instead of wb to write
    # add newline = "" to avoid blank lines
    results_file = open(results_csv_filename, 'w', newline="")
    results_fieldnames = [u'taskId', u'userId', u'projectId', u'timestamp', u'result', u'duplicates']
    results_writer = csv.DictWriter(results_file, fieldnames=results_fieldnames, dialect='excel')

    row_results = {}
    for row in data:
        # get every result out of the exported csv from mysql and fill the dict keys for every row
        row_results[u'taskId'] = row[0]
        row_results[u'userId'] = row[1]
        row_results[u'projectId'] = row[2]
        row_results[u'timestamp'] = row[3]
        row_results[u'result'] = row[4]
        row_results[u'duplicates'] = row[5]
        # write row to file

        results_writer.writerow(row_results)
    # close file
    results_file.close()

    return results_csv_filename

def create_results_psql(results_csv_filename, results_table_name):

    # Open CSV file
    results_file = open(results_csv_filename, 'r')
    columns = ('taskId', 'userId', 'projectId', 'timestamp', 'result', 'duplicates')
    raw_results_table_name = 'raw_' + results_table_name

    p_con = psqlDB()
    # first, create table with group id and completed count
    sql_insert = '''
            DROP TABLE IF EXISTS {};
            CREATE TABLE {} (
              taskId VARCHAR NOT NULL
              ,userId VARCHAR NOT NULL
              ,projectId INT NOT NULL
              ,timestamp BIGINT NOT NULL
              ,result INT NOT NULL
              ,duplicates INT NOT NULL
              ,CONSTRAINT pk_result_id_raw PRIMARY KEY (taskId, userId, projectId)
            );
        '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(raw_results_table_name),
                                            sql.Identifier(raw_results_table_name))

    p_con.query(sql_insert, None)

    # copy completed count data to psql
    p_con.copy_from(results_file, raw_results_table_name, sep=',', columns=columns)
    results_file.close()
    os.remove(results_csv_filename)
    print('copied results to temporary psql table.')

    sql_insert = '''
        INSERT INTO {} (taskid, userid, projectid, timestamp, result, duplicates)
          SELECT
            *
          FROM {} as b
          ON CONFLICT ON CONSTRAINT "pk_result_id"
          DO UPDATE
          SET
          (taskid, userid, projectid, timestamp, result, duplicates)
          =
          (results.taskid, results.userid, results.projectid, results.timestamp, results.result, results.duplicates);
        DROP TABLE IF EXISTS {} CASCADE;
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(results_table_name),
                                            sql.Identifier(raw_results_table_name),
                                            sql.Identifier(raw_results_table_name))

    p_con.query(sql_insert, None)
    p_con.close()
    return

def download_results(timestamp, offset, limit):
    # establish mysql connection
    m_con = mapswipe_psqlDB()
    # sql command
    higher_timestamp = '''
        SELECT
         task_id
         ,user_id
         ,project_id
         ,timestamp
         ,info ->> 'result' as result
         ,duplicates
        FROM
          results
        WHERE
          timestamp > %s
        ORDER BY timestamp ASC
        OFFSET %s
        LIMIT %s
        '''

    # query new results, store in tuple
    data = [timestamp, offset, limit]
    new_results = m_con.retr_query(higher_timestamp, data)
    # save results in temp csv
    csv_name = save_in_csv(new_results)

    # delete/close db connection
    del m_con

    return new_results


def get_changed_projects(timestamp):
    m_con = mapswipe_psqlDB()
    changed_projects = '''
      SELECT
        project_id
      FROM
        results
      WHERE
        timestamp >%s
      GROUP BY
        project_id
    '''
    # query list of projects, the new results refer to
    project_list_raw = m_con.retr_query(changed_projects, [timestamp])

    changed_projects_list = []
    for i in project_list_raw:
        changed_projects_list.append(i[0])
    # delete/close db connection

    del m_con
    return changed_projects_list


def get_results_count(timestamp):
    m_con = mapswipe_psqlDB()
    sql_insert = '''
      SELECT
        count(*)
      FROM
        results
      WHERE
        timestamp >%s
    '''
    # query list of projects, the new results refer to
    count = m_con.retr_query(sql_insert, [timestamp])[0][0]
    print(count)
    del m_con
    return count


def get_results(results_table_name, timestamp, limit):

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # record time
    starttime = time.time()

    # get the count of the results before downloading
    mysql_results_count = get_results_count(timestamp)
    print('there are %s new results.' % mysql_results_count)
    logging.warning('there are %s new results.' % mysql_results_count)

    # we loop the download. downloading everything at the same time would result in huge use of memory (>10GB)
    offset = 0
    passes = (mysql_results_count // limit) + 1

    for i in range(0, passes):

        print('we are in loop: ', i)
        logging.warning('we are in loop: %s' % i)

        # save new results to temporary csv
        mysql_results = download_results(timestamp, offset, limit)
        print('downloaded %s new results.' % len(mysql_results))
        logging.warning('downloaded %s new results.' % len(mysql_results))

        # copy results from csv to psql
        results_csv_filename = save_in_csv(mysql_results)

        # save results in psql
        # save_results_psql(mysql_results)
        create_results_psql(results_csv_filename, results_table_name)

        # we need to adjust the offset
        offset = offset + limit


    # get list of changed projects
    changed_projects_list = get_changed_projects(timestamp)

    # calc process time
    endtime = time.time() - starttime
    print('finished results import, %f sec.' % endtime)
    logging.warning('finished results import, %f sec.' % endtime)
    return changed_projects_list
    


########################################################################################################################
if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')


    changed_projects = get_results(args.results_table_name, args.timestamp, args.limit)
    print('projects with new results: ', changed_projects)
