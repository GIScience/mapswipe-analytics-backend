#!/usr/bin/python3
#
# Author: B. Herfort, M. Reinmuth, 2017
############################################

import psycopg2  # handle postgres
from psycopg2 import sql

# add some files in different folders to sys.
# these files can than be loaded directly
import sys
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

from auth import psqlDB

from get_user_contributions import clean_up_database

import logging
import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--task_table_name', required=False, default='tasks', type=str,
                    help='the prefix of the tasks tables in your database')
parser.add_argument('-p', '--projects', nargs='+', required=True, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')

########################################################################################################################

def get_unique_tasks(project_id, task_table_name):
    # some tasks might be duplicated in the database since they are part of two different groups
    # the completed count of these tasks will be merged

    p_con = psqlDB()

    input_table_name = task_table_name
    output_table_name = 'tasks_unique_{}'.format(project_id)

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      t.taskid
      ,t.projectid
      ,Sum(t.completedcount) as completedcount
      -- don't forget the geometry
      ,t.st_geomfromtext
    FROM
      {} as t
    WHERE
     projectid = %s
    GROUP BY
      t.taskid
      ,t.projectid
      ,t.st_geomfromtext
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))
    data = [str(project_id)]

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def create_all_contributions(project_id, unique_task_table_name, user_contributions_table):
    # user contributions and unique tasks are joined
    # this step is necessary since user contributions may leave out tasks where no user contributed any data

    p_con = psqlDB()

    input_table_name = user_contributions_table
    output_table_name = 'contributions_{}'.format(project_id)
    tasks_table = unique_task_table_name


    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          t.taskid
          ,t.projectid
          ,t.completedCount
          ,c.userid
          ,c.group_timestamp
          ,c.result
          ,t.st_geomfromtext as geo
        FROM
          {} as t
        LEFT JOIN
          {} as c ON (t.taskid = c.taskid AND t.projectid::int = c.projectid::int)
        WHERE
          t.projectid = %s 
        '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(tasks_table),
                                            sql.Identifier(input_table_name))
    data = [str(project_id)]

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

########################################################################################################################

def get_all_contributions(projects, tasks_table_name):
    # create an empty list for table names that will be deleted in the end

    logging.basicConfig(filename='enrichment.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    output_tables = []

    for project_id in projects:

        delete_table_list = []
        tasks_table_name = tasks_table_name + '_' + str(project_id)
        user_contributions_table = 'user_contributions_{}'.format(project_id)

        unique_tasks_table = get_unique_tasks(project_id, tasks_table_name)
        logging.warning('created: %s' % unique_tasks_table)
        delete_table_list.append(unique_tasks_table)

        all_contributions = create_all_contributions(project_id, unique_tasks_table, user_contributions_table)
        logging.warning('created: %s' % all_contributions)
        delete_table_list.append(user_contributions_table)

        clean_up_database(delete_table_list)
        logging.warning('deleted: %s' % delete_table_list)
        output_tables.append(all_contributions)

    return output_tables


########################################################################################################################

if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    get_all_contributions(args.projects, args.task_table_name)





