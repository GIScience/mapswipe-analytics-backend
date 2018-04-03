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

import logging
import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--task_table_name', required=False, default='tasks', type=str,
                    help='the prefix of the tasks tables in your database')
parser.add_argument('-r', '--results_table_name', required=False, default='results', type=str,
                    help='the name of the results table in your database')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')




def select_data_for_project(table_name, projectid):
    p_con = psqlDB()

    input_table_name = table_name
    output_table_name = '{}_{}'.format(input_table_name, projectid)

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          *
        FROM
          {}
        WHERE
          projectid = %s'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))
    data = [str(projectid)]

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

def get_duplicated_tasks(project_tasks):
    # Get tasks that are contained in two different groups for that project

    p_con = psqlDB()
    input_table_name = project_tasks
    output_table_name = project_tasks + '_duplicates_marked'

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
            SELECT
              b.*
              ,e.count
            FROM
              {} as b
            LEFT JOIN
              (SELECT
                taskid
                ,count(taskid)
               FROM 
                 {}
                GROUP BY
                 taskid, projectid
               ) as e ON
               (e.taskid = b.taskid)
               '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

def get_user_tasks(project_results, project_tasks_duplicates_marked):
    #  get all potential groups
    # Clean the potential list of groups by filtering out unreliable groups
    # these 'unreliable' groups are groups where the user only worked in the overlapping area,
    # this means that we don't know for sure for which group the user submitted the result

    p_con = psqlDB()

    input_table_name_a = project_tasks_duplicates_marked
    input_table_name_b = project_results
    output_table_name = project_results + '_user_tasks'

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
            SELECT
              a.taskid
              ,b.userid
              ,b.projectid
              --,b.groupid
              ,b.group_timestamp
              ,a.st_geomfromtext as geo
            FROM
             {} as a,
            (
            -- this part selects all groups the user worked on
            -- all results per group are counted
            -- if the user contributed more results in total than edge tasks
            -- the group is valid, else we assume that the user did not work in this group
            SELECT
              r.userid as userid
              ,t.projectid
              ,t.groupid
              ,count(t.groupid)
              ,Sum(CASE
                WHEN count = 1 THEN 0
                WHEN count > 1 THEN 1
               END) as edge_count
              -- we select the maximum timestamp for each group, this will function as the timestamp for
              -- all individual tasks of this group for each user
              ,max(r.timestamp) as group_timestamp
            FROM
              {} as t, {} as r
            WHERE
              t.taskid = r.taskid
              AND
              t.projectid::int = r.projectid
            GROUP BY
              t.projectid, t.groupid, r.userid
            --ORDER BY
            --  r.userid, t.projectid, t.groupid
              ) as b
            WHERE
              b.count > b.edge_count
              AND
              b.projectid::int = a.projectid::int
              AND
              b.groupid = a.groupid
            GROUP BY
             -- we need to group by taskid so that we avoid duplicates
            a.taskid, a.st_geomfromtext, b.projectid, b.userid, b.group_timestamp
            '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name_a),
                                            sql.Identifier(input_table_name_a),
                                            sql.Identifier(input_table_name_b))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

def create_user_contributions(project_results, user_tasks):
    p_con = psqlDB()

    input_table_name_a = user_tasks
    input_table_name_b = project_results
    output_table_name = '{}_{}'.format('user_contributions', project_results.split('_')[-1])

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          b.taskid
          ,b.userid
          ,b.projectid
          --,b.groupid
          ,CASE
            WHEN r.result > 0 THEN r.result
            ELSE 0
           END as result
          ,(b.group_timestamp / 1000)::int as group_timestamp
          ,b.geo
        FROM 
          {} as b
          LEFT JOIN {} as r
          ON (b.userid = r.userid
              AND
              b.taskid = r.taskid
              AND
              b.projectid::int = r.projectid)
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name_a),
                                            sql.Identifier(input_table_name_b))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

def clean_up_database(delete_table_list):
    p_con = psqlDB()
    for i in range(0, len(delete_table_list)):


        sql_insert = '''
        DROP TABLE IF EXISTS {};
        '''

        sql_insert = sql.SQL(sql_insert).format(sql.Identifier(delete_table_list[i]))

        p_con.query(sql_insert, None)
        print('deleted: %s' % delete_table_list[i])
    del p_con

########################################################################################################################

def get_user_contributions(projects, result_table_name, tasks_table_name):

    logging.basicConfig(filename='enrichment.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    output_tables = []

    for project_id in projects:

        # create an empty list for table names that will be deleted in the end
        delete_table_list = []

        # get all results and tasks per project
        # raw_results, tasks = select_data_for_project(projectid)
        project_results = select_data_for_project(result_table_name, project_id)
        project_tasks = tasks_table_name + '_' + str(project_id)
        logging.warning('created: %s' % project_results)
        delete_table_list.extend((project_results,))


        # calculate all tasks for all users per project
        tasks_duplicates_marked = get_duplicated_tasks(project_tasks)
        logging.warning('created: %s' % tasks_duplicates_marked)
        user_tasks = get_user_tasks(project_results, tasks_duplicates_marked)
        logging.warning('created: %s' % user_tasks)
        user_contributions = create_user_contributions(project_results, user_tasks)
        logging.warning('created: %s' % user_contributions)
        delete_table_list.extend((tasks_duplicates_marked, user_tasks))

        # clean up database
        clean_up_database(delete_table_list)
        logging.warning('deleted: %s' % delete_table_list)
        output_tables.append(user_contributions)


    return output_tables

########################################################################################################################

if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    get_user_contributions(args.projects, args.results_table_name, args.task_table_name)

















