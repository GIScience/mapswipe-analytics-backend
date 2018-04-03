#!/usr/bin/python3
#
# Author: B. Herfort, M. Reinmuth, 2017
############################################

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
parser.add_argument('-p', '--projects', nargs='+', required=True, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')

def create_aggregation(project_id, contributions_table):
    p_con = psqlDB()

    input_table_name = contributions_table
    output_table_name = 'final_{}'.format(project_id)

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          b.*
          ,CASE
            WHEN b.completed_count = 1 THEN 1.0
            ELSE (
            round(((1.0 / (b.completed_count::numeric * (b.completed_count::numeric - 1.0)))
          *
          (
          ((b.yes_count::numeric ^ 2.0) - b.yes_count::numeric)
          +
          ((b.maybe_count::numeric ^ 2.0) - b.maybe_count::numeric)
          +
          ((b.badimage_count::numeric ^ 2.0) - b.badimage_count::numeric)
          +
          ((b.no_count::numeric ^ 2.0) - b.no_count::numeric)
          )),3)
          ) END as agreement
          ,round(((b.yes_count::numeric + b.maybe_count::numeric)/b.completed_count::numeric),3)
           as msi
          ,round((b.no_count::numeric/b.completed_count::numeric),3)
           as no_si
        FROM(
        SELECT
          c.taskid
          ,c.projectid
          ,CASE
            WHEN c.completedcount >= count(c.taskid) THEN c.completedcount
            ELSE count(taskid)
          END as completed_count
          ,count(c.taskid)
          -- no count
          ,completedcount -
           sum(
            CASE
              WHEN result > 0 THEN 1
              ELSE 0
            END) AS no_count
          -- yes count
          ,sum(
            CASE
              WHEN result = 1 THEN 1
              ELSE 0
            END) AS yes_count
          -- maybe count
          ,sum(
            CASE
              WHEN result = 2 THEN 1
              ELSE 0
            END) AS maybe_count
          -- bad image count
          ,sum(
            CASE
              WHEN result = 3 THEN 1
              ELSE 0
            END) AS badimage_count
          -- don't forget the geometry
          ,c.geo
        FROM
          {} as c
        GROUP By
          c.taskid
          ,c.projectid
          ,c.completedcount
          ,c.geo) as b
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))
    data = [str(project_id)]

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name

########################################################################################################################

def aggregate_contributions(projects):
    # create an empty list for table names that will be deleted in the end

    logging.basicConfig(filename='enrichment.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    output_tables = []

    for project_id in projects:
        contributions_table = 'contributions_{}'.format(project_id)
        aggregated_tasks = create_aggregation(project_id, contributions_table)
        logging.warning('created: %s' % aggregated_tasks)
        output_tables.append(aggregated_tasks)

    return output_tables

########################################################################################################################

if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    aggregate_contributions(args.projects)
