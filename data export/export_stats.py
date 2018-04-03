#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

from auth import psqlDB
from psycopg2 import sql
import ntpath
import os, sys
import json

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument('-t', '--table_export', required=True, default='stats_general', type=str,
                    help='the name of the table you want to export')

parser.add_argument('-o', '--output_path', required=True, default=None, type=str,
                    help='path and filename where to export the table.')


def create_stats_general_view():
    p_con = psqlDB()
    # sql for creating the overall stats
    sql_insert = '''CREATE OR REPLACE VIEW stats_general AS
SELECT count(a.*) AS project_total,
    sum(
        CASE
            WHEN a.progress = 100 THEN 1
            ELSE 0
        END) AS project_finished,

    sum(
        CASE
            WHEN a.progress < 100 AND a.state = 3 THEN 1
            ELSE 0
        END) AS project_inactive,

    sum(
        CASE
            WHEN a.state = 0 AND a.progress < 100 THEN 1
            ELSE 0
        END) AS project_active,

    ( SELECT count(*) AS count
           FROM users) AS user_total,

    sum(a.contributors) / count(*) AS user_avg_project,

    ( SELECT id
           FROM projects
          GROUP BY id
          ORDER BY (sum(st_area(extent))) DESC
         LIMIT 1) AS largest_project,

    ( SELECT round(sum(st_area(extent::geography))::numeric / 1000000::numeric, 3) AS round
           FROM projects
          GROUP BY id
          ORDER BY (sum(st_area(extent))) DESC
         LIMIT 1) AS largest_area,

    ( SELECT id
           FROM projects
          GROUP BY id
          ORDER BY (sum(st_area(extent)))
         LIMIT 1) AS smallest_project,

    ( SELECT round(sum(st_area(extent::geography))::numeric / 1000000::numeric, 3) AS round
           FROM projects
          GROUP BY id
          ORDER BY (sum(st_area(extent)))
         LIMIT 1) AS smallest_area,

    ( SELECT sum(round(st_area(extent::geography)::numeric / 1000000::numeric, 3)) AS sum
           FROM projects ) AS total_km_sq_covered


   FROM 
   		projects a;'''

    p_con.query(sql_insert, None)
    print('created view: stats_general')
    p_con.close()


def run_stats_export(table_name, path):
    # open db connection
    p_con = psqlDB()


    # check if stats_general view is already created in db and ready to be querried
    check_stats_view = '''
            SELECT EXISTS (
              SELECT 1
              FROM   information_schema.tables 
              WHERE  table_schema = 'public'
              AND    table_name = 'stats_general'
   );'''
    check_view = p_con.retr_query(check_stats_view, None)
    #p_con.close()

    if check_view[0][0] == False:
        create_stats_general_view()
        print('view does not exist and will be created')
    #backup_path = os.getcwd()
    # seperate location from file
    try:
        head, tail = ntpath.split(path)
    except:
        print('slicing of path/file failed')
        print('path: %s' % path)
        print('head: %s' % head)
        print('tail: %s' % tail)
    # check if path is provided
    if head:
        os.chdir(head)
        print('changed dir')

    #  p_con2 = psqlDB()

    # define query to stas view
    sql_insert = '''
            SELECT
              row_to_json({})
            FROM
              {}
        '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(table_name),sql.Identifier(table_name))
    # execute query
    retr_data = p_con.retr_query(sql_insert, None)
    # delete db connection
    p_con.close()
    #w write date to file as json
    with open(tail, 'w') as fo:
        json.dump(retr_data[0][0], fo, sort_keys = False, indent = 2)


########################################################################################################################

if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')
    run_stats_export(args.table_export, args.output_path)

