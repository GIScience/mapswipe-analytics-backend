#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys, os

from distutils.dir_util import mkpath
import traceback

# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

from create_layer import run_create
from create_layer import create_projects_layers
from seed_layer import run_seed
from export_stats import run_stats_export
from wfs_to_geojson import run_wfs_export
from auth import psqlDB
from psycopg2 import sql
import error_handling


import logging

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')
parser.add_argument('-pt', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the project table in your database')
parser.add_argument('-l', '--loop', dest='loop', action='store_true',
                    help='if loop is set, the import will be repeated several times. You can specify the behaviour using --sleep_time and/or --max_iterations.')
parser.add_argument('-s', '--sleep_time', required=False, default=None, type=int,
                    help='the time in seconds for which the script will pause in beetween two imports')
parser.add_argument('-m', '--max_iterations', required=False, default=None, type=int,
                    help='the maximum number of imports that should be performed')
parser.add_argument('-o', '--output_path', required=True, default=None, type=str,
                    help='path only where to export the data.')
parser.add_argument('-e', '--export_all', dest='export_all', action='store_true',
                    help='flag to export all projects which are not yet exported')


########################################################################################################################


def get_psql_projects(project_table_name):
    psq_projects = []

    p_con = psqlDB()
    # each row is converted to json format using psql function row_to_json
    sql_insert = '''
        SELECT
          id
        FROM
          {}
        WHERE
          corrupt is False
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(project_table_name))

    retr_data = p_con.retr_query(sql_insert, None)
    p_con.close()

    for id in retr_data:
        psq_projects.append(id[0])

    return psq_projects


def get_all_non_corrupt_projects(projects):
    # this function looks for the project ids of all projecst in our database
    # the projects need to be non-corrupt
    # the projects need to be enrichted (e.g. final_5519 table must be existing)
    # input is the project ids list, output is the project ids list without projects that are corrupt or not existing

    p_con = psqlDB()
    sql_insert = '''
    SELECT
      p.id
      ,i.table_name
    FROM
      projects as p, information_schema.tables as i  
    WHERE
      not p.corrupt
      AND
      i.table_schema = 'public'
      AND
      left(i.table_name, 6) = 'final_' 
      AND
      ltrim(i.table_name, 'final_')::int = p.id
    ORDER BY
      p.id
    '''

    retr = p_con.retr_query(sql_insert, None)
    existing_projects = []
    for i in range(0, len(retr)):
        existing_projects.append(retr[i][0])

    # intersect existing projects and input projects
    filtered_projects = list(set(existing_projects).intersection(set(projects)))
    print('filtered projects. original input: %s, remaining in list: %s' % (projects, filtered_projects))
    logging.warning('filtered projects. original input: %s, remaining in list: %s' % (projects, filtered_projects))
    return filtered_projects


def run_export(projects, project_table_name, output_path, export_all):

    logging.basicConfig(filename='export.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    logging.warning('>>> start of script export_workflow.py')

    if export_all:
        logging.warning('script started in -e mode: for every project in the provided tablename the layer will be created and seeded')
        projects = get_psql_projects(project_table_name)

    # test if projects really exist in our database
    projects = get_all_non_corrupt_projects(projects)

    # if flag is set, lsit of projects gets overwritten with every project in psql db

    # do the export
    # first step create layers, the function will check if layers already exist
    run_create(projects, project_table_name)

    # second step seed layers
    run_seed(projects)

    # third step create project extents and centroids, if not already there
    # the functions already checks if the layera are existing
    create_projects_layers(project_table_name)


    # export wfs as geojson to get the projects centroid geometries and extent

    layer_names = ['{}_centroids'.format(project_table_name), '{}_extents'.format(project_table_name)]

    # check if a data folder already exists
    if not os.path.exists(output_path):
        mkpath(output_path)
    logging.warning('Added the output folder')


    for layer_name in layer_names:
        path_output = '{}/{}.geojson'.format(output_path, layer_name)
        run_wfs_export(layer_name, path_output)


    # export general and detailed stats for every project
    stats_table_name = 'stats_general'

    path_output = '{}/{}.json'.format(output_path, stats_table_name)
    run_stats_export(stats_table_name, path_output)


########################################################################################################################
if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    # check whether arguments are correct
    if args.loop and (args.max_iterations is None):
        parser.error('if you want to loop the script please provide number of maximum iterations.')
    elif args.loop and (args.sleep_time is None):
        parser.error('if you want to loop the script please provide a sleep interval.')

    # create a variable that counts the number of imports
    counter = 1
    x = 1

    while x>0:

        print(' ')
        print('###### ###### ###### ######')
        print('###### iteration: %s ######' % counter)
        print('###### ###### ###### ######')


        # this runs the script and sends an email if an error happens within the execution
        try:
            run_export(args.projects, args.project_table_name, args.output_path, args.export_all)
        except Exception as error:
            error_handling.send_error(error, 'processing_workflow.py')

        # check if the script should be looped
        if args.loop:
            if args.max_iterations > counter:
                counter = counter + 1
                print('export finished. will pause for %s seconds' % args.sleep_time)
                x = 1
                time.sleep(args.sleep_time)
            else:
                x = 0
                # print('import finished and max iterations reached. stop here.')
                print('export finished and max iterations reached. sleeping now.')
                time.sleep(args.sleep_time)
        # the script should run only once
        else:
            print("Don't loop. Stop after the first run.")
            x = 0