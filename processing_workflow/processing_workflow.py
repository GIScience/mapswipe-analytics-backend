#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
import time
import logging
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')
sys.path.insert(0, '../data import')
sys.path.insert(0, '../data enrichment')
sys.path.insert(0, '../data export')

from import_workflow import run_import
from enrichment_workflow import run_enrichment
from export_workflow import run_export
import error_handling


import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--timestamp', required=False, default=None, type=int,
                    help='the timestamp to extract the results')
parser.add_argument('-i', '--initial_setup', dest='initial_setup', action='store_true')
parser.add_argument('-l', '--loop', dest='loop', action='store_true',
                    help='if loop is set, the import will be repeated several times. You can specify the behaviour using --sleep_time and/or --max_iterations.')
parser.add_argument('-s', '--sleep_time', required=False, default=None, type=int,
                    help='the time in seconds for which the script will pause in beetween two imports')
parser.add_argument('-m', '--max_iterations', required=False, default=None, type=int,
                    help='the maximum number of imports that should be performed')

parser.add_argument('-p', '--user_project_list', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')
parser.add_argument('-tt', '--task_table_name', required=False, default='tasks', type=str,
                    help='the prefix of the tasks tables in your database')
parser.add_argument('-rt', '--results_table_name', required=False, default='results', type=str,
                    help='the name of the results table in your database')
parser.add_argument('-pt', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the projects table in your database')
parser.add_argument('-o', '--output_path', required=False, default='data', type=str,
                    help='path only where to export the data.')

########################################################################################################################

def run_processing(user_project_list, timestamp, initial_setup, project_table_name, task_table_name, results_table_name, output_path):

    logging.basicConfig(filename='processing.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    logging.warning('***##### start of script processing_workflow.py #####***')


    # first import
    logging.warning('start import')
    new_projects, updated_projects = run_import(user_project_list, timestamp, initial_setup)
    logging.warning('finish import')

    # then enrich
    logging.warning('start enrich')
    projects = list(set().union(new_projects, updated_projects))
    run_enrichment(projects, project_table_name, task_table_name, results_table_name, None)
    logging.warning('finish enrich')

    # finally export
    logging.warning('start export')
    run_export(projects, project_table_name, output_path, None)
    logging.warning('finish export')
    # will be added later
    logging.warning('***##### end of script processing_workflow.py #####***')

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

    while x > 0:

        print(' ')
        print('###### ###### ###### ######')
        print('###### iteration: %s ######' % counter)
        print('###### ###### ###### ######')

        # this runs the script and sends an email if an error happens within the execution
        try:
            run_processing(args.user_project_list, args.timestamp, args.initial_setup, args.project_table_name, args.task_table_name, args.results_table_name, args.output_path)
        except Exception as error:
            error_handling.send_error(error, 'processing_workflow.py')

        # check if the script should be looped
        if args.loop:
            if args.max_iterations > counter:
                counter = counter + 1
                print('import finished. will pause for %s seconds' % args.sleep_time)
                x = 1
                time.sleep(args.sleep_time)
            else:
                x = 0
                # print('import finished and max iterations reached. stop here.')
                print('import finished and max iterations reached. sleeping now.')
                time.sleep(args.sleep_time)
        # the script should run only once
        else:
            print("Don't loop. Stop after the first run.")
            x = 0
