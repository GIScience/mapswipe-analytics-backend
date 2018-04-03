#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
import traceback

# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

import gc
import os

import logging
from get_projects import get_projects
from get_projects import check_projects
from get_projects import get_all_projects
from get_projects import save_projects_psql
from get_tasks import get_tasks
from get_tasks_completed_count import get_tasks_completed_count
from get_users import get_users
from get_results import get_results
from get_results import get_last_timestamp
import error_handling

# from memory_consumption import get_memory_consumption

from create_database_and_tables import create_database_and_tables
import time
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


########################################################################################################################

def run_import(user_project_list, timestamp, initial_setup):
    print(os.getcwd())

    # print('start run import: ', get_memory_consumption())

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    logging.warning('>>> start of script import.py')

    # check if the user provided an argument to set up the database
    if initial_setup:
        print('start the initial setup')
        logging.warning('start the initial setup')
        project_table_name, results_table_name, task_table_name = create_database_and_tables()
        users_table_name = 'users'
    else:
        # define some variables that are related to the configuration of the psql database
        project_table_name = 'projects'
        results_table_name = 'results'
        task_table_name = 'tasks'
        users_table_name = 'users'

    # get list of all project_ids if no list of projects is provided
    if not user_project_list:
        project_list = get_all_projects()
        project_list = check_projects(project_list)
        print('got all projects from firebase: ', project_list)
        logging.warning('got all projects from firebase: %s' % project_list)
    else:
        print('user provided project ids: ', user_project_list)
        logging.warning('user provided project ids: %s' % user_project_list)
        project_list = check_projects(user_project_list)

    if not project_list:
        print('there are no projects to process. stop here.')
        logging.warning('there are no projects to process. stop here.')
        sys.exit(0)

    # get project information
    new_projects, updated_projects, project_dict = get_projects(project_list, project_table_name)
    print('new projects in firebase: ', new_projects)
    logging.warning('new projects in firebase: %s' % new_projects)
    print('updated projects in firebase: ', updated_projects)
    logging.warning('updated projects in firebase: %s' % updated_projects)
    logging.warning('get_projects() was successfull')

    # check if the user provided a timestamp for the processing
    if timestamp:
        print('use timestamp provided by user')
        logging.warning('use timestamp provided by user')
        pass
    else:
        # print('get timestamp from database')
        timestamp = get_last_timestamp(results_table_name)
        # timestamp = 1509637220000
        # timestamp = int((time.time() - 3600)*1000) # this creates a timestamp representing the last 1 hour, in milliseconds
        print(timestamp)

    # get latest results, retrieve a list object with project id's of latest results
    changed_projects = get_results(results_table_name, timestamp, 500000)
    print('projects with new results: ', changed_projects)
    logging.warning('get_results() was successfull')

    # add the projects which need a update based on results to the ones based on contr. | progres | state
    # basidally merge the two lists with changed projects and remove the duplicates

    # merge updated projects from get_projects and get_results
    updated_projects = updated_projects + list(set(changed_projects) - set(updated_projects))
    # remove new projects from updated projects list
    # when importing new projects, we already get the latest completed count
    updated_projects = list(set(updated_projects) - set(new_projects))

    print('new projects: ', new_projects)
    logging.warning('new projects: %s' % new_projects)
    print('updated projects: ', updated_projects)
    logging.warning('updated projects: %s' % updated_projects)

    # get tasks for new projects
    get_tasks(new_projects, task_table_name)
    logging.warning('get_tasks() was successfull')

    # update projects that need an update
    # get latest completed count for projects that need an update
    get_tasks_completed_count(updated_projects, task_table_name)
    logging.warning('get_tasks_completed_count() was successfull')

    # save project data in psql database
    save_projects_psql(project_table_name, project_dict)
    print('saved project info to psql')
    logging.warning('saved project info to psql')
    # get user information
    get_users(users_table_name)
    logging.warning('get_users() was successfull')

    logging.warning('<<< end of script import.py')
    # print('after  garbage collection: ', get_memory_consumption())

    # garbage collection
    for i in range(2):
        n = gc.collect()

    return new_projects, updated_projects


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
            run_import(args.user_project_list, args.timestamp, args.initial_setup)
        except Exception as error:
            error_handling.send_error(error, 'import_workflow.py')

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
