#!/usr/bin/python3
#
# Author: B. Herfort, M. Reinmuth, 2017
############################################



from get_user_contributions import get_user_contributions
from get_all_contributions import get_all_contributions
from aggregate_contributions import aggregate_contributions
from psycopg2 import sql

import os
import logging

from auth import psqlDB


import sys
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')


import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--task_table_name', required=False, default='tasks', type=str,
                    help='the prefix of the tasks tables in your database')
parser.add_argument('-r', '--results_table_name', required=False, default='results', type=str,
                    help='the name of the results table in your database')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')
parser.add_argument('-e', '--enrich_all', dest='enrich_all', action='store_true',
                    help='flag to enrich all projects which are not yet enriched')
parser.add_argument('-pt', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the project table in your database')


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


def run_enrichment(projects, project_table_name, task_table_name, results_table_name, enrich_all):

    logging.basicConfig(filename='enrichment.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    logging.warning('>>> start of script enrich.py')

    if enrich_all:
        logging.warning('script started in -e mode: for every project in the provided tablename the enriched table will be created')
        projects = get_psql_projects(project_table_name)

   # print(projects)
    for project_id in projects:
        print('###########')
        position = projects.index(project_id)
        print('** start enrichment for project: %s | %s / %s **' % (project_id, position, str(len(projects))))
        logging.warning('start enrichment for project: %s' % project_id)
        project_id = [project_id]
        # first get user contributions
        user_contributions = get_user_contributions(project_id, results_table_name, task_table_name)
        #logging.warning('created: user_contributions_%s' % project_id[0])

        # second get all contributions
        all_contributions = get_all_contributions(project_id, task_table_name)
        #logging.warning('deleted: user_contributions_%s' % project_id[0])
        #logging.warning('created: contributions_%s' % project_id[0])

        # third aggregate contributions
        aggregated_tasks = aggregate_contributions(project_id)
        print('finished enrichment for project: %s | %s / %s' % (project_id[0], position, str(len(projects))))
        logging.warning('** finished enrichment for project: %s **' % project_id[0])
        print('###########')

    logging.warning('<<< end of script enrich.py')


########################################################################################################################

if __name__ == '__main__':
    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    run_enrichment(args.projects, args.project_table_name, args.task_table_name, args.results_table_name, args.enrich_all)

########################################################################################################################