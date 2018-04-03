#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

import requests
from requests.auth import HTTPBasicAuth
from auth import geoserver_config

import logging

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')
parser.add_argument('-g', '--flag_delete_initial_layers', dest='flag_delete_initial_layers', action='store_true')

parser.add_argument('-pt', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the project table in your database')


########################################################################################################################

def delete_layer(g, layer):
    auth = HTTPBasicAuth(g['username'], g['password'])

    url = '{baseurl}/rest/layers/{layer}.xml'.format(
        baseurl=g['baseurl'],
        layer=layer
    )

    url_2 = '{baseurl}/rest/workspaces/{workspace}/datastores/{datastore}/featuretypes/{layer}.xml'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        datastore=g['datastore'],
        layer=layer
    )

    r = requests.delete(url, auth=auth)
    r_2 = requests.delete(url_2, auth=auth)

    return r, r_2

def run_delete(projects, flag_delete_initial_layers, project_table_name):

    print(projects)

    logging.basicConfig(filename='export.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # get geoserver config
    g = geoserver_config()
    responses = []

    if flag_delete_initial_layers:

        layer_names = ['{}_centroids'.format(project_table_name), '{}_extents'.format(project_table_name)]
        # define layer names
        for layer_name in layer_names:

            logging.warning('start delete layer for: %s' % layer_name)
            print('start delete layer for: %s' % layer_name)

            # call function to delete centroid layer @ geoserver
            ri, ri_2 = delete_layer(g, layer_name)

            # check response
            if ri.status_code == 200:
                logging.warning('sucessfully deleted layer: %s' % layer_name)
            else:
                logging.warning('failed to delete layer: %s' % layer_name)
                logging.warning('response status code: %s' % ri.status_code)
                logging.warning('response status text: %s' % ri.text)

            if ri_2.status_code == 200:
                logging.warning('sucessfully deleted featuretype for layer: %s' % layer_name)
            else:
                logging.warning('failed to delete featuretype for layer: %s' % layer_name)
                logging.warning('response status code: %s' % ri_2.status_code)
                logging.warning('response status text: %s' % ri_2.text)

            responses.append([layer_name, ri.status_code, ri_2.status_code])
            logging.warning('finish delete layer for: %s' % layer_name)
            print('finished delete layer for: %s' % layer_name)



    if projects:
        for project_id in projects:
            # get layer name
            logging.warning('start delete layer for: %s' % project_id)
            print('start delete layer for: %s' % project_id)
            layer = 'final_{}'.format(project_id)

            # delete layer
            r, r_2 = delete_layer(g, layer)

            # check response
            if r.status_code == 200:
                logging.warning('sucessfully deleted layer: %s' % layer)
            else:
                logging.warning('failed to delete layer: %s' % layer)
                logging.warning('response status code: %s' % r.status_code)
                logging.warning('response status text: %s' % r.text)

            if r_2.status_code == 200:
                logging.warning('sucessfully deleted featuretype for layer: %s' % layer)
            else:
                logging.warning('failed to delete featuretype for layer: %s' % layer)
                logging.warning('response status code: %s' % r_2.status_code)
                logging.warning('response status text: %s' % r_2.text)

            responses.append([project_id, r.status_code, r_2.status_code])
            logging.warning('finish delete layer for: %s' % project_id)
            print('finished delete layer for: %s' % project_id)
    else:
        print('no projects given for delete layer')
    return responses

########################################################################################################################

if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    responses = run_delete(args.projects, args.flag_delete_initial_layers, args.project_table_name)
    print(responses)