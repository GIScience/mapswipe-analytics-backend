#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
import requests
from requests.auth import HTTPBasicAuth

# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

from auth import geoserver_config
import logging
import json
import argparse

# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-p', '--projects', nargs='+', required=None, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')


########################################################################################################################

def seed_layer(g, layer, type, style):
    auth = HTTPBasicAuth(g['username'], g['password'])
    url = '{baseurl}/gwc/rest/seed/{workspace}:{layer}.xml'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        layer=layer
    )

    # we use xml to send the data for the request
    # geoserver also provides json, but I'm not sure whether all functionality is available
    seed_request_xml = '''
    <seedRequest>
        <name>{layer}</name>
        <srs>
            <number>900913</number>
        </srs>
        <zoomStart>1</zoomStart>
        <zoomStop>14</zoomStop>
        <format>image/png</format>
        <type>{type}</type>
        <threadCount>1</threadCount>
        <parameters>
            <entry>
                <string>STYLES</string>
                <string>{style}</string>
            </entry>
        </parameters>
    </seedRequest>
    '''.format(
        layer=layer,
        type=type,
        style=style
    )
    r = requests.post(url, auth=auth, data=seed_request_xml)

    return r


def check_task_queue(g, layer):
    auth = HTTPBasicAuth(g['username'], g['password'])

    url = '{baseurl}/gwc/rest/seed/{workspace}:{layer}.json'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        layer=layer
    )
    response = requests.get(url, auth=auth)
    str_response = response.content.decode("utf-8")


    # load as json
    array = json.loads(str_response)
    # set pending false, for no queue
    pending = False
    if not array['long-array-array']:
        # no tasks are running, returning false
        return pending
    # loop through tasks of layer
    for task in array['long-array-array']:
        # check if task is pending
        if task[4] == 0:

            # no pending tasks
            pending = True

        else:
            # pending tasks
            pending = False

    return pending


def abort_pending_tasks(g, layer):
    auth = HTTPBasicAuth(g['username'], g['password'])

    url_get = '{baseurl}/gwc/rest/seed/{workspace}:{layer}.json'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        layer=layer
    )
    url_post = '{baseurl}/gwc/rest/seed/{workspace}:{layer}'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        layer=layer
    )

    data = [
        ('kill_all', 'pending'),
    ]

    response = requests.get(url_get, auth=auth)
    str_response = response.content.decode("utf-8")
    array = json.loads(str_response)

    response = []
    for task in array['long-array-array']:
        if task[4] == 0:
            taskid = task[3]
            rsp = requests.post(url_post, auth=auth, data=data)
            response.append([taskid, rsp])
    return response


def run_seed(projects):
    logging.basicConfig(filename='export.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # get geoserver config
    g = geoserver_config()

    responses = []

    for project_id in projects:
        # get layer name
        logging.warning('start seed layer for: %s' % project_id)
        layer = 'final_{}'.format(project_id)

        # set the type (seed, reseed, truncate)
        type = 'reseed'

        # check if there are seeding tasks are pending for layer
        if check_task_queue(g, layer) is True:
            # abort seeding
            logging.warning('tasks are running and or pending')
            aborted_tasks = abort_pending_tasks(g, layer)
            logging.warning('aborted tasks and response: %s' % aborted_tasks)
        # seed layer for the different styles
        for style in g['styles']:
            r = seed_layer(g, layer, type, style)
        if r.status_code == 200:
            logging.warning('sucessfully seeded layer: %s' % layer)
        else:
            logging.warning('failed to seed layer: %s' % layer)
            logging.warning('response status code: %s' % r.status_code)
            logging.warning('response status text: %s' % r.text)

        responses.append([project_id, r.status_code])
        logging.warning('finished seed layer for: %s' % project_id)

    return responses


########################################################################################################################

if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    responses = run_seed(args.projects)
    print(responses)
