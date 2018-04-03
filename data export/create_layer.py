#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
import requests
from requests.auth import HTTPBasicAuth
from psycopg2 import sql

# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

from auth import geoserver_config
from auth import psqlDB
from jdbc_type_to_java_type import jdbc_type_to_java_type


import logging

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-p', '--projects', nargs='+', required=False, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')
parser.add_argument('-pt', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the project table in your database')
parser.add_argument('-g', '--initialize_projects_layer', dest='initialize_projects_layer', action='store_true')


########################################################################################################################

def check_if_layer_exists(g, layer):
    # this function checks if a layer exists in geoserver, it does not check whether the table is defined in psql
    # make sure to pass the layer name as input, e.g. "final_5519", not only the project_id

    auth = HTTPBasicAuth(g['username'], g['password'])

    url = '{baseurl}/rest/workspaces/{workspace}/datastores/{datastore}/featuretypes/{layer}.html'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        datastore=g['datastore'],
        layer=layer
    )
    r = requests.get(url, auth=auth)

    if r.status_code == 200:
        #print('layer %s exists' % layer)
        return True
    else:
        #print('layer %s does not exist' % layer)
        return False


def get_attributes_from_table(table):
    p_con = psqlDB()
    sql_insert = '''
    SELECT
      a.attname as column_name
      ,format_type(a.atttypid, a.atttypmod) AS data_type
    FROM pg_attribute a
    JOIN pg_class b ON (a.attrelid = b.relfilenode)
    WHERE
      b.relname = %s
      AND
      a.attstattarget = -1;
    '''
    data = [table]
    attributes_raw = p_con.retr_query(sql_insert, data)
    return attributes_raw


def create_xml_attributes(attributes_raw):

    xml_attribute_template = '''
    <attribute>
      <name>{name}</name>
      <minOccurs>0</minOccurs>
      <maxOccurs>1</maxOccurs>
      <nillable>true</nillable>
      <binding>{type}</binding>
    </attribute>
    '''

    xml_attributes = ''

    for i in range(0, len(attributes_raw)):
        name = attributes_raw[i][0]
        type = jdbc_type_to_java_type(attributes_raw[i][1])
        xml_attributes = xml_attributes + xml_attribute_template.format(
            name = name,
            type = type
        )

    # print(xml_attributes)
    return xml_attributes


def get_layer_bbox(project_table_name, project_id):

    p_con = psqlDB()
    sql_insert = '''
    SELECT
      id
      ,st_XMin(extent)
      ,st_XMax(extent)
      ,st_YMin(extent)
      ,st_YMax(extent)
    FROM {}
    WHERE
      id = %s
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(project_table_name))
    data = [project_id]

    bbox_raw = p_con.retr_query(sql_insert, data)
    # the function returns a bounding box as a list of minx, maxx, miny, maxy
    bbox = [bbox_raw[0][1], bbox_raw[0][2], bbox_raw[0][3], bbox_raw[0][4]]
    return bbox


def create_layer(g, table_src, layer, bbox):
    auth = HTTPBasicAuth(g['username'], g['password'])

    # the url to which the request is send
    url = '{baseurl}/rest/workspaces/{workspace}/datastores/{datastore}/featuretypes/'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        datastore=g['datastore']
    )

    namespace_href = '{baseurl}/rest/namespaces/{namespace}.xml'.format(
        baseurl=g['baseurl'],
        namespace=g['namespace']
    )

    datastore_name = '{workspace}:{datastore}'.format(
        workspace=g['workspace'],
        datastore=g['datastore']
    )

    datastore_href = '{baseurl}/rest/workspaces/{workspace}/datastores/{datastore}.xml'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        datastore=g['datastore']
    )

    crs = '''
    GEOGCS["WGS 84", 
      DATUM["World Geodetic System 1984", 
      SPHEROID["WGS 84", 6378137.0, 298.257223563, AUTHORITY["EPSG","7030"]], 
      AUTHORITY["EPSG","6326"]], 
      PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]], 
      UNIT["degree", 0.017453292519943295], 
      AXIS["Geodetic longitude", EAST], 
      AXIS["Geodetic latitude", NORTH], 
      AUTHORITY["EPSG","4326"]]
    '''

    srs = 'EPSG:4326'

    # creta json data for request
    # we should rewrite this to xml

    raw_attributes = get_attributes_from_table(table_src)


    # delete extent geom for centroid layer
    if 'centroids' in layer:
        del raw_attributes[-2]


    # delete centroid geom for extent layer
    if 'extents' in layer:
        #del raw_attributes[1:-2]
        del raw_attributes[-1]


    xml_attributes = create_xml_attributes(raw_attributes)

    xml_data = '''
    <featureType>
      <name>{layer}</name>
      <nativeName>{table_src}</nativeName>
      <namespace>
        <name>{namespace}</name>
        <atom:link rel="alternate" href="{namespace_href}" type="application/xml"/>
      </namespace>
      <title>{layer}</title>
      <keywords>
        <string>features</string>
        <string>{layer}</string>
      </keywords>
      <nativeCRS>
        {crs}
      </nativeCRS>
      <srs>{srs}</srs>
      <nativeBoundingBox>
        <minx>{min_x}</minx>
        <maxx>{max_x}</maxx>
        <miny>{min_y}</miny>
        <maxy>{max_y}</maxy>
        <crs>{srs}</crs>
      </nativeBoundingBox>
      <latLonBoundingBox>
        <minx>{min_x}</minx>
        <maxx>{max_x}</maxx>
        <miny>{min_y}</miny>
        <maxy>{max_y}</maxy>
        <crs>{srs}</crs>
      </latLonBoundingBox>
      <projectionPolicy>FORCE_DECLARED</projectionPolicy>
      <enabled>true</enabled>
      <store class="dataStore">
        <name>{datastore_name}</name>
        <atom:link rel="alternate" href="{datastore_href}" type="application/xml"/>
      </store>
      <maxFeatures>0</maxFeatures>
      <numDecimals>0</numDecimals>
      <overridingServiceSRS>false</overridingServiceSRS>
      <skipNumberMatched>false</skipNumberMatched>
      <circularArcPresent>false</circularArcPresent>
      <attributes>
        {attributes}
      </attributes>
    </featureType>
    '''

    xml_data = xml_data.format(
        layer = layer,
        table_src = table_src,
        namespace = g['namespace'],
        namespace_href = namespace_href,
        datastore_name = datastore_name,
        datastore_href = datastore_href,
        min_x = bbox[0],
        max_x = bbox[1],
        min_y = bbox[2],
        max_y = bbox[3],
        crs = crs,
        srs = srs,
        attributes = xml_attributes
    )

    headers = {'Content-Type': 'text/xml'}
    r = requests.post(url, headers=headers, auth=auth, data=xml_data)
    return r, xml_data


def create_final_layer(g, project_id, project_table_name):
    # get layer name
    table_src = 'final_{}'.format(project_id)
    layer = 'final_{}'.format(project_id)

    # check if layer already exists to distinguish novell projects and projects which just need to be reseeded
    if check_if_layer_exists(g, layer):
        logging.warning('layer already exists: %s' % layer)
        print('layer already exists: %s' % layer)
        response = [project_id, 'layer already exists']
        return response
    # layer does not exist
    else:
        logging.warning('start create layer for: %s' % project_id)
        print('start create layer for: %s' % project_id)

        # get project bounding box
        bbox = get_layer_bbox(project_table_name, project_id)

        # create layer
        r, xml = create_layer(g, table_src, layer, bbox)

        # check the returned message and check response
        if r.status_code == 201:
            logging.warning('sucessfully created layer: %s' % layer)
        else:
            logging.warning('failed to create layer: %s' % layer)
            logging.warning('response status code: %s' % r.status_code)
            logging.warning('response status text: %s' % r.text)
            if r.status_code == 500:
                logging.warning(xml)

        response = ([project_id, r.status_code])
        logging.warning('finished create layer for: %s' % project_id)
        print('finished create layer for: %s' % project_id)
        return response


def create_projects_layers(project_table_name):
    # creates either two layers: one layer with centroids and another one with extents for all mapswipe projects

    # get geoserver config
    g = geoserver_config()

    responses = []
    # define world wide bbox since projects potentiall span the whole globe
    bbox_world = [-180.0, 180.0, -85.0, 85.0]
    # define layer names
    layer_names = ['{}_centroids'.format(project_table_name), '{}_extents'.format(project_table_name)]

    for layer_name in layer_names:
        # check if the layer already exists and skip if so
        if check_if_layer_exists(g, layer_name):
            responses.append([layer_name, 'layer already exists'])
            logging.warning('layer already exists: %s' % layer_name)
            print('layer already exists: %s' % layer_name)
        else:
            logging.warning('start create layer for: %s' % layer_name)
            print('start create layer for: %s' % layer_name)

            # call function to create centroid layer @ geoserver
            r, xml = create_layer(g, project_table_name, layer_name, bbox_world)

            # check if error appeared
            if r.status_code == 201:
                logging.warning('successfully created layer: %s' % layer_name)
            else:
                logging.warning('failed to create layer: %s' % layer_name)
                logging.warning('response status code: %s' % r.status_code)
                logging.warning('response status text: %s' % r.text)

            responses.append([layer_name, r.status_code])
            logging.warning('finished create layer for: %s' % layer_name)
            print('finished create layer for: %s' % layer_name)

    return responses


def run_create(projects, project_table_name):
    # this function will create the geoserver layers for specified projects
    # this functions does not create a layer for project extents of all projects

    logging.basicConfig(filename='export.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    # get geoserver config
    g = geoserver_config()
    responses = []

    # first step create the layers for all projects specified:
    if projects:
        for project_id in projects:
            response = create_final_layer(g, project_id, project_table_name)
            responses.append(response)
    else:
        print('no projects given for create layer')

    return responses


########################################################################################################################

if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    responses = []

    resp = run_create(args.projects, args.project_table_name)
    responses.append(resp)

    # second step create layer with project extents and project centroids
    if args.initialize_projects_layer:
        resp = create_projects_layers(args.project_table_name)
        responses.append(resp)

    print(responses)