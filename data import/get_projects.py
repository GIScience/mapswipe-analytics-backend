#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import sys
# add some files in different folders to sys.
# these files can than be loaded directly
sys.path.insert(0, '../cfg')

import datetime
import json  # Require module json for handling json files
import os
import time
import logging
from fnmatch import fnmatch, fnmatchcase  # match strings

import ogr
from psycopg2 import sql

from auth import firebase_admin_auth
from auth import psqlDB

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', '--project_table_name', required=False, default='projects', type=str,
                    help='the name of the projects table in your database')
parser.add_argument('-p', '--projects', nargs='+', required=False, default=None, type=int,
                    help='project id of the project to process. You can add multiple project ids.')


def check_projects(project_list):
    firebase = firebase_admin_auth()
    fb_db = firebase.database()
    for project_id in project_list:
        # we need to add an try because the user may provide a project id that does not exist
        project_val = fb_db.child('projects').child(project_id).shallow().get().val()
        if project_val == None:
            print('the project id is not in firebase: ', project_id)
            project_list.remove(project_id)
        elif len(project_val) < 12:
            print('the project missed critical information in firebase: ', project_id)
            project_list.remove(project_id)
        else:
            pass

    del fb_db
    return project_list


def get_all_projects():
    ### this functions gets the IDs of all projects in firebase
    ### and returns a list

    firebase = firebase_admin_auth()
    fb_db = firebase.database()

    project_list = []

    all_projects = fb_db.child("projects").shallow().get().val()
    for project_id in all_projects:
        project_list.append(int(project_id))

    del fb_db
    return project_list


def get_new_projects(project_list):
    ### this functions gets the latest project information from firebase
    ### and returns a dict

    firebase = firebase_admin_auth()
    fb_db = firebase.database()


    project_dict = {}
    timestamp = datetime.datetime.now().isoformat()

    for project_id in project_list:
        project_val = fb_db.child("projects").child(project_id).get().val()
        project_val["last_check"] = timestamp
        # check whether all information exist for the project, skip if no import key
        project_dict[str(project_id)] = project_val

    project_dict = json.dumps(project_dict)
    project_dict = json.loads(project_dict)

    del fb_db
    return project_dict


def get_existing_projects(project_list, project_table_name):
    ### this function gets all project information that is already stored in the psql database
    ### and returns a json object
    existing_projects = {}

    p_con = psqlDB()
    # each row is converted to json format using psql function row_to_json
    sql_insert = '''
        SELECT
          row_to_json({})
        FROM
          {}
        WHERE
          id = ANY(%s)
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(project_table_name), sql.Identifier(project_table_name))
    data = (project_list,)

    retr_data = p_con.retr_query(sql_insert, data)
    p_con.close()

    for i in range(0,len(retr_data)):
        project_id = retr_data[i][0]["id"]
        existing_projects[project_id] = retr_data[i][0]

    existing_projects = json.dumps(existing_projects)
    existing_projects = json.loads(existing_projects)
    return existing_projects


def remove_error_projects(existing_projects, new_projects):
    ### this function removes entries from the projects
    # some projects in firebase are faulty, or only for testing
    # these projects will be removed
    project_with_errors = [982, 1602, 2433]
    try:
        for i in project_with_errors:
            del new_projects[str(i)]
    except:
        pass

    return new_projects


def compare_projects(existing_projects,new_projects):
    ### this function looks for projects that changed
    # go through all new projects and check whether something changed or the project is new
    # we look for changes in the following attributes:
    # contributors, progress
    for i in list(new_projects):
        try:
            new_projects[i]['isNew'] = 0
            new_projects[i]['corrupt'] = existing_projects[i]['corrupt']
            if new_projects[i]['contributors'] > existing_projects[i]['contributors']:
                new_projects[i]['needUpdate'] = 1
            elif new_projects[i]['progress'] > existing_projects[i]['progress']:
                new_projects[i]['needUpdate'] = 1
            elif new_projects[i]['state'] != existing_projects[i]['state']:
                new_projects[i]['needUpdate'] = 1
            else:
                new_projects[i]['needUpdate'] = 0
        except:
            new_projects[i]['isNew'] = 1
            new_projects[i]['needUpdate'] = 1
            new_projects[i]['corrupt'] = False

    return new_projects


def insert_project_info(project_table_name, new_project):
    p_con = psqlDB()

    sql_insert = '''
        INSERT INTO {} Values(
          %s -- id
          ,%s -- contributors,
          ,%s -- groupAverage
          ,%s -- image
          ,%s -- importKey
          ,%s -- isFeatured
          ,%s -- lookFor
          ,%s -- name
          ,%s -- progress,
          ,%s -- projectDetails
          ,%s -- state
          ,%s -- verificationCount
          ,%s -- corrupt
          ,%s -- last_check
          ,ST_GeometryFromText(%s, 4326) -- project geometry 
          -- this is a workaround to create centroids that can be exported as geojson and do not produce infinite numbers error
          ,ST_MakePoint(round(ST_X(ST_CENTROID(ST_GeometryFromText(%s, 4326)))::numeric,6), round(ST_Y(ST_CENTROID(ST_GeometryFromText(%s, 4326)))::numeric,6)) -- project centroid geometry
          )
    '''
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(project_table_name))
    data = [
        new_project['id'],
        new_project['contributors'],
        new_project['groupAverage'],
        new_project['image'],
        new_project['importKey'],
        new_project['isFeatured'],
        new_project['lookFor'],
        new_project['name'],
        new_project['progress'],
        new_project['projectDetails'],
        new_project['state'],
        new_project['verificationCount'],
        new_project['corrupt'],
        new_project['last_check'],
        new_project['geometry'],
        new_project['geometry'],
        new_project['geometry']
    ]

    p_con.query(sql_insert, data)


def update_project_info(project_table_name, new_project):
    p_con = psqlDB()

    # update the information in the table
    # we don't update all columns, but just those that might change
    sql_insert = '''
        UPDATE {}
        SET
          contributors = %s
          ,progress = %s
          ,state = %s
          ,isFeatured = %s
          ,corrupt = %s
          ,lastcheck = %s
        WHERE
          id = %s
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(project_table_name))
    data = [
        new_project['contributors'],
        new_project['progress'],
        new_project['state'],
        new_project['isFeatured'],
        new_project['corrupt'],
        new_project['last_check'],
        new_project['id']
    ]
    p_con.query(sql_insert, data)

    # delete database connection
    p_con.close()

    return


def save_projects_psql(project_table_name, new_projects):
    ### this functions saves the new project information to psql
    p_con = psqlDB()

    # insert new values for each project
    for i in list(new_projects):
        if new_projects[i]['isNew'] == 1:
            insert_project_info(project_table_name, new_projects[i])
            print('insert data in psql for new project:', i)
        # we only delete and insert information for projects that need an update
        elif new_projects[i]['needUpdate'] == 1:
            update_project_info(project_table_name, new_projects[i])
            print('update data in psql for updated project:', i)

    return


def get_project_geom(project_id, project_import_key):

    ### this functions gets the geometry of a project from firebase
    ### the geometry will be obtained from the imports table in firebase

    firebase = firebase_admin_auth()
    fb_db = firebase.database()

    # get import-key for the project
    #project_import_key = fb_db.child("projects").child(str(project_id)).child("importKey").get().val()
    # get kml geometry from firebase imports table
    kml_geom = fb_db.child("imports").child(project_import_key).child("kml").get().val()

    # we need to check whether there is any kml in the firebase table
    if kml_geom is None:
        return None
    else:
        # write valid kml string to file
        temp_file = '{}_extent.kml'.format(project_id)
        temp_file_obj = open(temp_file, 'w', encoding='utf-8')
        temp_file_obj.write(kml_geom)
        temp_file_obj.close()

        adresse = temp_file
        driver = ogr.GetDriverByName('KML')
        datasource = driver.Open(adresse)
        layer = datasource.GetLayer()
        nbFeat = layer.GetFeatureCount()

        #create new multipolygon geometry
        project_geom = ogr.Geometry(ogr.wkbMultiPolygon)

        # we check how many features are in the layer
        # most mapswipe projects will contain only 1 feature, but this may change in the future
        if nbFeat == 1:
            feature = layer.GetFeature(0)
            feat_geom = feature.GetGeometryRef()
            # add this to remove z-coordinates, z-coordinates produced a wrong geometry type
            feat_geom.FlattenTo2D()

            if feat_geom.GetGeometryType() == ogr.wkbPolygon:
                project_geom.AddGeometry(feat_geom)
            elif feat_geom.GetGeometryType() == ogr.wkbMultiPolygon:
                project_geom = feat_geom


        else:
            # get geometry for each feature
            for i in range(0, nbFeat):
                feature = layer.GetFeature(i)
                feat_geom = feature.GetGeometryRef()

                if feat_geom.GetGeometryType() == ogr.wkbPolygon:

                    project_geom.AddGeometry(feat_geom)
                # if the geometry is a multipolygon, we loop through each individual polygon
                elif feat_geom.GetGeometryType() == ogr.wkbMultiPolygon:

                    for new_feat_geom in feat_geom:
                        project_geom.AddGeometry(new_feat_geom)

        # check thevalidity of geoms
        if not check_project_geometry(layer):
            corrupt = True
            project_geom = None
        else:
            # convert geometry to wkt
            corrupt = False
            project_geom = project_geom.ExportToWkt()

        # close data source and remove kml file
        datasource.Destroy()
        os.remove(temp_file)
        return project_geom, corrupt


def check_project_geometry(layer):
    # this function checks whether the project geometry is valid

    # check if layer is empty
    if layer.GetFeatureCount() < 1:
        err = 'empty file. No geometries provided'
        print(err)
        return False

    # check if more than 1 geometry is provided
    if layer.GetFeatureCount() > 1:
        err = 'Input file contains more than one geometry. Make sure to provide exact one input geometry.'
        print(err)
        return False

    # check if the input geometry is a valid polygon
    for feature in layer:
        feat_geom = feature.GetGeometryRef()
        geom_name = feat_geom.GetGeometryName()

        if not feat_geom.IsValid():
            err = 'geometry is not valid: %s. Tested with IsValid() ogr method. probably self-intersections.' % geom_name
            return False

        # we accept only POLYGON or MULTIPOLYGON geometries
        if geom_name != 'POLYGON' and geom_name != 'MULTIPOLYGON':
            err = 'invalid geometry type: %s. please provide "POLYGON" or "MULTIPOLYGON"' % geom_name
            print(err)
            return False

    del layer
    print('geometry is correct')
    return True


def update_project_extent(new_projects):
    # get new projects
    for project_id in list(new_projects):
        if new_projects[project_id]['isNew'] == 1:

            try:
                new_projects[project_id]['geometry'], new_projects[project_id]['corrupt'] = get_project_geom(project_id, new_projects[project_id]['importKey'])
            except:
                print('something went wrong with get project geom')
                new_projects[project_id]['geometry'] = None
                new_projects[project_id]['corrupt'] = True

    return new_projects


########################################################################################################################

def get_projects(project_list, project_table_name):

    logging.basicConfig(filename='import.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )
    # record time
    starttime = time.time()

    ## complete workflow to Import Projects ##

    # get list of all project_ids if no list of projects is provided
    if not project_list:
        project_list = get_all_projects()
        print('got all projects from firebase: ', project_list)
        logging.warning('got all projects from firebase: %s' % project_list)
    else:
        print('user provided project ids: ', project_list)
        logging.warning('user provided project ids: %s' % project_list)
        project_list = check_projects(project_list)

    # get all projects from firebase
    project_dict = get_new_projects(project_list)
    print('returned new project information')
    logging.warning('returned new project information')

    # get all existing projects from psql
    existing_projects = get_existing_projects(project_list, project_table_name)
    print('returned existing projects')
    logging.warning('returned existing projects')

    # compare existing projects and latest projects
    project_dict = compare_projects(existing_projects, project_dict)
    print('compared projects')
    logging.warning('compared projects')

    # add extent geometry to new projects
    project_dict = update_project_extent(project_dict)
    print('added extent to projects')
    logging.warning('added extent to projects')

    new_projects = []
    updated_projects = []

    for project_id in list(project_dict):
        if project_dict[project_id]['isNew'] == 1:
            new_projects.append(int(project_id))
        elif project_dict[project_id]['needUpdate'] == 1:
            updated_projects.append(int(project_id))
        else:
            pass

    # calc process time
    endtime = time.time() - starttime
    print('finished get projects, %f sec.' % endtime)
    logging.warning('finished get projects, %f sec.' % endtime)

    return new_projects, updated_projects, project_dict

########################################################################################################################
if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    new_projects, updated_projects, project_dict = get_projects(args.projects, args.project_table_name)

    print('new projects: ', new_projects)
    print('updated projects: ', updated_projects)
