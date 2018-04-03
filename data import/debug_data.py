#!/bin/python
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth
########################################################################################################################
#libs
import urllib2  # Require urllib for accessing hosted data
import json  # Require module json for handling json files
import csv # create csv
import sys  # Require module sys for reading program options
import os  # Require module os for file/directory handling
import time  # Require module time for run time measurement
import numpy #arrays
import psycopg2 # handle postgres
import MySQLdb #handle mysql db api
########################################################################################################################

class mapswipe_db(object):
    _db_connection = None
    _db_cur = None

    def __init__(self):
        self._db_connection = psycopg2.connect(dbname='mapswipe', user='mreinmuth', password='Marcel72983467', host='localhost')

    def query(self, query):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query)
        self._db_connection.commit()
        self._db_cur.close()
        return

    def retr_query(self, retr_query):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(retr_query)
        content = self._db_cur.fetchall()
        self._db_connection.commit()
        self._db_cur.close()
        return content

    def __del__(self):
        self._db_cur.close()
        self._db_connection.close()

def readURL(inputURL):
    url = inputURL
    req = urllib2.Request(url)
    resp = urllib2.urlopen(req)
    data = resp.read()
    return data
wkd ='/home/data/db_import'
os.chdir(wkd)

sql ='''    SELECT
            id
            FROM
            project
'''
db_con = mapswipe_db()
projects = db_con.retr_query(sql)
i=1
#get all projects and store them in a list
for project in projects:
    url = 'https://msf-mapswipe.firebaseio.com/groups/%s.json' % project[0]
    url_data = readURL(url)
    group_data = json.loads(url_data)
    print "data for project %s loaded %s/%s" % (project[0], i, len(projects))
    task_csv = 'task_%s.csv' % project[0]
    if os.path.isfile(os.getcwd() + '/' + task_csv):
        os.remove(os.getcwd() + '/' + task_csv)
    #open csv in writing mode
    tasks_file = open(task_csv, 'wb')
    #define columns
    tasks_fieldnames = [u'extern_id', u'projectId', u'taskX', u'taskY', u'taskZ', u'url', u'wkt', u'group_id']

    #define writer as dictonary based
    tasks_writer = csv.DictWriter(tasks_file, fieldnames = tasks_fieldnames)

    #create header in file
    tasks_writer.writeheader()
    #dict for storing data as rows
    row_tasks = {}
    row_groups = {}
    #loop through api data
    for key in group_data:
        #skip empty slots
        #empty var
        item = 0
        if key == None:
            continue
            #check how the dict is wrapped up
        if type(key) == dict:
            item = key
        #if type(task_data[key]) == dict:
        else:
            item = group_data[key]

        #row_groups['id'] = item['id']
        #row_groups['completedCount'] = item['completedCount']
        #row_groups['count'] = item['count']
        #row_groups['projectId'] = item['projectId']
        #row_groups['reportCount'] = item['reportCount']
        #write row to file

        #store all tasks of one group in new object
        tasks_of_group = item['tasks']
        #loop through these tasks
        for task in tasks_of_group:
            #get task properties and store them in the respective dictionary
            row_tasks['extern_id'] = str(task)
            row_tasks['projectId'] = tasks_of_group[task]['projectId']
            row_tasks['taskX'] = tasks_of_group[task]['taskX']
            row_tasks['taskY'] = tasks_of_group[task]['taskY']
            row_tasks['taskZ'] = tasks_of_group[task]['taskZ']
            row_tasks['url'] = tasks_of_group[task]['url']
            row_tasks['wkt'] = tasks_of_group[task]['wkt']
            row_tasks['group_id'] = item['id']
            #write row to file
            tasks_writer.writerow(row_tasks)
    tasks_file.close()
    print "data saved"
    sql_command = "COPY task (extern_id,"+str('"projectId","taskX","taskY","taskZ",')+str("url,wkt,group_id) FROM '/home/data/db_import/%s' "% task_csv) +str(' Delimiter ')+str("','")+str("CSV HEADER;")
    build_task_geom = '''INSERT INTO task_geom (task_id,task_geom,group_id, project_id)
                            SELECT extern_id as task_id,
                             ST_PolygonFromText(wkt, 4326) as task_geom,
                             group_id as group_id,
                             "projectId" as project_id
                             FROM task
                             WHERE "projectId"=%s;''' % project[0]
    db_con.query(sql_command)
    db_con.query(build_task_geom)
    #remove temporary csv files
    print "data geoms updated"
    os.remove(os.getcwd() + '/' + task_csv)
    print '%s project succesfull imported, neeext' % project[0]
    i = i + 1
print "finish"

    #tasks_file.close()
