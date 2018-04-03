#!/bin/python
#
# Author: B. Herfort, M. Reinmuth, 2017
############################################

import sys
import os
import time
import psycopg2  # handle postgres
from psycopg2 import sql
from auth import psqlDB




def select_data_for_project(project_id):
    ####################
    #  select data from the big results and task_geom tables in the database
    ####################

    ####################
    #  1. get all result for project X
    ####################

    p_con = psqlDB()

    input_table_name = 'result'
    output_table_name = str(project_id) + '_results'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      *
    FROM
      {}
    WHERE
      "projectId" = %s'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))

    data = [int(project_id)]

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    ####################
    #  2. get all tasks for project X
    ####################
    p_con = psqlDB()

    input_table_name = 'task_geom'
    output_table_name_2 = str(project_id) + '_task_geom'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      *
    FROM
      {}
    WHERE
      project_id = %s'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name_2), sql.Identifier(output_table_name_2),
                                            sql.Identifier(input_table_name))

    data = [int(project_id)]
    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name_2)
    del p_con

    return output_table_name, output_table_name_2


def create_groups_per_user(tasks, raw_results):
    ####################
    # input are two tables
    #   tasks --> this table contains all tasks for the project
    #   raw_results --> this table contains the raw results from the mapswipe app for the project
    #
    # the overall workflow in this function has 2 steps
    #   1. Get tasks that are contained in two different groups for the project
    #   2. Identify all groups a single user potentially worked on
    #   3. Clean the potential list of groups by filtering out unreliable groups
    #      these 'unreliable' groups are groups where the user only worked in the overlapping area,
    #      this means that we don't know for sure for which group the user submitted the result
    #
    ####################


    ####################
    #  1. get tasks and 'edge tasks'
    ####################
    p_con = psqlDB()

    input_table_name = tasks
    output_table_name = tasks + '_edge'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
        SELECT
          b.*
          ,e.count
        FROM
          {} as b
        LEFT JOIN
          (SELECT
            task_id
            ,count(task_id)
           FROM 
             {}
            GROUP BY
             task_id, project_id
           ) as e ON
           (e.task_id = b.task_id)
           '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name), sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    task_geom_edge = output_table_name

    ####################
    #  2. get all potential groups
    ####################
    p_con = psqlDB()

    input_table_name_a = task_geom_edge
    input_table_name_b = raw_results
    output_table_name_2 = raw_results + '_groups_raw'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
        SELECT
          r."userId"
          ,t.project_id
          ,t.group_id
          ,count(t.group_id)
          ,Sum(CASE
            WHEN count = 1 THEN 0
            WHEN count = 2 THEN 1
           END) as edge_count
          ,array_agg(r."taskId") as tasks
          ,array_agg(r.timestamp) as timestamps
          -- we select the maximum timestamp for each group, this will function as the timestamp for
          -- all individual tasks of this group for each user
          ,max(r.timestamp) as group_timestamp
        FROM
          {} as t, {} as r
        WHERE
          t.task_id = r."taskId"
          AND
          t.project_id = r."projectId"
        GROUP BY
          t.project_id, t.group_id, r."userId"
        ORDER BY
          r."userId", t.project_id, t.group_id'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name_2), sql.Identifier(output_table_name_2),
                                            sql.Identifier(input_table_name_a), sql.Identifier(input_table_name_b))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name_2)
    del p_con

    groups_per_user_raw = output_table_name_2

    ####################
    #  3. get tasks in final groups per user
    ####################

    p_con = psqlDB()

    input_table_name = groups_per_user_raw
    output_table_name_3 = raw_results + '_groups'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      b.*
    FROM 
      {} as b
    WHERE
      b.count > b.edge_count'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name_3), sql.Identifier(output_table_name_3),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name_3)
    del p_con

    return output_table_name, output_table_name_2, output_table_name_3


def create_all_tasks_per_user(groups_per_user, task_geom):
    p_con = psqlDB()

    input_table_name_a = groups_per_user
    input_table_name_b = task_geom
    output_table_name = task_geom + '_per_user'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      b."userId" as user_id
      ,t.task_id
      ,b.project_id
      ,Min(b.group_timestamp) as group_timestamp
      --,b.group_id
      --,b.count
      --,b.edge_count
      ,t.task_geom
    FROM
      {} as b,
      {} as t
    WHERE
      b.group_id = t.group_id
    GROUP BY
      -- we need to group by task_id so that we avoid duplicates
      user_id, t.task_id, b.project_id, t.task_geom
    -- ORDER BY user_id, t.task_id'''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name_a), sql.Identifier(input_table_name_b))
    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def create_all_results_per_user(tasks_per_user, raw_results):
    p_con = psqlDB()

    input_table_name_a = tasks_per_user
    input_table_name_b = raw_results
    output_table_name = raw_results + '_all'
    index_task_id = raw_results + '_all_task_id_index'
    index_group_id = raw_results + '_all_group_id_index'
    index_user_id = raw_results + '_all_user_id_index'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      b.task_id
      ,b.user_id
      ,b.project_id
      --,b.group_id
      ,CASE
        WHEN r.result > 0 THEN r.result
        ELSE 0
       END as result
      ,(b.group_timestamp / 1000)::int as group_timestamp
      ,b.task_geom
    FROM 
      {} as b
      LEFT JOIN {} as r
      ON (b.user_id = r."userId" AND b.task_id = r."taskId" AND b.project_id = r."projectId");

    DROP INDEX IF EXISTS {};
    CREATE INDEX {}
      ON {}
      USING btree
      (task_id); 
       
    DROP INDEX IF EXISTS {};
    CREATE INDEX {}
      ON {}
      USING btree
      (user_id);
    '''

    sql_insert = sql.SQL(sql_insert).format(
      sql.Identifier(output_table_name),
      sql.Identifier(output_table_name),
      sql.Identifier(input_table_name_a),
      sql.Identifier(input_table_name_b),
      sql.Identifier(index_task_id),
      sql.Identifier(index_task_id),
      sql.Identifier(output_table_name),
      sql.Identifier(index_user_id),
      sql.Identifier(index_user_id),
      sql.Identifier(output_table_name),
    )

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def aggregate_results_using_array(results_per_user):
    ####################
    # input is a table that contains individual user results encoded as 0,1,2,3 etc.
    # e.g. 'all_results_per_user' table
    # the table should have the following columns with the names:
    #   task_id
    #   project_id
    #   result
    #   task_geom
    ####################

    p_con = psqlDB()

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS 
          SELECT
            b.task_id
            ,count(task_id) as completed_count
            ,array_agg(result) as results
            ,b.project_id
            ,b.task_geom
        FROM
          {} as b
        GROUP BY
          b.project_id
          ,b.task_id
          ,b.task_geom'''

    input_table_name = results_per_user
    output_table_name = results_per_user + '_' + 'array'

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def select_first_n_results(table_name, redundancy, limit):
    ####################
    # input is a table that aggregated results per tasks as an array
    # the table should have the following columns with the names:
    #   task_id
    #   completed_count
    #   results
    #   project_id
    #   task_geom
    ####################


    p_con = psqlDB()

    output_table_name = table_name + '_red_' + str(redundancy) + '_limit_' + str(limit)

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS 
        SELECT
          t.task_id
          ,t.real_completed_count 
          ,count(t.task_id) as completed_count
          ,sum(
            CASE
              WHEN t.result = 0 THEN 1
              ELSE 0
            END) AS no_count
           ,sum(
            CASE
              WHEN t.result = 1 THEN 1
              ELSE 0
            END) AS yes_count
            ,sum(
            CASE
              WHEN t.result = 2 THEN 1
              ELSE 0
            END) AS maybe_count
            ,sum(
            CASE
              WHEN t.result = 3 THEN 1
              ELSE 0
            END) AS badimage_count
          ,t.project_id
          ,t.task_geom
        FROM
          (
          SELECT
            b.task_id
            ,b.completed_count as real_completed_count
            -- set b.results[1:n]  -> n is the number of results to include, e.g. take first 4 results
            ,unnest(b.results[1:%s]) as result  
            ,b.project_id
            ,b.task_geom
          FROM
            {} as b
          WHERE
            completed_count >= %s
           ) as t
        GROUP BY t.task_id, t.project_id, t.task_geom, t.real_completed_count'''

    input_table_name = table_name
    output_table_name = table_name + '_red' + str(redundancy) + '_limit' + str(limit)
    data = [int(limit), int(redundancy)]

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, data)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def calc_agreement_from_results(table_name):
    ####################
    # the table should have the following columns with the names:
    #   task_id
    #   completed_count
    #   yes_count
    #   maybe_count
    #   badimage_count
    #   no_count
    ####################

    p_con = psqlDB()

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      b.*
      ,CASE
        WHEN (b.yes_count + b.maybe_count)/b.completed_count::numeric > 0.3 THEN 1
        WHEN b.no_count >= b.badimage_count THEN 0
        WHEN b.badimage_count > b.no_count THEN 2
        ELSE 9
      END as class
      ,CASE
        WHEN b.completed_count = 1 THEN 1.0
        ELSE (
        round(((1.0 / (b.completed_count::numeric * (b.completed_count::numeric - 1.0)))
      *
      (
      ((b.yes_count::numeric ^ 2.0) - b.yes_count::numeric)
      +
      ((b.maybe_count::numeric ^ 2.0) - b.maybe_count::numeric)
      +
      ((b.badimage_count::numeric ^ 2.0) - b.badimage_count::numeric)
      +
      ((b.no_count::numeric ^ 2.0) - b.no_count::numeric)
      )),3)
      ) END as agreement
      ,round(((b.yes_count::numeric + b.maybe_count::numeric)/b.completed_count::numeric),3)
       as msi
      ,round((b.no_count::numeric/b.completed_count::numeric),3)
       as no_si
    FROM
      {} as b'''

    input_table_name = table_name
    output_table_name = table_name + '_agreement'

    # we need to pass table names using the psycopg2 SQL module
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name


def merge_agreement_layers(results_per_user_array, redundancy):
    p_con = psqlDB()

    input_table_names = []
    for i in range(1, redundancy + 1):
        limit = i
        input_table_name = results_per_user_array + '_red' + str(redundancy) + '_limit' + str(limit) + '_agreement'
        input_table_names.append(input_table_name)

    output_table_name = results_per_user_array + '_merged'

    sql_insert = '''
    DROP TABLE IF EXISTS {};
    CREATE TABLE {} AS
    SELECT
      a.task_id
      ,a.project_id
      ,a.real_completed_count
      ,a.task_geom
      ,a.agreement as agreement_1
      ,b.agreement as agreement_2
      ,c.agreement as agreement_3
      ,d.agreement as agreement_4
      ,e.agreement as agreement_5
      ,f.agreement as agreement_6
      ,a.msi as msi_1
      ,b.msi as msi_2
      ,c.msi as msi_3
      ,d.msi as msi_4
      ,e.msi as msi_5
      ,f.msi as msi_6
      ,a.no_si as no_si_1
      ,b.no_si as no_si_2
      ,c.no_si as no_si_3
      ,d.no_si as no_si_4
      ,e.no_si as no_si_5
      ,f.no_si as no_si_6
    FROM
      {} as a
      ,{} as b
      ,{} as c
      ,{} as d
      ,{} as e
      ,{} as f
    WHERE
      a.task_id = b.task_id
      AND
      b.task_id = c.task_id
      AND
      c.task_id = d.task_id
      AND
      d.task_id = e.task_id
      AND
      e.task_id = f.task_id
      AND
      a.project_id = b.project_id
      AND
      b.project_id = c.project_id
      AND
      c.project_id = d.project_id
      AND
      d.project_id = e.project_id
      AND
      e.project_id = f.project_id
    '''

    # we need to pass table names using the psycopg2 SQL module
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_names[0]), sql.Identifier(input_table_names[1]),
                                            sql.Identifier(input_table_names[2]), sql.Identifier(input_table_names[3]),
                                            sql.Identifier(input_table_names[4]), sql.Identifier(input_table_names[5]))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    return output_table_name



def group_tasks(results_per_user):
    p_con = psqlDB()

    input_table_name = results_per_user
    output_table_name = results_per_user + '_tasks_raw'


    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          task_id
          ,project_id
          ,count(*) as completed_count
          ,sum(
            CASE
              WHEN result = 0 THEN 1
              ELSE 0
            END) AS no_count
           ,sum(
            CASE
              WHEN result = 1 THEN 1
              ELSE 0
            END) AS yes_count
            ,sum(
            CASE
              WHEN result = 2 THEN 1
              ELSE 0
            END) AS maybe_count
            ,sum(
            CASE
              WHEN result = 3 THEN 1
              ELSE 0
            END) AS badimage_count
          ,(sum(CASE WHEN result = 1 THEN 1.0 ELSE 0.0 END)  +
            sum(CASE WHEN result = 2 THEN 1.0 ELSE 0.0 END)) /  count(*) as msi
          ,(sum(CASE WHEN result = 0 THEN 1.0 ELSE 0.0 END) /  count(*)) as no_si
          ,task_geom
        FROM
          {}
        GROUP BY
          task_id
          ,project_id
          ,task_geom
        '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name),
                                            sql.Identifier(output_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name)
    del p_con

    #  2. step calculate the aggregated answer
    p_con = psqlDB()
    input_table_name = results_per_user + '_tasks_raw'
    output_table_name_2 = results_per_user + '_tasks'

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} AS
        SELECT
          t.task_id
          ,t.project_id
          ,t.completed_count
          ,round(t.msi,  3) as msi
          ,round(t.no_si, 3) as no_si
          ,CASE
            WHEN completed_count =  1  AND ((yes_count+maybe_count) =  1) THEN 1
            WHEN completed_count <  5  AND ((yes_count+maybe_count) >= 2) THEN 1
            WHEN completed_count >= 5  AND ((yes_count+maybe_count) >= 3) THEN 1
            WHEN no_count >= badimage_count THEN 0
            ELSE 2
          END as crowd_answer
          ,CASE
            WHEN t.completed_count = 1 THEN 1.0
            ELSE (
             round(((1.0 / (t.completed_count::numeric * (t.completed_count::numeric - 1.0)))
             *
             (
             ((t.yes_count::numeric ^ 2.0) - t.yes_count::numeric)
             +
             ((t.maybe_count::numeric ^ 2.0) - t.maybe_count::numeric)
             +
             ((t.badimage_count::numeric ^ 2.0) - t.badimage_count::numeric)
             +
             ((t.no_count::numeric ^ 2.0) - t.no_count::numeric)
             )),3)
           ) END as agreement
           ,task_geom
        FROM
          {} as t
         '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name_2),
                                            sql.Identifier(output_table_name_2),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    print('created: %s' % output_table_name_2)
    del p_con

    return output_table_name, output_table_name_2


def clean_up_database(delete_table_list):
    for i in range(0, len(delete_table_list)):
        p_con = psqlDB()

        sql_insert = '''
        DROP TABLE IF EXISTS {};
        '''

        sql_insert = sql.SQL(sql_insert).format(sql.Identifier(delete_table_list[i]))

        p_con.query(sql_insert, None)
        print('deleted: %s' % delete_table_list[i])
        del p_con


def create_table_all_results():
    p_con = psqlDB()
    sql_insert = '''
    DROP TABLE IF EXISTS all_results_raw;
    CREATE TABLE all_results_raw
    (
    task_id character varying,
    user_id character varying,
    project_id integer,
    result integer,
    group_timestamp integer,
    task_geom geometry
    )
    '''

    p_con.query(sql_insert, None)
    del p_con

def create_table_all_tasks():
    p_con = psqlDB()
    sql_insert = '''
    DROP TABLE IF EXISTS all_tasks;
    CREATE TABLE all_tasks
    (
    task_id character varying,
    project_id integer,
    completed_count bigint,
    msi numeric,
    no_si numeric,
    crowd_answer integer,
    agreement numeric,
    task_geom geometry
    )
    '''

    p_con.query(sql_insert, None)
    del p_con

def create_table_all_redundant_tasks():
    p_con = psqlDB()
    sql_insert = '''
    DROP TABLE IF EXISTS all_redundant_tasks;
    CREATE TABLE all_redundant_tasks
    (
      task_id character varying,
      project_id integer,
      real_completed_count bigint,
      task_geom geometry,
      agreement_1 numeric,
      agreement_2 numeric,
      agreement_3 numeric,
      agreement_4 numeric,
      agreement_5 numeric,
      agreement_6 numeric,
      msi_1 numeric,
      msi_2 numeric,
      msi_3 numeric,
      msi_4 numeric,
      msi_5 numeric,
      msi_6 numeric,
      no_si_1 numeric,
      no_si_2 numeric,
      no_si_3 numeric,
      no_si_4 numeric,
      no_si_5 numeric,
      no_si_6 numeric
    )
    '''

    p_con.query(sql_insert, None)
    del p_con

def merge_all_results(project_id):
    p_con = psqlDB()
    sql_insert = '''
    INSERT INTO all_results_raw (task_id, user_id, project_id, result, group_timestamp, task_geom)
    SELECT
      s.*
    FROM {} as s
    '''

    input_table_name = str(project_id) + '_results_all'
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    del p_con

def merge_all_tasks(project_id):
    p_con = psqlDB()
    sql_insert = '''
    INSERT INTO all_tasks (task_id, project_id, completed_count, msi, no_si, crowd_answer, agreement, task_geom)
    SELECT
      s.*
    FROM {} as s
    '''

    input_table_name = str(project_id) + '_results_all_tasks'
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    del p_con

def merge_all_redundant_tasks(project_id):
    p_con = psqlDB()
    sql_insert = '''
    INSERT INTO all_redundant_tasks (
          task_id,
          project_id,
          real_completed_count,
          task_geom,
          agreement_1,
          agreement_2,
          agreement_3,
          agreement_4,
          agreement_5,
          agreement_6,
          msi_1,
          msi_2,
          msi_3,
          msi_4,
          msi_5,
          msi_6,
          no_si_1,
          no_si_2,
          no_si_3,
          no_si_4,
          no_si_5, 
          no_si_6)
    SELECT
      s.*
    FROM {} as s
    '''

    input_table_name = str(project_id) + '_results_all_array_merged'
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)
    del p_con


def calc_user_matrix():
    p_con = psqlDB()
    sql_insert = '''
    DROP TABLE IF EXISTS user_matrix_raw;
    CREATE TABLE user_matrix_raw AS
    SELECT
      a.user_id
      -- quantity dimension
      ,count(*) total_contributions
      ,count(distinct (a.group_timestamp)) as groups_count
      -- agreement dimension
      -- crowd answer is "no building"
      ,Sum(CASE
        WHEN (t.crowd_answer = 0 AND a.result = 0) THEN 1.0
        ELSE 0.0
       END) / count(*) as no_no
      ,Sum(CASE
        WHEN (t.crowd_answer = 0 AND (a.result = 1 OR a.result = 2)) THEN 1.0
        ELSE 0.0
       END) / count(*) as no_bui
      ,Sum(CASE
        WHEN (t.crowd_answer = 0 AND a.result = 3) THEN 1.0
        ELSE 0.0
       END) / count(*) as no_bad
    -- crowd answer is "building"
      ,Sum(CASE
        WHEN (t.crowd_answer = 1 AND a.result = 0) THEN 1.0
        ELSE 0.0
       END) / count(*) as bui_no
      ,Sum(CASE
        WHEN (t.crowd_answer = 1 AND (a.result = 1 OR a.result = 2)) THEN 1.0
        ELSE 0.0
       END) / count(*) as bui_bui
      ,Sum(CASE
        WHEN (t.crowd_answer = 1 AND a.result = 3) THEN 1.0
        ELSE 0.0
       END) / count(*) as bui_bad
    -- crowd answer is "bad imagery"
      ,Sum(CASE
        WHEN (t.crowd_answer = 2 AND a.result = 0) THEN 1.0
        ELSE 0.0
       END) / count(*) as bad_no
      ,Sum(CASE
        WHEN (t.crowd_answer = 2 AND (a.result = 1 OR a.result = 2)) THEN 1.0
        ELSE 0.0
       END) / count(*) as bad_bui
      ,Sum(CASE
        WHEN (t.crowd_answer = 2 AND a.result = 3) THEN 1.0
        ELSE 0.0
       END) / count(*) as bad_bad
    -- get further dimensions
       ,count( distinct (t.project_id)) as project_count
       ,array_agg( distinct (t.project_id)) as project_ids
    -- temporal dimension
       ,Min(a.group_timestamp) as first_contribution
       ,Max(a.group_timestamp) as last_contribution
       ,(Max(a.group_timestamp) - Min(a.group_timestamp)) as contribution_term
       ,count( distinct( date_trunc( 'day', to_timestamp(a.group_timestamp)))) as active_days
    FROM
      all_results_raw as a
      ,all_tasks as t
    WHERE
      a.task_id = t.task_id
      AND
      a.project_id = t.project_id
    GROUP BY
      a.user_id
    '''

    p_con.query(sql_insert, None)
    del p_con
    print('created: user_matrix_raw')

    p_con = psqlDB()
    sql_insert = '''
    DROP TABLE IF EXISTS user_matrix;
    CREATE TABLE user_matrix as
    SELECT
      u.user_id
      -- quantity dimension
      ,u.total_contributions
      ,u.groups_count
      -- agreement dimension
      ,(u.no_no + u.bui_bui + u.bad_bad) as accuracy
      ,CASE
         WHEN (u.bui_bui + u.bui_no + u.bui_bad) = 0 THEN NULL
         ELSE ( u.bui_bui / (u.bui_bui + u.bui_no + u.bui_bad) )
       END as building_sensitivity
       ,CASE
         WHEN (u.bui_bui + u.no_bui + u.bad_bui) = 0 THEN NULL
         ELSE (u.bui_bui / (u.bui_bui + u.no_bui + u.bad_bui))
       END as building_precision
       ,CASE
         WHEN ((2 * u.bui_bui) + u.no_bui + u.bad_bui + u.bui_no + u.bui_bad) = 0 THEN NULL
         ELSE ((2 * u.bui_bui) / ((2 * u.bui_bui) + u.no_bui + u.bad_bui + u.bui_no + u.bui_bad) )
       END as building_fscore
      -- spatial dimension
      ,u.project_count
      ,u.project_ids
      -- temporal dimension
      ,u.first_contribution
      ,u.last_contribution
      ,u.contribution_term
      ,u.active_days
    FROM
      user_matrix_raw as u;
    '''

    p_con.query(sql_insert, None)
    del p_con

    print('created: user_matrix')

def complete_workflow(project_id):
    delete_table_list = []

    raw_results, tasks = select_data_for_project(project_id)
    delete_table_list.extend((raw_results, tasks))

    tasks_edge, groups_raw, groups = create_groups_per_user(tasks, raw_results)
    delete_table_list.extend((tasks_edge, groups_raw, groups))

    tasks_per_user = create_all_tasks_per_user(groups, tasks)
    delete_table_list.append(tasks_per_user)

    results_per_user = create_all_results_per_user(tasks_per_user, raw_results)
    delete_table_list.append(results_per_user)

    all_tasks_raw, all_tasks = group_tasks(results_per_user)
    delete_table_list.extend((all_tasks_raw, all_tasks))

    results_per_user_array = aggregate_results_using_array(results_per_user)
    # we need to add this later, once we tested the script
    delete_table_list.append(results_per_user_array)

    redundancy = 6
    for i in range(1, redundancy + 1):
        limit = i
        results = select_first_n_results(results_per_user_array, redundancy, limit)
        results_agreement = calc_agreement_from_results(results)
        delete_table_list.extend((results, results_agreement))

    agreement_final = merge_agreement_layers(results_per_user_array, redundancy)
    delete_table_list.append(agreement_final)


    merge_all_redundant_tasks(project_id)
    merge_all_tasks(project_id)
    merge_all_results(project_id)



    clean_up_database(delete_table_list)




