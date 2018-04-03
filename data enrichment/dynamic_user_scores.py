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

def create_table_dynamic_user_matrix():
    p_con = psqlDB()

    sql_insert ='''
    DROP TABLE IF EXISTS user_matrix_dynamic;
    CREATE TABLE user_matrix_dynamic
    (
      user_id character varying,
      project_id integer,
      projects_worked_on integer[],
      cum_project_count bigint,
      cum_groups_count bigint,
      group_timestamp integer,
      --contrib_term integer,
      diff_to_last_contrib integer,
      contrib_session bigint,
      group_total bigint,
      group_no_no bigint,
      group_no_bui bigint,
      group_no_bad bigint,
      group_bui_no bigint,
      group_bui_bui bigint,
      group_bui_bad bigint,
      group_bad_no bigint,
      group_bad_bui bigint,
      group_bad_bad bigint,
      group_accuracy numeric,
      group_no_building_sensitivity numeric,
      group_no_building_precision numeric,
      group_no_building_fscore numeric,
      group_building_sensitivity numeric,
      group_building_precision numeric,
      group_building_fscore numeric,
      group_bad_sensitivity numeric,
      group_bad_precision numeric,
      group_bad_fscore numeric,
      cum_total bigint,
      cum_no_no bigint,
      cum_no_bui bigint,
      cum_no_bad bigint,
      cum_bui_no bigint,
      cum_bui_bui bigint,
      cum_bui_bad bigint,
      cum_bad_no bigint,
      cum_bad_bui bigint,
      cum_bad_bad bigint,
      cum_accuracy numeric,
      cum_no_building_sensitivity numeric,
      cum_no_building_precision numeric,
      cum_no_building_fscore numeric,
      cum_building_sensitivity numeric,
      cum_building_precision numeric,
      cum_building_fscore numeric,
      cum_bad_sensitivity numeric,
      cum_bad_precision numeric,
      cum_bad_fscore numeric
    )
    '''

    p_con.query(sql_insert, None)

    print('created: user_matrix_dynamic')

def get_all_user_ids():
    p_con = psqlDB()

    sql_insert = '''
    SELECT
      user_id
    FROM 
      user_matrix
    '''

    content = p_con.retr_query(sql_insert, None)

    user_ids = []
    for i in range(0, len(content)):
        user_ids.append(content[i][0])

    return user_ids


def calc_dynamik_user_matrix(user_id):
    p_con = psqlDB()

    output_table_name = 'raw_d_user_matrix_' + user_id

    sql_insert = '''
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} as
        SELECT
          a.user_id
          ,t.project_id
        -- quantity dimension
          ,count(*) as group_total
          ,Sum(count(*)) OVER (PARTITION BY group_timestamp ORDER BY group_timestamp) as cum_total
        -- agreement dimension
        -- not cumulative, but only for this group
          ,Sum(CASE
            WHEN (t.crowd_answer = 0 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END) as group_no_no
          ,Sum(CASE
            WHEN (t.crowd_answer = 0 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END)  as group_no_bui
          ,Sum(CASE
            WHEN (t.crowd_answer = 0 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END) as group_no_bad
          ,Sum(CASE
            WHEN (t.crowd_answer = 1 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END) as group_bui_no
          ,Sum(CASE
            WHEN (t.crowd_answer = 1 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END) as group_bui_bui
          ,Sum(CASE
            WHEN (t.crowd_answer = 1 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END) as group_bui_bad
          ,Sum(CASE
            WHEN (t.crowd_answer = 2 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END) as group_bad_no
          ,Sum(CASE
            WHEN (t.crowd_answer = 2 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END) as group_bad_bui
          ,Sum(CASE
            WHEN (t.crowd_answer = 2 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END) as group_bad_bad 
        -- cumulative counts
        -- crowd answer is "no building" dynamic
          ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 0 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_no_no
          ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 0 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_no_bui
          ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 0 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_no_bad
        -- crowd answer is "building" dynamic
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 1 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bui_no
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 1 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bui_bui
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 1 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bui_bad
        -- crowd answer is "bad" dynamic 
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 2 AND a.result = 0) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bad_no
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 2 AND (a.result = 1 OR a.result = 2)) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bad_bui
           ,Sum(Sum(CASE
            WHEN (t.crowd_answer = 2 AND a.result = 3) THEN 1.0
            ELSE 0.0
           END)) OVER (ORDER BY group_timestamp) as cum_bad_bad
        -- get further dimensions
           ,UNNEST(array_agg(t.project_id) OVER (ORDER BY group_timestamp)) as projects_so_far
        -- temporal dimension
           ,a.group_timestamp as group_timestamp
           ,a.group_timestamp -  (array_agg(a.group_timestamp) OVER (ORDER BY group_timestamp))[(count(a.group_timestamp) OVER (ORDER BY group_timestamp))-1] as diff_to_last_contrib
           ,(array_agg(a.group_timestamp) OVER (ORDER BY group_timestamp)) as unix_timestamps_so_far
        FROM
          all_results_raw as a
          ,all_tasks as t
        WHERE
          user_id = %s
          AND
          a.project_id = t.project_id
          AND
          a.task_id = t.task_id
        GROUP BY
          a.user_id
          ,a.group_timestamp
          ,t.project_id
        ORDER BY
          group_timestamp ASC
        '''

    data = [user_id]
    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(output_table_name), sql.Identifier(output_table_name))

    p_con.query(sql_insert, data)

    print('created: ', output_table_name)

    ###########
    #
    # 2. step: aggregate values for dynamic user matrix
    #
    ############

    p_con = psqlDB()

    input_table_name = output_table_name
    output_table_name_2 = 'd_user_matrix_' + user_id

    sql_insert = '''
    INSERT INTO user_matrix_dynamic
    SELECT
     foo.user_id
     ,foo.project_id
     ,array_agg(distinct foo.projects_so_far) projects_worked_on
     ,count(distinct foo.projects_so_far) as cum_project_count
     ,array_length(foo.unix_timestamps_so_far, 1) as cum_groups_count
     ,foo.group_timestamp
     -- temporal dimension
     /*,CASE 
       WHEN cum_total = 0 THEN NULL
       ELSE foo.unix_timestamps_so_far[completed_groups_so_far] - foo.unix_timestamps_so_far[1]
     END as contrib_term*/
     ,foo.diff_to_last_contrib
     ,Sum(CASE
       WHEN foo.diff_to_last_contrib > 43200 THEN 1
       ELSE 0
      END) OVER (ORDER BY group_timestamp) +1 as contrib_session
     -- only for group, not cumulative
     ,foo.group_total
     ,foo.group_no_no
     ,foo.group_no_bui
     ,foo.group_no_bad
     ,foo.group_bui_no
     ,foo.group_bui_bui
     ,foo.group_bui_bad
     ,foo.group_bad_no
     ,foo.group_bad_bui
     ,foo.group_bad_bad
     -- accuracy
     ,CASE
       WHEN foo.group_total = 0 THEN NULL
       ELSE round( (foo.group_no_no + foo.group_bui_bui + foo.group_bad_bad)::numeric / foo.group_total
      ,4) END as group_accuracy
     -- regarding no building
     ,CASE
       WHEN (foo.group_no_no + foo.group_no_bui + foo.group_no_bad) = 0 THEN NULL
       ELSE round( (foo.group_no_no / (foo.group_no_no + foo.group_no_bui + foo.group_no_bad) )
      ,4) END as no_building_sensitivity
      ,CASE
       WHEN (foo.group_no_no + foo.group_bui_no + foo.group_bad_no) = 0 THEN NULL
       ELSE round( (foo.group_no_no / (foo.group_no_no + foo.group_bui_no + foo.group_bad_no))
      ,4) END as no_building_precision
      ,CASE
        WHEN ((2 * foo.group_no_no) + foo.group_bui_no + foo.group_bad_no + foo.group_no_bui + foo.group_no_bad) = 0 THEN NULL
        ELSE round( ((2 * foo.group_no_no) / ((2 * foo.group_no_no) + foo.group_bui_no + foo.group_bad_no + foo.group_no_bui + foo.group_no_bad) )
       ,4) END as no_building_fscore
     -- regarding building
     ,CASE
       WHEN (foo.group_bui_bui + foo.group_bui_no + foo.group_bui_bad) = 0 THEN NULL
       ELSE round( (foo.group_bui_bui / (foo.group_bui_bui + foo.group_bui_no + foo.group_bui_bad) )
      ,4) END as building_sensitivity
     ,CASE
       WHEN (foo.group_bui_bui + foo.group_no_bui + foo.group_bad_bui) = 0 THEN NULL
       ELSE round( (foo.group_bui_bui / (foo.group_bui_bui + foo.group_no_bui + foo.group_bad_bui))
      ,4) END as building_precision
      ,CASE
        WHEN ((2 * foo.group_bui_bui) + foo.group_no_bui + foo.group_bad_bui + foo.group_bui_no + foo.group_bui_bad) = 0 THEN NULL
        ELSE round( ((2 * foo.group_bui_bui) / ((2 * foo.group_bui_bui) + foo.group_no_bui + foo.group_bad_bui + foo.group_bui_no + foo.group_bui_bad) )
       ,4) END as building_fscore
     -- regardin bad image
     ,CASE
       WHEN (foo.group_bad_bad + foo.group_bad_no + foo.group_bad_bui) = 0 THEN NULL
       ELSE round( (foo.group_bad_bad / (foo.group_bad_bad + foo.group_bad_no + foo.group_bad_bui) )
      ,4) END as bad_sensitivity
     ,CASE
       WHEN (foo.group_bad_bad + foo.group_no_bad + foo.group_bui_bad) = 0 THEN NULL
       ELSE round( (foo.group_bad_bad / (foo.group_bad_bad + foo.group_no_bad + foo.group_bui_bad))
      ,4) END as bad_precision
     ,CASE
        WHEN ((2 * foo.group_bad_bad) + foo.group_bui_bad + foo.group_no_bad + foo.group_bad_bui + foo.group_bad_no) = 0 THEN NULL
        ELSE round( ((2 * foo.group_bad_bad) / ((2 * foo.group_bad_bad) + foo.group_bui_bad + foo.group_no_bad + foo.group_bad_bui + foo.group_bad_no) )
       ,4) END as bad_fscore
     -- cumulative counts
     ,Sum(foo.group_total) OVER (ORDER BY group_timestamp) as cum_total
     ,foo.cum_no_no
     ,foo.cum_no_bui
     ,foo.cum_no_bad
     ,foo.cum_bui_no
     ,foo.cum_bui_bui
     ,foo.cum_bui_bad
     ,foo.cum_bad_no
     ,foo.cum_bad_bui
     ,foo.cum_bad_bad
     -- accuracy
    -- accuracy
     ,CASE
       WHEN (Sum(foo.group_total) OVER (ORDER BY group_timestamp)) = 0 THEN NULL
       ELSE round( (foo.cum_no_no + foo.cum_bui_bui + foo.cum_bad_bad)::numeric / (Sum(foo.group_total) OVER (ORDER BY group_timestamp))
      ,4) END as accuracy
     -- regarding no building
     ,CASE
       WHEN (foo.cum_no_no + foo.cum_no_bui + foo.cum_no_bad) = 0 THEN NULL
       ELSE round( (foo.cum_no_no / (foo.cum_no_no + foo.cum_no_bui + foo.cum_no_bad) )
      ,4) END as no_building_sensitivity
      ,CASE
       WHEN (foo.cum_no_no + foo.cum_bui_no + foo.cum_bad_no) = 0 THEN NULL
       ELSE round( (foo.cum_no_no / (foo.cum_no_no + foo.cum_bui_no + foo.cum_bad_no))
      ,4) END as no_building_precision
      ,CASE
        WHEN ((2 * foo.cum_no_no) + foo.cum_bui_no + foo.cum_bad_no + foo.cum_no_bui + foo.cum_no_bad) = 0 THEN NULL
        ELSE round( ((2 * foo.cum_no_no) / ((2 * foo.cum_no_no) + foo.cum_bui_no + foo.cum_bad_no + foo.cum_no_bui + foo.cum_no_bad) )
       ,4) END as no_building_fscore
     -- regarding building
     ,CASE
       WHEN (foo.cum_bui_bui + foo.cum_bui_no + foo.cum_bui_bad) = 0 THEN NULL
       ELSE round( (foo.cum_bui_bui / (foo.cum_bui_bui + foo.cum_bui_no + foo.cum_bui_bad) )
      ,4) END as building_sensitivity
     ,CASE
       WHEN (foo.cum_bui_bui + foo.cum_no_bui + foo.cum_bad_bui) = 0 THEN NULL
       ELSE round( (foo.cum_bui_bui / (foo.cum_bui_bui + foo.cum_no_bui + foo.cum_bad_bui))
      ,4) END as building_precision
      ,CASE
        WHEN ((2 * foo.cum_bui_bui) + foo.cum_no_bui + foo.cum_bad_bui + foo.cum_bui_no + foo.cum_bui_bad) = 0 THEN NULL
        ELSE round( ((2 * foo.cum_bui_bui) / ((2 * foo.cum_bui_bui) + foo.cum_no_bui + foo.cum_bad_bui + foo.cum_bui_no + foo.cum_bui_bad) )
       ,4) END as building_fscore
     -- regardin bad image
     ,CASE
       WHEN (foo.cum_bad_bad + foo.cum_bad_no + foo.cum_bad_bui) = 0 THEN NULL
       ELSE round( (foo.cum_bad_bad / (foo.cum_bad_bad + foo.cum_bad_no + foo.cum_bad_bui) )
      ,4) END as bad_sensitivity
     ,CASE
       WHEN (foo.cum_bad_bad + foo.cum_no_bad + foo.cum_bui_bad) = 0 THEN NULL
       ELSE round( (foo.cum_bad_bad / (foo.cum_bad_bad + foo.cum_no_bad + foo.cum_bui_bad))
      ,4) END as bad_precision
     ,CASE
        WHEN ((2 * foo.cum_bad_bad) + foo.cum_bui_bad + foo.cum_no_bad + foo.cum_bad_bui + foo.cum_bad_no) = 0 THEN NULL
        ELSE round( ((2 * foo.cum_bad_bad) / ((2 * foo.cum_bad_bad) + foo.cum_bui_bad + foo.cum_no_bad + foo.cum_bad_bui + foo.cum_bad_no) )
       ,4) END as bad_fscore
    FROM
      {} as foo
    GROUP BY
      foo.user_id
      ,foo.project_id
      ,foo.group_timestamp
      ,foo.diff_to_last_contrib
      ,foo.unix_timestamps_so_far
      ,foo.group_total
      ,foo.cum_total
      ,foo.cum_no_no
      ,foo.cum_no_bui
      ,foo.cum_no_bad
      ,foo.cum_bui_no
     ,foo.cum_bui_bui
     ,foo.cum_bui_bad
     ,foo.cum_bad_no
     ,foo.cum_bad_bui
     ,foo.cum_bad_bad
       ,foo.group_no_no
      ,foo.group_no_bui
      ,foo.group_no_bad
      ,foo.group_bui_no
     ,foo.group_bui_bui
     ,foo.group_bui_bad
     ,foo.group_bad_no
     ,foo.group_bad_bui
     ,foo.group_bad_bad
    ORDER BY
      foo.group_timestamp;
    DROP TABLE IF EXISTS {};
    '''

    sql_insert = sql.SQL(sql_insert).format(sql.Identifier(input_table_name),
                                            sql.Identifier(input_table_name))

    p_con.query(sql_insert, None)

    print('inserted data into: user_matrix_dynamic')

##################################################

create_table_dynamic_user_matrix()


user_ids = get_all_user_ids()
print(len(user_ids))

counter = 0
for user_id in user_ids:
    #if counter == 10:
    #    sys.exit()
    counter = counter +1
    print('######### %s ########' % counter)
    print(user_id)
    calc_dynamik_user_matrix(user_id)


