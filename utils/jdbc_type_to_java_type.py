#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-j', '--jdbc_type', required=None, default=None, type=str,
                    help='jdbc type as string')


def jdbc_type_to_java_type(jdbc_type):

    type_dict = {
        'CHAR': 'java.lang.String',
        'CHARACTER VARYING': 'java.lang.String',
        'VARCHAR': 'java.lang.String',
        'LONGVARCHAR': 'java.lang.String',
        'NUMERIC': 'java.math.BigDecimal',
        'DECIMAL': 'java.math.BigDecimal',
        'BOOLEAN': 'java.lang.Boolean',
        'TINYINT': 'java.lang.Byte',
        'SMALLINT': 'java.lang.Short',
        'INTEGER': 'java.lang.Integer',
        'BIGINT': 'java.lang.Long',
        'REAL': 'java.lang.Float',
        'FLOAT': 'java.lang.Double',
        'DOUBLE': 'java.lang.Double',
        'DOUBLE PRECISION': 'java.lang.Double',
        'BINARY': 'java.lang.Byte[]',
        'VARBINARY': 'java.lang.Byte[]',
        'LONGVARBINARY': 'java.lang.Byte[]',
        'DATE': 'java.sql.Date',
        'TIME': 'java.sql.Time',
        'TIMESTAMP': 'java.sql.Timestamp',
        'TIMESTAMP WITHOUT TIME ZONE': 'java.sql.Timestamp',
        'GEOMETRY': 'com.vividsolutions.jts.geom.Geometry'
    }
    try:
        java_type = type_dict[jdbc_type.upper()]
        return java_type

    except:
        print('currently we implemented the following data types')
        print(type_dict)
        return False




########################################################################################################################
if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    java_type = jdbc_type_to_java_type(args.jdbc_type)
    print(java_type)

