#!/bin/python3
# -*- coding: UTF-8 -*-
# Author: M. Reinmuth, B. Herfort
########################################################################################################################
import logging

import ntpath
import os, sys

sys.path.insert(0, '../cfg')
sys.path.insert(0, '../utils')

import requests

from auth import geoserver_config

from create_layer import check_if_layer_exists

import argparse
# define arguments that can be passed by the user
parser = argparse.ArgumentParser(description='Process some wfs uri.')
parser.add_argument('-l', '--layer_name', required=True, default=None, type=str,
                    help='the url pointing to the wfs you want to export')
parser.add_argument('-o', '--output_path', required=True, default=None, type=str,
                    help='path and filename where to export the table.')

def get_out_format(file_extension):

    file_type_dict = {
        'csv': 'csv',
        'geojson': 'application/json',
        'json': 'application/json',
        #'shp': 'shape-zip',
        'gml': 'GML3'
    }
    try:
        wfs_type = file_type_dict[file_extension.lower()]
        return wfs_type

    except:
        print('currently we implemented the following file types')
        print(file_type_dict)
        return False


def build_wfs_url(g, layer, format):
    #auth = HTTPBasicAuth(g['username'], g['password'])
    # the url to which the request is send
    url = '{baseurl}/{workspace}/ows?service=WFS&version=2.0.0&request=GetFeature&typeName={workspace}:{layer}&outputFormat={format}'.format(
        baseurl=g['baseurl'],
        workspace=g['workspace'],
        layer=layer,
        format=format
    )
    return url


def run_wfs_export(layer, path):

    logging.basicConfig(filename='export.log',
                        level=logging.WARNING,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filemode='a'
                        )

    g = geoserver_config()

    if check_if_layer_exists(g, layer):
        logging.warning('Export of Layer: %s started' % layer)

        # seperate location from file
        try:
            head, tail = ntpath.split(path)
        except:
            print('slicing of path/file failed')
            print('path: %s' % path)
            print('head: %s' % head)
            print('tail: %s' % tail)
        # separate file extension from filename
        file_ext = tail.split('.')[1]

        # check if fileextension is available @ geoserver
        format = get_out_format(file_ext)

        # check if format is of type shape zip to overwrite filextension to .zip format
        '''if format == 'shape-zip':
            tail = tail.split('.')[0] + '.zip'
        '''


        if not format:
            logging.warning('Export format not found, requested filextension: %s' % file_ext)
            sys.exit()

        logging.warning('Export format found, using %s' % format)
        # build url from layer geoserver config and output format
        url = build_wfs_url(g, layer, format)

        logging.warning('Url built: %s' % url)

        # check if path is provided
        '''if head:
            print(head)
            # convert path to absolute path regardless if it alread is an absolute path
            head = os.path.abspath(head)
            print(head)
            os.chdir(head)
            print('changed dir')
        '''

        # request wfs
        wfs = requests.get(url)
        logging.warning('Export successfull. WFS: %s written to location/file: %s ' % (layer, path))
        with open(path, "w") as fo:
            fo.write(wfs.text)

        return wfs.status_code

    else:
        logging.warning('There is no layer: {layer} registered at geoserver with url {g_url}'.format(
            layer=layer,
            g_url=g['baseurl']
        ))

########################################################################################################################
if __name__ == '__main__':

    try:
        args = parser.parse_args()
    except:
        print('have a look at the input arguments, something went wrong there.')

    run_wfs_export(args.layer_name, args.output_path)
