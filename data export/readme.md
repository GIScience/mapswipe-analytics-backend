# Export Module Documentation
The export module provides services that can be used by MapSwipe Analytics (e.g. TMS layers) or files that can be downloaded (e.g. for Tasking Manager or on statistics).

This module creates:
* geoserver Layers for Visualization in web map application
* vector Data for web map application
* statistics for web map application

## Authentification Configuration and Requirements
To properly run the export process you need the following credentials:
* access to your local postgresql database (make sure that your user has all rights otherwise the export might fail)
* access to your geoserver

The default configuration is stored in [auth.py](cfg/auth.py). You can adapt to your own settings by providing a `config.cfg` file in the same folder as the auth.py script. You can use the [template](cfg/your_config_file.cfg) for this.

## How to run export
You can run the complete export script like this:
* `python export_workflow.py`
* `python export_workflow.py -p 5519 124 303`
* `python export_workflow.py -l -m 10 -s 1800 -p 5519 9172 124 303`

Parameters:
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* `-pt` or `--project_table_name`: name of the projects table in your local psql database as string (default: 'projects')
* `-l` or `--loop`: if this option is set, the import will be looped
* `-m` or `--max_iterations`: the maximum number of imports that should be performed in integer
* `-s` or `--sleep_time`: the time in seconds for which the script will pause in beetween two imports

Workflow Description:
* loop for each project id:
    * delete layers that already exist
    * create new layers
    * seed layers
* logging is active and writes to export.log

## Submodules
### Delete Layer
What is it good for:
* deletes layers in geoserver

You can run the script like this:
* `python delete_layer.py -p 5519 124`

Parameters:
* `-p`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.

Output:
* list of the http responses
* e.g. `[[5519, 200, 200], [10460, 200, 200]]`
    * successfully deleted the layers for project 5519 and 10460

### Create Layer
What is it good for:
* creates layers in geoserver

You can run the script like this:
* `python create_layer.py -p 5519 124`

Parameters:
* `-p`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* `-pt` or `--project_table_name`: name of the projects table in your local psql database as string (default: 'projects')

Output:
* list of the http responses
* e.g. `[[5519, 201], [10460, 201]]`
    * successfully created the layers for project 5519 and 10460

### Seed Layer
What is it good for:
* ???

You can run the script like this:
* ???

Parameters:
* `-p`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* **add more**

Output:
* list of the http responses
* **add example**
