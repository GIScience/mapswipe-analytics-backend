# Processing Workflow Documentation
The processing workflow combines **import**, **enrichment** and **export**. The workflow can be used to regularly import mapswipe data, process it (e.g. calculate agreement per task) and finally serve it via geoserver.

## How to run processing workflow
You can run the complete enrichment script like this:
* `python processing_workflow.py`
* `python processing_workflow.py -l -m 10 -s 1800 -p 5519 9172 124 303`

You can loop the script using pm2 process manager:
* `pm2 start process_all_looped_py.json`

Parameters:
* `-t` or `--timestamp`: unix timestamp in milliseconds. Only results that have been submitted after this timestamp will be downloaded. You can use this option to download a subset of the existing MapSwipe data
* `-i` or `--initial_setup`: if this option is set, an empty database is created.
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* `-tt` or `--tasks_table_name`: name of the tasks table in your local psql database as string (default: 'tasks')
* `-rt` or `--results_table_name`: name of the results table in your local psql database as string (default: 'results')

* `-l` or `--loop`: if this option is set, the import will be looped
* `-m` or `--max_iterations`: the maximum number of imports that should be performed in integer
* `-s` or `--sleep_time`: the time in seconds for which the script will pause in beetween two imports

Workflow Description:
* import mapswipe data and derive new_projects and updated projects as output
* run enrichment new and updated projects
* generate geoserver layers and export data for new and updated projects (**!!to be implemented !!**)
* sleep for x seconds and start the workflow again

