# Import Module Documentation
The import module transfers data created by MapSwipe users to a local instance of a postgresql database. The module consists of several subparts that will be explained in further detail. The import module is designed for an ubuntu system.

## Authentification Configuration and Requirements
To properly run the import process you need the following credentials:
* access to MapSwipe mysql database on GoogleCloud
* access to MapSwipe realtime database on Firebase
* access to your local postgresql database

The default configuration is stored in [auth.py](cfg/auth.py). You can adapt to your own settings by providing a `config.cfg` file in the same folder as the auth.py script. You can use the [template](cfg/your_config_file.cfg) for this.

## How to run import
You can run the complete import script like this:
* `python import_workflow.py`
* `python import_workflow.py -i -p 5519 -t 1511126000000`
* `python import_workflow.py -l -m 10 -s 1800 -p 5519 9172 124 303`

Parameters:
* `-t` or `--timestamp`: unix timestamp in milliseconds. Only results that have been submitted after this timestamp will be downloaded. You can use this option to download a subset of the existing MapSwipe data
* `-i` or `--initial_setup`: if this option is set, an empty database is created.
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* `-l` or `--loop`: if this option is set, the import will be looped
* `-m` or `--max_iterations`: the maximum number of imports that should be performed in integer
* `-s` or `--sleep_time`: the time in seconds for which the script will pause in beetween two imports

### Run initial import
Before running an import make sure to provide a correct configuration file, such as [this one](cfg/your_config_file.cfg).

`python import_workflow.py -i -p 5519 -t 1511126000000`

* `-i` --> creates database and tables
* `-p` --> inserts tasks and updated completed count for project 5519
* `-t` --> downloads only results that have been submitted after 1511126000000

This produces an new, empty database. It downloads all projects available from firebase and inserts the data in your local psql database. All results that have been submitted after the timestamp provided will be downloaded and stored in your local psql database. Tasks are created for project 5519, completed count is updated for project 5519. Information on all mapswipe users is downloaded and stored in the local psql database.

### Run regular import
Before running an import make sure to provide a correct configuration file, such as [this one](cfg/your_config_file.cfg).

`python import_workflow.py`

The regular import downloads all projects available from firebase. New projects are inserted into the local psql database. Existing projects for which number of contributors or progress changed will be updated. Results are obtained using the timestamp of the latest result in the local psql database. Tasks are inserted for new projects. Completed Count is updated for all projects where number of contributors or progress changed or for which new results have been obtained. Information for all mapswipe users is downloaded and replaced in the local psql database.

### loop regular import
Before running an import make sure to provide a correct configuration file, such as [this one](cfg/your_config_file.cfg).

`python import_workflow.py -l -m 3 -s 60`

When the `-l` flag is set, the import will be looped. In this example three iterations are performed. After an iteration there is a break time of 60 seconds.

On the mapswipe server the script is running for testing purpose. You can access it using pm2 process manager:
* `sudo pm2 list`
* `sudo pm2 describe import`
* `sudo pm2 logs import`

You can start the script with pm2 using this command that also passes the correct arguments to the script:
* `cd "/home/b/bherfort/Import_Module/MapSwipe_privat/data import/"`
* `sudo pm2 start import_workflow.py --interpreter=python3  -- -l -m 3 -s 30`

## Submodules

### Get Projects
What is it good for:
* import projects from firebase into pgsql
* get information on which projects in pgsql information from firebase is missing
  * outdated or new projects
* update projects

You can run the script like this:
* `python get_projects.py`
* `python get_projects.py -p 5519 -t projects`

Parameters:
* `-p`: project id as integer. Only projects corresponding to the provided ids will be downloaded and/or updated.
* `-t`: name of the projects table in your local psql database as string (default: 'projects')

Output:
* `new_projects`: list of project IDs of all projects that are in firebase but not in your local psql database
* `updated_projects`: list of project IDs of all projects that are already in your local database, but for which progress or number of contributors changed in firebase
* `project_dict`: python dictionary with the latest information on all projects in your database.

Workflow Description:
* Firstly firebase is querried either with user provided project id(s) or all projects
* These projects are compared to the corresponding projects in pgsql
* In the end two lists are created for projects which need an update and those which are new
* Further faulty projects containing errors are removed and the spatial extent of new projects calculated
* The function `save_projects_psql()` is used to save new or update projects. In the overall workflow of `import.py` it is used after the other objects are updated to document the latest status of mapswipe data in projects table


### Get Results
What is it good for:
* import results from mysql to pgsql

You can run this script like this:
* `python get_results.py`
* `python get_results.py -r results -t 1511271261462`

Parameters:
* `-r`: name of the results table in your local psql database as string (default: 'results')
* `-t`: unix timestamp as integer in milliseconds, only results that have been submitted after this timestamp will be downloaded (default: `int((time.time() - 3600)*1000)` --> the last hour)

Output:
* `changed_projects`: list of project IDs of all projects for which new results have been submitted
* if a timestamp is provided, all results uploaded after

Workflow Description:
* we should create a diagram for this.
* output 2: updates "results" table in local psql database. Inserts all information for the downloaded results.


### Get Tasks
You can run this script like this:
* `python get_tasks.py`
* `python get_tasks.py -p 5519 -t tasks`
* `python get_tasks.py -p 5519 9172 124 -t tasks`

Parameters:
* `-p`: list of project IDs
* `-t`: name of the tasks table in your local psql database as string (default: 'tasks')

Output:
*

Workflow Description:
* we should create a diagram for this
* input 1: "project_dict" as dictionary. For this module we check whether `project_dict[project_id]["isNew"] == 1`. Make sure your python dictionary provides information on this.
* input 2: "task_table_name" as string, this refers to the name of the tasks table in the local instance of the postgresql database.
* output: Inserts task table in local psql database. Inserts all tasks for new projects.


### Get Tasks Completed Count
You can run this script like this:
* `python get_tasks_completed_count.py`
* `python get_tasks_completed_count.py -p 5519 -t tasks`
* `python get_tasks_completed_count.py -p 5519 9172 124 -t tasks`

Parameters:
* `-p`: list of project IDs
* `-t`: name of the tasks table in your local psql database as string (default: 'tasks')

Output:
*

Workflow Description:
* we should create a diagram for this.
* input 1: "project_dict" as dictionary. For this module we check whether `project_dict[project_id]["needUpdate"] == 1`. Make sure your python dictionary provides information on this.
* input 2: "task_table_name" as string, this refers to the name of the tasks table in the local instance of the postgresql database.
* output: Updates "completed_count" field in tasks table in local psql database.

### Get Users
You can run this script like this:
* `python get_users.py`
* `python get_users.py -u users`

Parameters:
* `-u`: name of the users table in your local psql database as string (default: 'tasks')

Output:
*

Workflow Description:
* we should create a diagram for this.
* * input: "users_table_name" as string, this refers to the name of the users table in the local instance of the postgresql database.
* output: updated users table in psql database.
