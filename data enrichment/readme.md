## Data Enrichment Module

The enrichment module aggregates mapswipe data within a psql database and derives statistics such as *agreement*, *no count*, *crowd answer* for each mapswipe task. The module consists of several submodules, which will be explained in further detail.

We define the following terms:

* `results`: data that is obtained from mapswipe app
* `contributions`: all mapswipe data, contains also information on "no" classifications and user attributes
    * `user contributions`: all contributions that can be unambiguously matched to a specific user
* `final tasks`: aggregated contributions for each task, contains information on agreement, msi, average user characteristics, aggregated crowd answer


What do the fields in the output refer to?

| column name | description |
| ----------- | ----------- |
| *taskid* | id of the task, using z, x,y coordinates of the corresponding tile in a TMS, e.g. `18-86062-119060` |
| *projectid* | id of the MapSwipe project as depicted in firebase |
| *completed_count* | we expect `completed count` and `count` to be equal. However sometimes they don't match. We will always use the highest number. |
| *count* | count of all contributions, tasks in the contributions table. `count` can't be higher than `completed_count` |
| *no_count* | count of all user contributions for which the result is `no`/`0` |
| *yes_count* | count of all user contributions for which the result is `yes`/`1` |
| *maybe_count* | count of all user contributions for which the result is `maybe`/`2` |
| *badimage_count* | count of all user contributions for which the result is `bad_image`/`3` |
| *agreement* | agreement calculated as Scott's Pi following Fleiss (1971) |
| *msi* | proportion of all `yes` contributions on all contributions |
| *no_si* | proportion of all `no` contributions on all contributions |


## Authentification Configuration and Requirements
To properly run the import process you need the following credentials:
* access to your local postgresql database
Furthermore, for each project to enrich make sure to import results beforehand.
* results table in psql database

## How to run enrichment
You can run the complete enrichment script like this:
* `python enrichment_workflow.py`
* `python enrichment_workflow.py -p 124 5519`
* `python enrichment_workflow.py -p 124 -t tasks -r results`

Parameters:
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be processed.
* `-t`: name of the tasks table in your local psql database as string (default: 'tasks')
* `-r`: name of the results table in your local psql database as string (default: 'results')

Workflow Description:
* loops for every project_id in list of projects
    * get user contributions
    * get all contributions
    * aggregate contributions

## Submodules

### Get User Contributions
What is it good for:
* get all contributions that can be matched to individual users

You can run the script like this:
* `python get_user_contributions.py -p 124`

Parameters:
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be processed.
* `-t`: name of the tasks table in your local psql database as string (default: 'tasks')
* `-r`: name of the results table in your local psql database as string (default: 'results')

Workflow Description:
* loop for every project:
    * Get tasks that are contained in two different groups for that projec
    * get all potential groups for each user
    * Clean the potential list of groups by filtering out unreliable groups
    * these 'unreliable' groups are groups where the user only worked in the overlapping area,
    * this means that we don't know for sure for which group the user submitted the result
    * join tasks (derived from groups for each user) and results for each user

### Get All Contributions
What is it good for:
* get all contributions including those that can't be matched to individual users

You can run the script like this:
* `python get_all_contributions.py -p 124`

Parameters:
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be processed.
* `-t`: name of the tasks table in your local psql database as string (default: 'tasks')

Workflow Description:
* loop for every project:
    * get unique tasks
    * some tasks might be duplicated in the database since they are part of two different groups
    * the completed count of these tasks will be merged
    * user contributions and unique tasks are joined
    * this step is necessary since user contributions may leave out tasks where no user contributed any data

### Aggregate Contributions
What is it good for:
* aggregate all contributions to derive information per final task such as agreement, no count, ...

You can run the script like this:
* `python aggregate_contributions.py -p 124`

Parameters:
* `-p` or `--projects`: project id as integer. Only projects corresponding to the provided ids will be processed.

Workflow Description:
* loop for every project:
    * group all contributions by taskid, projectid, completedcount and geometry
    * calculate agreement, no_si, msi, no_count, yes_count, maybe_count, bad_count