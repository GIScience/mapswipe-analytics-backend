# Setup your MapSwipe analytics backend environment

1. Initialize a virtualenvironment
2. Install python & libraries
3. Install & configure PostgreSQL + PostGIS
4. Install & configure GeoServer


## 1. Inititalize a virtualenvironment


get python, pip and virtualenv

    sudo apt-get install python3 python3-pip virtualenv
    pip install --user pipenv

create a virtual environment and activate it

    mkvirtualenv data-import-export
    workon data-import-export


## 2. Install python & libraries


Install additional libraries

    sudo add-apt-repository ppa:ubuntugis/ppa
    sudo apt-get update


First, install `GDAL` at the system level:

     sudo apt-get install libgdal-dev


Before installing the Python library, you'll need to set up your environment to build it correctly (it needs to know where the system `GDAL` libraries are). Set the following environment variables to do that:

     export CPLUS_INCLUDE_PATH=/usr/include/gdal
     export C_INCLUDE_PATH=/usr/include/gdal

 Finally, install the Python library. You'll need to specify the same version for the Python library as you have installed on the system. Use this to find your system version:

     gdal-config --version

 and install the library via pip with:

     pip install GDAL==$VERSION

 or this handy one-liner for both:

     pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

  install the other libraries from requirements.txt

    pip3 install -r requirements.txt

## 3. Install & configure PostgreSQL + PostGIS (currently we use PostgreSQL 9.6.5)


    sudo add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main"
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update
    sudo apt-get install postgresql-9.6
    sudo su postgres
    psql ALTER USER "postgres" with password 'postgres';
    sudo apt-get install postgis

### Prepare Tables
(auslagern in anderem md doc?)

**If you run the import.py script in 'initial import' mode you don't need to setup the tables yourself**

```sql

CREATE DATABASE mapswipe;

CREATE EXTENSION postgis;

CREATE TABLE projects (
  id int NOT NULL
  ,contributors int NOT NULL
  ,groupAverage double precision NOT NULL
  ,image character varying NOT NULL
  ,importKey character varying NOT NULL
  ,isFeatured boolean NOT NULL
  ,lookFor character varying NOT NULL
  ,name character varying NOT NULL
  ,progress int NOT NULL
  ,projectDetails character varying NOT NULL
  ,state int NOT NULL
  ,verificationCount int NOT NULL
  ,corrupt boolean NOT NULL
  ,lastCheck timestamp without time zone
  ,extent geometry
  ,CONSTRAINT pk_project_id PRIMARY KEY (id)
  );
  ```

corrupt >> flags projects which are missing critical information

last_check >> leaves a timestamp fo the last table injection, NOT UPDATES

```sql
CREATE TABLE results (
  taskId varchar NOT NULL
  ,userId varchar NOT NULL
  ,projectId int NOT NULL
  ,timestamp bigint NOT NULL
  ,result int NOT NULL
  ,duplicates int
  ,CONSTRAINT pk_result_id PRIMARY KEY (taskId, userId, projectId)
);

CREATE INDEX results_taskId_index
  ON public.results
  USING btree
  (taskId);

CREATE INDEX results_timestamp_index
  ON public.results
  USING btree
  (timestamp);

CREATE INDEX results_projectId_index
  ON public.results
  USING btree
  (projectId);

CREATE INDEX results_index
  ON public.results
  USING btree
  (result);

CREATE TABLE tasks (
  taskId varchar NOT NULL
  ,projectId int NOT NULL
  ,groupId int
  ,completedCount int NOT NULL 
  ,geo geometry
  ,CONSTRAINT pk_task_id PRIMARY KEY (taskId, projectId, groupId)
);

CREATE INDEX tasks_taskId_index
  ON public.tasks
  USING btree
  (taskId);

CREATE INDEX tasks_projectId_index
  ON public.tasks
  USING btree
  (projectId);


CREATE INDEX tasks_groupId_index
  ON public.tasks
  USING btree
  (groupId);


CREATE INDEX tasks_geo_index
  ON public.tasks
  USING gist
  (geo);

```
## 4. Install & configure GeoServer
