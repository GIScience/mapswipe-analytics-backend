# MapSwipe Processing

The diagram provides an overview of the role of this repo towards other MapSwipe related projects. Processing is the central to offer our MapSwipe related services and consists of three major submodules: import, enrichment and export. All submodules are combined in the [processing_workflow](/processing_workflow/readme.md).

Have a look at the documentation of each sub module:
* [import](/data import/readme.md): data is extracted from the mapswipe MySQL database
* [enrichment](/data enrichment/readme.md): we calculate agreement, number of contributors, aggregated answer for each task and project
* [export](/data export/readme.md): geoserver is utilized to serve MapSwipe data

<img src="img/overview.PNG" width="600px">

## Server Setup:
We have two servers that are involved in the processing:
* (1) mapswipe.geog.uni-heidelberg.de
* (2) mapswipe-backend.geog.uni-heidelberg.de

Currently (1) is used as the production instance. On this server the following scripts are active:
* [loop_ms_import.py](/data import-export/loop_ms_import.py)
* [loop_service.py](/data import-export/loop_service.py)

On (1) you can monitor the processes like this:
```
sudo su
pm2 list
pm2 monit loop_ms_import
pm2 monit loop_ms_service
```

Server (2) is used for testing of the new code at the moment. On this server the following script is active:
* [processing_workflow.py](/processing_workflow/processing_workflow.py)

On (2) you can monitor the process like this:
```
sudo su
pm2 list
tail -100 /data/MapSwipe_privat/processing_workflow/processing.log
```

## pm2 process manager
We use the pm2 process manager. pm2 will start the scripts after a server restart. To set up this we use the following commands:

### Installation:
- `sudo apt install nodejs-legacy`
- `sudo npm install pm2 -g`

### Add processes:
- `sudo pm2 start /home/b/bherfort/src/loop_ms_import.py`
- `sudo pm2 start /home/b/bherfort/src/loop_service.py`

### Daemonize processes:
- `sudo pm2 startup`
- `sudo pm2 save`

### Monitor processes:
overview on running scripts:
- `sudo pm2 list`
- ` sudo pm2 monit`

get more detailed information on each process:
- `sudo pm2 describe loop_ms_import`
- `sudo pm2 log loop_ms_import`

# Main Goals of our MapSwipe related activities:
* develop workflows and tools that enable NGOs to integrate MapSwipe data in the HOT Tasking Manger
    * provide a platform where users can get insight about and download up-to-date MapSwipe data
* investigate the quality of crowdsourced geographic information from the MapSwipe app
    * derive intrinsic indicators (e.g. agreement) to estimate data quality
* develop workflows and methods to improve the quality of processed MapSwipe data
    * use automated methods to extract information from the satellite imagery (e.g. DeepVGI) --> train model using tasks with a high agreement and estimate probability for tasks with low agreement
    * use information on individual users
    * crowdsourced workflow to improve data quality
    * use other datasets available to improve data quality (e.g. Worldpop, Global Human Settlement Layer) (Hint for Marcel: Can these datasets improve/complement our workflow?)


## What we cannot do (currently)
* Improve the MapSwipe App (e.g. user design, features to be stored)