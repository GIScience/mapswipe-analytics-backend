# Test the import module

This file describes different import states and how we could test whether our approach produces correct results.


## Get Projects()

Here's a list what might happen:
* there is no connection to firebase
* there is no connection to our psql database
* a new project added to firebase:
* progress of a project updated in firebase
* number of contributors of a project updated in firebase
* verification count changed in firebase (e.g. manually set to 5 to get better results)
* entry from project table deleted in firebase
* entry from import table deleted in firebase


## Get Results()

Here's a list what migt happen:
* there is no connection to the mysql database
* there is no connection to our psql database
* there is no last timestamp in the database
* results are already saved in our psql database
* there are many new results (e.g. > 100.000 in the mysql databse)
* there are no new results to save in psql


## Get Tasks()

Here's a list what might happen:
* there is no connection to our psql database
* thre is no connection to firebase
* tasks already exist in our psql table


## Get Tasks Completed Count()

Here's a list what might happen:
* there is no connection to firebase
* there is not connection to our psql
* a task is part of two groups
* a task is part of two projects
* the csv file with the completed count information could not be saved and or deleted


## Get Users()
* there is no connection to firebase
* there is not connection to our psql
* * the csv file with the user information could not be saved and or deleted
