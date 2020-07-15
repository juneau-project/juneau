# The Juneau Project

[![juneau-project](https://circleci.com/gh/juneau-project/juneau.svg?style=svg)](https://app.circleci.com/pipelines/github/juneau-project)
 
The past decade has brought a sea change in the availability of data. Instead of a world in which we have small number of carefully curated data sources in a centralized database -- instead we have a plethora of datasets, data versions, and data representations that span users, groups, and organizations. Devices and data acquisition tools make it easy to acquire new data, cloud hosting makes it easy to centralize and share files, and cloud data analytics and machine learning tools have driven a desire to integrate and extract value from that data.

What is missing is management tools to centralize and capture such data resources. Data scientists often end up doing redundant work because they have no effective way of finding appropriate resources to reuse and retarget to new applications.

The Juneau Project develops holistic data management tools to find, standardize, and benefit from the existing resources in the data lake.  This extension to Jupyter Notebook is a point of access for our dataset management tools. 

For more on the project, please see:
https://dbappserv.cis.upenn.edu/home/?q=node/259

## Setup

### Prerequisites: relational and graph databases

First, be sure you have installed (1) PostgreSQL and (2) Neo4J.  You can easily do this by downloading [this file](https://bitbucket.org/penndb/pennprov/raw/f6fa02fdebdd1bf99a6abc25f56b9dcaf4d28e26/docker-container/docker-compose.yml) for Docker-Compose, and then running `docker-compose up` from the directory.

This skips the steps you would otherwise have to perform: 

* Run `sudo -u postgres psql` and then enter `\password`.  Set a password for the account (by default this is assumed to be `habitat1`).
* Open your browser to `localhost:7474` and change the password on the `neo4j` password, by default to `habitat1`.

You can edit the settings in `config.py` to match your password and account info.

### Sample data lake corpus

Next, download sample_data.zip and unzip it.

* `neo4j-admin load --database=graph.db --from=juneauG.dump --force`
* `psql -h localhost -u postgres < juneauD.pgsql`

### Install Jupyter Notebook extensions

* `sudo -H python setup.py install`
* `sudo -H jupyter serverextension enable --py juneau`
* `jupyter nbextension install dataset_inspector/ --user`
* `jupyter nbextension enable dataset_inspector/main --user`