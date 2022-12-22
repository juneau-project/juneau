# The Juneau Project

[![juneau-project](https://circleci.com/gh/juneau-project/juneau.svg?style=shield)](https://app.circleci.com/pipelines/github/juneau-project)
 
The past decade has brought a sea change in the availability of data. Instead of a world in which we have small number 
of carefully curated data sources in a centralized database -- instead we have a plethora of datasets, data versions, 
and data representations that span users, groups, and organizations. Devices and data acquisition tools make it easy to 
acquire new data, cloud hosting makes it easy to centralize and share files, and cloud data analytics and machine learning
 tools have driven a desire to integrate and extract value from that data.

We have been missing management tools to centralize and capture such data resources. Data scientists often end up doing
 redundant work because they have no effective way of finding appropriate resources to reuse and retarget to new applications.

The Juneau Project develops holistic data management tools to find, standardize, and benefit from the existing resources
 in the data lake.  This extension to Jupyter Notebook is a point of access for our dataset management tools. 

For more on the project, please see the project
[home](https://dbappserv.cis.upenn.edu/home/?q=node/259), as well as our research papers:

* [Finding Related Tables in Data Lakes for Interactive Data Science](https://www.cis.upenn.edu/~zives/research/Finding_Related_Tables_in_Data_Lakes_for_Interactive_Data_Science.pdf). Yi Zhang and Zachary G. Ives. SIGMOD 2020.
* [Dataset Relationship Management](http://cidrdb.org/cidr2019/papers/p55-ives-cidr19.pdf). Yi Zhang, Soonbo Han, Nan Zheng. CIDR 2019.

## Setup

### Prerequisites: relational and graph databases

#### Simple Version

Git clone the repo and build the docker juneau image:

```bash
docker build -t juneau -f docker/Dockerfile .
```

Now that we have built Juneau's image, run the three services (Postgres, Neo4j, and Juneau) via
docker-compose:

```bash
docker-compose -f docker/docker-compose.yaml up
```

* Copy `juneau/config-simple.py` to `juneau/config.py`

That's it! As you would normally do, head over to the link that Jupyter will show on the terminal.


#### Simple Version Using PennProv

Install Docker, including docker-compose, for your preferred operating system.

* Download [this file](https://bitbucket.org/penndb/pennprov/raw/f6fa02fdebdd1bf99a6abc25f56b9dcaf4d28e26/docker-container/docker-compose.yml) for Docker-Compose
* Run `docker-compose up` from the directory.
* Copy `juneau/config-mprov.py` to `juneau/config.py`

These will use the default user IDs and passwords that exist in `config.yaml`.  You should change the password 

#### Custom Version

First, be sure you have installed:

* PostgreSQL, version 12 or later
* Neo4J 3.3, version or later

Then set up a default user ID and password for each: 

* Run `sudo -u postgres psql` and then enter `\password`.  Set a password for the account (by default this is assumed to be `habitat1`).
* Open your browser to `localhost:7474` and change the password on the `neo4j` password, by default to `habitat1`.
* Copy `juneau/config-mprov.yaml` to `juneau/config.yaml` 

Now either edit the YAML file in `juneau/config/config.yaml` to match your password and account info or
change the environment variables in your terminal.

### Sample data lake corpus

Next, download [juneau_start.zip](https://slack-files.com/T239LE4BH-F017E751CR2-dc5d4c5c86) and unzip it.

For the Docker container, you can import as follows:
* Run `./neo4j-update.sh`

Otherwise, you can use:
* `neo4j-admin load --database=data.db --from=juneauG.dump --force`
* `psql -h localhost -U postgres < juneauD.pgsql`

And finally you need to edit the `neo4j.conf` file to set the database
to `data.db`.

### Install Jupyter Notebook extensions

See the [Developer's Guide](docs/Developers.md) for details.

* `sudo -H python setup.py install`
* `sudo -H jupyter serverextension enable --py juneau`
* `jupyter nbextension install dataset_inspector --user`
* `jupyter nbextension enable dataset_inspector/main --user`

### Install SQL UDFs

Copy the `postgres` directory into your `hab-postgres` docker container:

* `docker cp join-size/ docker-container_postgres_1:/juneau_funcs`
* `docker cp sketch/ docker-container_postgres_1:/juneau_funcs`


Log into your `hab-postgres` container with the interactive terminal:

```
apt update
apt install -y postgresql-server-dev-15
apt install -y gcc g++

mkdir /juneau_funcs/
cd /juneau_funcs/
cd join_size/c
cc -fPIC -c -I /usr/include/postgresql/15/server/ join_score.cpp score.cpp
cc -shared -o join_score.so join_score.o score.o
cd ../../sketch/c/ks
cc -fPIC -c -I /usr/include/postgresql/15/server/ ks.cpp hist.cpp evaluate.cpp
cc -shared -o ks.so ks.o hist.o evaluate.o
cd ../lshe
cc -fPIC -c -I /usr/include/postgresql/15/server/ -Ifnv/ fnv/hash_64a.c evaluate.cpp hash.cpp lshe.cpp probability.cpp sig.cpp
```
