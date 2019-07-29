# The Juneau Project

The past decade has brought a sea change in the availability of data. Instead of a world in which we have small number of carefully curated data sources in a centralized database -- instead we have a plethora of datasets, data versions, and data representations that span users, groups, and organizations. Devices and data acquisition tools make it easy to acquire new data, cloud hosting makes it easy to centralize and share files, and cloud data analytics and machine learning tools have driven a desire to integrate and extract value from that data.

What is missing is management tools to centralize and capture such data resources. Data scientists often end up doing redundant work because they have no effective way of finding appropriate resources to reuse and retarget to new applications.

The Juneau Project develops holistic data management tools to find, standardize, and benefit from the existing resources in the data lake.  This extension to Jupyter Notebook is a point of access for our dataset management tools. 

For more on the project, please see:
https://dbappserv.cis.upenn.edu/home/?q=node/259

## Setup

* `sudo -H python setup.py install`
* `sudo -H jupyter serverextension enable --py data_extension`
* `jupyter nbextension install dataset_inspector/ --user`
* `jupyter nbextension enable dataset_inspector/main --user`