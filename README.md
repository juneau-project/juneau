# notebook_data_extension
python setup.py install

cd data_extension

jupyter serverextension enable --py data_extension

jupyter nbextension install varInspector/ --user

jupyter nbextension enable varInspector/main --user