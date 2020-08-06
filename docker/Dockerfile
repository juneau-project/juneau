FROM python:3
COPY . /juneau
WORKDIR /juneau
RUN pip install -e .

# Install the Juneau extension
RUN jupyter serverextension enable --py juneau
RUN jupyter nbextension install juneau/dataset_inspector --user
RUN jupyter nbextension enable dataset_inspector/main --user

# Start jupyter
CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--no-browser", "--allow-root"]