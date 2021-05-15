FROM jupyter/scipy-notebook:latest as build

LABEL maintainer="Jupyter Project <jupyter@googlegroups.com>"

# Fix DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

USER $NB_UID

RUN conda install python=3.8
RUN conda install pandas
RUN conda install -c conda-forge jupyterlab_execute_time
RUN conda install -c conda-forge python-dotenv
<<<<<<< Updated upstream
RUN conda install -c conda-forge airflow=2.0.1
RUN conda install -c conda-forge apache-airflow-providers-postgres
||||||| ancestor
RUN conda install -c conda-forge airflow
RUN conda install -c conda-forge sweetviz
=======
RUN conda install -c conda-forge airflow=2.0.1
RUN conda install -c conda-forge sweetviz
>>>>>>> Stashed changes
RUN conda install -c conda-forge pyarrow
RUN conda install -c anaconda psycopg2
<<<<<<< Updated upstream
RUN conda install -c conda-forge geopandas
RUN conda install -c anaconda openpyxl
RUN conda install -c conda-forge geoalchemy2
||||||| ancestor
RUN pip install pandas-profiling
=======
RUN pip install pandas-profiling
RUN conda install -c conda-forge apache-airflow-providers-postgres
RUN conda install -c anaconda pytest

>>>>>>> Stashed changes

ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/work"

RUN echo "export PYTHONPATH=/home/jovyan/work" >> ~/.bashrc

WORKDIR /home/jovyan/work

ENV JUPYTER_ENABLE_LAB=yes