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
RUN conda install -c conda-forge airflow
RUN conda install -c conda-forge sweetviz
RUN conda install -c conda-forge pyarrow
RUN conda install -c anaconda psycopg2
RUN pip install pandas-profiling

ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/work"

RUN echo "export PYTHONPATH=/home/jovyan/work" >> ~/.bashrc

WORKDIR /home/jovyan/work

ENV JUPYTER_ENABLE_LAB=yes