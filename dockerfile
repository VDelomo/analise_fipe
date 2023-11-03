FROM apache/airflow:2.2.4
COPY requirements.txt .
RUN mkdir -p /code/static
RUN echo $(ls -al)
RUN echo $(ls -al /code)
RUN chmod -R 777 /code/static/
RUN chmod -R 777 airflow-docker