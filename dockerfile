FROM apache/airflow:2.7.2
COPY requirements.txt .
RUN pip install pandas==1.4.2
#RUN mkdir -p /code/static
#RUN echo $(ls -al)
#RUN echo $(ls -al /code)
#RUN chmod -R 777 /code/static/
#RUN chmod -R 777 airflow-docker