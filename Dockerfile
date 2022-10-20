FROM apache/airflow:2.4.1

USER root
RUN apt-get update

# install gcloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y

# install mysql
RUN apt-get install -y python3-dev default-libmysqlclient-dev build-essential

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir mysqlclient