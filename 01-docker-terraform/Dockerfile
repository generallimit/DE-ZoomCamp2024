FROM python:3.11.1

RUN apt-get update && apt-get install wget -y
RUN pip install pandas pyarrow sqlalchemy psycopg2-binary glob2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
