FROM python:3.10
WORKDIR /app
COPY ETL_Polygol.py .
COPY claves.json .
COPY requirements.txt .
RUN pip install -r /app/requirements.txt
CMD [ "python", "-u","ETL_Polygol.py" ]