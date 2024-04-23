from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
import os
import smtplib

def enviar():
    try:
        email_from = os.environ.get('EMAIL_FROM')
        pass_email_from = Variable.get("SECRETS_EMAIL_KEY")
        email_to = os.environ.get('EMAIL_TO')
        x=smtplib.SMTP('smtp.mailersend.net',587)
        x.starttls()
        x.login(email_from,pass_email_from)
        subject='Ganaste un premio'
        body_text='Has ganado un premio fantastico!!!!'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(email_from,email_to,message)
        print('Exito')
    except Exception as exception:
        print('Se rompe el job',exception)
        print('Failure')

default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2022,9,6)
}

with DAG(
    dag_id='dag_smtp_email_automatico',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    ) as dag:

    tarea_1=PythonOperator(
        task_id='dag_envio',
        python_callable=enviar
    )

    tarea_1
