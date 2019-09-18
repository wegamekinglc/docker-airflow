import datetime as dt
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone('Asia/Shanghai')

default_args = dict(
    start_date=dt.datetime(2019, 9, 10, tzinfo=local_tz),
    owner='airflow'
)

dag = DAG(dag_id="update_chatbot_models",
          default_args=default_args,
          schedule_interval='0 9 * * *')


def customer_service_model(ds, **kwargs):
    print("Model building starts ...")
    print("Model building is finished ...")


update_customer_service_model = PythonOperator(
    task_id="custom_service_model",
    provide_context=True,
    python_callable=customer_service_model,
    dag=dag
)


