import datetime as dt
import pendulum
from data_analysis import Analysis
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone('Asia/Shanghai')

default_args = dict(
    start_date=dt.datetime(2019, 9, 10, tzinfo=local_tz),
    owner='airflow'
)

dag = DAG(dag_id="update_customer_service",
          default_args=default_args,
          schedule_interval='0 18 * * *')

conn = "postgresql+psycopg2://sunmiai:7jetVZJ0cZ7B25ei@pgm-bp199af1e28ooii114870.pg.rds.aliyuncs.com:3433/customer_service"


def product_info(ds, **kwargs):
    print("Extracting product info ...")
    analysis = Analysis(30)
    product_stat = analysis.product_stat()
    engine = sa.create_engine(conn)

    query = "delete from product;"
    engine.execute(query)
    product_stat.to_sql('product', engine, if_exists='append', index=False)

    print("Extracting product info is finished ...")


def topic_info(ds, **kwargs):
    print("Extracting topic info starts ...")
    analysis = Analysis(30)
    function_stat = analysis.function_stat()
    engine = sa.create_engine(conn)

    query = "delete from component;"
    engine.execute(query)
    function_stat.to_sql('component', engine, if_exists='append', index=False)
    print("Extracting topic info is finished ...")


update_product_info = PythonOperator(
    task_id="product_info",
    provide_context=True,
    python_callable=product_info,
    dag=dag
)


update_topic_info = PythonOperator(
    task_id="topic_info",
    provide_context=True,
    python_callable=topic_info,
    dag=dag
)


if __name__ == '__main__':
    topic_info(None)