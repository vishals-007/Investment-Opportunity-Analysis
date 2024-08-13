import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

dag=DAG(
    dag_id='Investment_Analysis_Final',
    description='daily_download_stock_data',
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
)
bashop=BashOperator(
    task_id='download_stock_data',
    bash_command="/home/talentum/shared/Project/Project_Final/daily_data_final.sh ",
    dag=dag
)
sparksub=SparkSubmitOperator(
    application='/home/talentum/shared/Project/Project_Final/hive_daily_upload_final.py',
    conn_id='spark_default',
    task_id='hive_upload',
    dag=dag
)
sparksub1=SparkSubmitOperator(
    application='/home/talentum/shared/Project/Project_Final/investment_analysis_final.py',
    conn_id='spark_default',
    task_id='portfolio_analysis',
    dag=dag
)
bashop1=BashOperator(
    task_id='move_csvfile',
    bash_command="/home/talentum/shared/Project/Project_Final/copycsv_final.sh ",
    dag=dag
)
bashop >> sparksub >> sparksub1 >> bashop1
