
Calling ETL Helper scripts from Apache Airflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is an `Apache Airflow
DAG <https://airflow.apache.org/docs/stable/concepts.html>`__ that uses
the ``copy_readings`` function defined in the script above. The Airflow
scheduler will create tasks for each day since 1 August 2019 and call
``copy_readings`` with the appropriate start and end times.

.. code:: python

   # readings_dag.py

   import datetime as dt
   from airflow import DAG
   from airflow.operators.python_operator import PythonOperator
   import copy_readings


   def copy_readings_with_args(**kwargs):
       # Set arguments for copy_readings from context
       start = kwargs.get('prev_execution_date')
       end = kwargs.get('execution_date')
       copy_readings.copy_readings(start, end)

   dag = DAG('readings',
             schedule_interval=dt.timedelta(days=1),
             start_date=dt.datetime(2019, 8, 1),
             catchup=True)

   t1 = PythonOperator(
       task_id='copy_readings',
       python_callable=copy_readings_with_args,
       provide_context=True,
       dag=dag)