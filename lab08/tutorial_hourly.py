import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "tutorial_hourly",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A tutorial DAG that runs hourly for lab8",
    schedule=timedelta(hours=1),
    start_date=datetime.now(),
    catchup=False,
    tags=["example", "lab8"],
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=textwrap.dedent(
            """
            {% for i in range(5) %}
                echo "{{ ds }}"
                echo "{{ macros.ds_add(ds, 7)}}"
            {% endfor %}
            """
        ),
    )

    t1 >> [t2, t3]
