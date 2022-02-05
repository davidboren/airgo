from airgo import DAG
from airgo.operators import BaseOperator
from airgo.utils.decorators import apply_defaults


class HelloWorldOperator(BaseOperator):
    @apply_defaults
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.message = message

    def execute(self, context):
        self.logger.info(self.message)


default_args = {
    "retries": 1,
    "requests_memory": "50Mi",
    "requests_cpu": "10m",
    "limits_memory": "100Mi",
    "limits_cpu": "20m",
}
dag = DAG(
    "hello-world",
    description="Hello World!",
    schedule_interval="15,30,45,100 * * * *",
    default_args=default_args,
    start_date="2019-03-07",
    concurrency_policy="Forbid",
)
hello_1 = HelloWorldOperator(message="Hello from operator1", task_id="hello-1", dag=dag)
hello_2 = HelloWorldOperator(message="Hello from operator2", task_id="hello-2", dag=dag)
hello_1 >> hello_2
hello_3 = HelloWorldOperator(message="Hello from operator3", task_id="hello-3", dag=dag)
hello_3.set_upstream(hello_1)
