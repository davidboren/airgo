# Airgo

Many times we have simple workflows that aren't in need of a complicated multi-container argo workflow, but can still benefit from containerized deployment without cross-pollination of dependencies with other workflows (looking at you, airflow).

This project is meant to allow for simple porting of airflow-like dags into the argo system, with the special emphasis on running single-container or low-number of container workflows.

# Installation

```bash
pip3 install -e git+ssh://git@github.com/davidboren/airgo#egg=airgo
```

# Initialization

In your project directory, run

```
airgo init --project-name my-project-name
```

This will create an airgo directory containing a templates directory for templating, a default Dockerfile and entrypoint.py in the root directory, as well as a dags directory with a simple hello_world.py workflow. The project name is used in prefixing argo and argo-events resources, so choose characters in way compatible with kubernetes resource naming conventions.

## Example DAG

The purpose of this project is allow for relatively painless porting of simple (!!) airflow-like dags.  This means the construction and executing code of a dag looks extremely similar, with near identical imports as
well as operator-overloading syntactical sugar for setting upstream and downstream dependencies.

```python
from airgo import DAG
from airgo.operators import BaseOperator
from airgo.utils.decorators import apply_defaults


class HelloWorldOperator(BaseOperator):

    @apply_defaults
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.message = message

    def execute(self):
        self.logger.info(self.message)


default_args = {
    'retries': 1,
    'requests_memory': '10Mi',
    'requests_cpu': '10m',
    'limits_cpu': '20Mi',
    'limits_memory': '20m',
}

dag = DAG('hello-world',
          description='Hello World!',
          schedule_interval='* */4 * * *',
          default_args=default_args,
          )

hello_1 = HelloWorldOperator(message="Hello from operator1", task_id="hello-1", dag=dag)

hello_2 = HelloWorldOperator(message="Hello from operator2", task_id="hello-2", dag=dag)

hello_1 >> hello_2

hello_3 = HelloWorldOperator(message="Hello from operator3", task_id="hello-3", dag=dag)

hello_3.set_upstream(hello_1)
```

## Note on dag\_ids, task_ids, and parameters:

Anything rendered into kubernetes argo templates (essentially `task_id`, `dag_id`, and additional `parameter` names) has a particular formatting requirement that we use alphanumeric characters and hyphens, so to avoid unintuitive character stripping/reformatting we do not allow underscores or other characters in these names at all.

# Running locally

If you only have the one container environment, it's possible to execute the entire dag locally in a repl or notebook:

```
from dags.my_dag_file import dag

dag.execute()
```

In addition, you can execute a task and all its dependencies locally as well:

```
from dags.my_dag_file import hello_3

hello_3.execute_all()
```

If you need to execute that task ALONE locally (helpful for debugging multi-container dags):

```
from dags.my_dag_file import dag, hello_3

hello_3.execute(context=dag.default_context)
```

Notice we pass a context dictionary object into the dag, which really just contains the `execution_date` for the dag.  This context can be used to parameterize outputs, if desired.

# Rendering

In your project directory, run

```
airgo render
```

This will render workflows with scheduling into a single "airgo/rendered\_yamls/scheduled\_workflows.yaml" ready for deployment via kubectl. In addition, should these workflows have a start_date parameter specified, they will have corresponding yamls ready for backfilling in the "airgo/rendered\_yamls/backfill\_workflows_" subdirectory.


Workflows without a schedule_interval parameter are rendered individually into yamls in the "airgo/rendered\_yamls/" subdirectory.


# Submission from Cron

You can submit a workflow from a deployed cronworkflow via the argo command. This requires that argo be installed locally:

```bash
brew install argoproj/tap/argo
```

Then issue a submission:

```bash
argo submit --from "CronWorkflow/my-dag-name"
```

# Dynamic Parallelization

Argo supports dynamic parallelization of tasks from artifacts passed as a json-serializable list input parameters to multiple downstream tasks.  We simplify this with an `artifact_property` decorator and a `RuntimeParallelizeOperator` that will give you a parameter property with access to it's individual value of a parameterized upstream output.  For example, assuming we have a dag already instantiated, if we want to parallelize across a simple list of integers of random length we can create the following task:

```python
from random import randint

from airgo.operators.base_operator import BaseOperator
from airgo.utils.decorators import artifact_property


class ArtifactGenerator(BaseOperator):
    def execute(self, context):
        self.logger.info("Executing Artifact Generator")

    @artifact_property()
    def stuff(self):
        return [i for i in range(randint(1, 20))]

parallelize = ArtifactGenerator(task_id="gen-artifact", dag=dag)
```

Then we can create a subclass of `RuntimeParallelizeOperator` and access the individual parameter value in our execute function:

```python
from airgo.operators.parallel import RuntimeParallelizeOperator

class Parameterized(RuntimeParallelizeOperator):
    def execute(self, context):
        self.logger.info(f"Got parameter '{self.parameter}'")

parameterized = Parameterized(
    task_id="parameterized", runtime_task=hello_1, property_name="stuff", dag=dag
)
```

Note that we must specify the property name of our runtime task across which to parallelize.

# Short Circuits

Occasionally we might want to terminate a dag based on a runtime condition (data not available or delayed, etc...) without failing explicitly.  In argo this amounts to a `when` condition placed upon subsequent tasks. This can be accomplished in airgo by subclassing your operator with the `ShortCircuitOperator` and implementing a simple boolean property (named `short_circuit_when`) indicating whether or not to short circuit the dag.  A `True` value indicates that the downstream tasks should be aborted.  The following operator randomly short-circuits the dag when a randomly drawn integer is less than some input value:

```python
from random import randint

from airgo.operators.short_circuit import ShortCircuitOperator
from airgo.utils.decorators import apply_defaults


class MyShortCircuitOp(ShortCircuitOperator):
    @apply_defaults
    def __init__(self, max_value, **kwargs):
        super().__init__(**kwargs)
        self.max_value = max_value

    def execute(self, context):
        self.logger.info(f"Max value is {self.max_value}")

    @property
    def short_circuit_when(self):
        return randint(0, 100) < self.max_value

sc = MyShortCircuitOp(task_id="short-circuit-task", max_value=50, dag=dag)
```

# Backfilling

Every task is given a context dictionary object, just as in airflow, with an 'execution_date' datetime object.  If you wish to parameterize this dag via this object it is often useful to be able to backfill for ranges.  Airgo supports this via argo workflow parameterization.  We simply support the airflow synatx of using start\_date as a dag-level input parameter and handle the invocation of the backfill via a simple argo submit command.In your project directory, you can then run

```bash
airgo backfill --dag-id my-dag-id
```
or optionally
```bash
airgo backfill --dag-id my-dag-id --end-date '2019-01-01 00:00:00' # Uses dag's start_date parameter for airflow-like compatibility
```
or
```bash
airgo backfill --dag-id my-dag-id --start-date '2019-01-01 00:00:00' --end-date '2019-02-01 00:00:00'
```

Where the "start-date" and "end-date" parameters correspond to the start and end, respectively, of the backfill period.

## Concurrency Policy 

Each dag has a `concurrency_policy` argument, which defaults to `Forbid`.  When set to `Allow`, argo will allow multiple runs of the workflow to exist simultaneously. When set to `Replace` the workflow will start over. 


## Templates

The airgo templates directory contains a subdirectory (`containers`) for container templates for use by your dags.  In it's simplest, most easily testable form you get a `default.j2` container template with an image tag that you must set yourself to your repository/image path.

```jinja2
container:
  image: ${SET IMAGE HERE}
  env:
  - name: CREATION_TIMESTAMP
    value: '{{workflow.creationTimestamp}}'
  - name: CREATION_TIMESTAMP_OVERRIDE
    value: '{{workflow.parameters.creationTimestampOverride}}'
  - name: TASK_ID
    value: '{{inputs.parameters.task-id}}'
  - name: DAG_ID
    value: '{{inputs.parameters.dag-id}}'
metadata:
  annotations:
    iam.amazonaws.com/role: ds-argo
inputs:
  parameters: []
```

Any additional templates placed in this folder can be referenced at the operator level upon instantiation of the operator by passing in the `template="my_template_name"` arg to init function.  This arg defaults to `default_template`.

## Configuration

You can also modify the templates in the `template\configuration` directory to modify labels or other settings on your workflow and cron_workflow objects.

By default every container in your graph will have `dag_id` and `task_id` set, allowing you to easily find them in kibana using `kubernetes.labels.dag_id` and `kubernetes.labels.task_id` filters.

## Parameters

The cpu limits/requests as well as the number of retries should be set at the operator level (or via the dag-level dictionary `default_args`) by passing them into the operator as args `limits_cpu`, `limits_memory`, `cpu_memory`, `requests_memory`, and `retries` args.  Since argo doesn't currently support parametrizing these values, we set them under the hood as new templates. 

## Multi-container setup

If you do wish to use this package with a multi-container setup where different dags/operators use different dependencies, the easiest way to go about it is through lazy importing, so that all operators/dags can still be imported by the entrypoint.


To further parameterize external containers, (or add different functionality to the entrypoint) you can add parameterized env variables to these templates by setting them in a dictionary of arbitrary parameter values as the operator-level arg `parameters`.

## Testing

We supply a simple test that ensures the dag folder can be traversed using the function that does so in the default entrypoint.py.  This test uses pytest and simply runs through your dags folder, instantiating all objects.  For your own testing you can always use this function, assuming your current working directory is your root project directory, to invoke dags or tasks individually in tests:

```python
from airgo import traverse_dags_folder

# Assumes current working directory
dag_dict = traverse_dags_folder()

# Uses full path
dag_dict = traverse_dags_folder("/full/path/to/dags")
```

Executing a particular graph sequentially is possible by calling the execute() function on a dag:

```python
dag_dict['my_dag_id'].execute()
```

Obviously you should parameterize your testing environment appropriately to ensure you run a scaled-down version of your dag.  In addition, this is likely only to be useful for single-container workflows.
