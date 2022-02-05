FROM ${SET BASE PYTHON IMAGE HERE}

ARG GITHUB_OAUTH_TOKEN

WORKDIR /pipenv

RUN apt update
RUN apt install -y git

WORKDIR /app

COPY ./pyproject.toml /pipenv/poetry.lock
COPY ./poetry.lock /pipenv/poetry.lock

RUN pip3 install pipenv
RUN pipenv install --system --deploy

RUN mkdir -p /app

COPY ./airgo airgo
COPY ./dags /app/dags


CMD ["python", "-m", "airgo.dag_entrypoint"]