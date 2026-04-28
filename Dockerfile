FROM python:3.12.4

WORKDIR /app

RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* /app/

RUN poetry cache clear --all pypi
RUN poetry lock && poetry install --no-root

COPY . .

ENTRYPOINT ["poetry", "run", "sh", "prod.sh"]
