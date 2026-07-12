FROM python:3.12.4

WORKDIR /app

RUN pip install poetry==2.4.1

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY ./pyproject.toml ./poetry.lock* /app/

RUN poetry cache clear --all pypi
RUN poetry install --only main --no-root

COPY . .

ENTRYPOINT ["poetry", "run", "sh", "prod.sh"]
