
FROM python:3.12

WORKDIR /pipeline

COPY pyproject.toml ./

RUN pip install poetry

RUN poetry install --no-root

COPY . .

EXPOSE 5000

CMD ["poetry", "run", "python", "app.py"]