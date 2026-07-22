FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY sql ./sql
RUN pip install --no-cache-dir .

CMD ["python", "-m", "order_command_center.pipelines.postgres", "--forever", "--quiet"]
