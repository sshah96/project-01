FROM python:3.9-slim-bookworm

WORKDIR /app

COPY . .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV API_KEY=<api_key>
ENV DB_SERVER_NAME=<server_name>
ENV DB_DATABASE_NAME=stock
ENV DB_USER=postgres
ENV DB_PASSWORD=<password>
ENV PORT=5432
ENV LOGGING_SERVER_NAME=<server_name>
ENV LOGGING_DATABASE_NAME=stock
ENV LOGGING_USERNAME=postgres
ENV LOGGING_PASSWORD=<password>
ENV LOGGING_PORT=5432

CMD ["python", "-m", "app.pipelines.stocks"]