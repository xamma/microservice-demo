FROM python:3.13-slim

WORKDIR /app

COPY src/requirements.txt /opt/requirements.txt

RUN pip install -r /opt/requirements.txt --no-cache

RUN adduser dogger --system

COPY src/main /app

EXPOSE 8000

USER dogger

CMD ["fastapi", "run", "--port", "8000"]