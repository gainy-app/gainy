FROM python:3.9-slim-buster

WORKDIR /srv
COPY . .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-u"]