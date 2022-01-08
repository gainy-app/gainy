FROM python:3.10

WORKDIR /srv
COPY . .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-u"]