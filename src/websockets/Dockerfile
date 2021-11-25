FROM python:3.10

WORKDIR /srv
COPY . .
RUN pip3 install -r requirements.txt

CMD ["python3", "-u", "client.py"]