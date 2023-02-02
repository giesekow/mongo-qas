FROM python:3.7

WORKDIR /app

RUN pip install mongo-qas-dashboard==0.1.20b0

COPY ./entrypoint.sh .

EXPOSE 5000

CMD [ "bash", "entrypoint.sh" ]