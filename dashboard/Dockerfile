FROM python:3.7

WORKDIR /app

RUN pip install mongo-qas-dashboard

COPY ./entrypoint.sh .

EXPOSE 5000

CMD [ "bash", "entrypoint.sh" ]