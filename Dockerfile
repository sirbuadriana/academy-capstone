FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /app
COPY requirements.txt requirements.txt

USER 0
ENV PIP_NO_CACHE_DIR=1
RUN pip install -r requirements.txt

COPY capstone.py capstone.py

CMD ["python3", "/app/capstone.py"]
