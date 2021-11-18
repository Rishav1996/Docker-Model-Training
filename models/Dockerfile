FROM continuumio/miniconda3:4.10.3-alpine
WORKDIR /app
COPY requirements.txt ./
COPY train.py ./
RUN pip install -r requirements.txt -q
ENTRYPOINT ["python", "train.py"]
