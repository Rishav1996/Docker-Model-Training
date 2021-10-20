FROM continuumio/miniconda3
WORKDIR /app
COPY requirements.txt ./
COPY models/train.py ./
RUN pip install -r requirements.txt -q
ENTRYPOINT ["python", "train.py"]
