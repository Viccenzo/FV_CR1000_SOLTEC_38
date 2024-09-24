FROM python:3.10.12

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache -r requirements.txt

COPY src/ .

CMD ["python3", "-u", "CR1000_SOLTEC_data_collector.py"]


