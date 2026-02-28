FROM python:latest

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY data/ data/

COPY src/ src/

COPY credentials/ credentials/

CMD ["python", "-m", "src.main"]
