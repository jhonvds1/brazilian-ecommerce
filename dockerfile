FROM python:lastest

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY data/ data/

COPY src/ src/

CMD ["python", "-m", "src.main"]
