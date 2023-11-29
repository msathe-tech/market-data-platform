FROM python:3.10
RUN pip install --upgrade pip
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "ValueAtRisk-VaR-MonteCarlo-UsingBQ.py"]