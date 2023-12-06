FROM python:3.10
RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
COPY ValueAtRisk-VaR-MonteCarlo-UsingBQ.py app.py
RUN pip install -r requirements.txt
CMD ["python", "app.py"]