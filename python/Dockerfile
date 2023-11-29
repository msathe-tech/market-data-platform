FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN mkdir -p /var/log/app
RUN chown -R app:app /var/log/app

CMD ["python", "ValueAtRisk-VaR-MonteCarlo-UsingBQ.py"]