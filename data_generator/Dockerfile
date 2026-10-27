FROM python:3.12.8-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replay.py .

# -u: in log ngay, không bị giữ lại trong bộ đệm
CMD ["python", "-u", "replay.py"]
