FROM alpine:3.18

# Install Python and pip
RUN apk update && apk add --no-cache \
    python3 \
    py3-pip \
    && pip3 install --no-cache-dir --upgrade pip

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python3", "working_metrics_ascii.py"]