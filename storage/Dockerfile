FROM python:3

LABEL maintainer="jerryhunt"

RUN mkdir /app

# Copy requirements first (better Docker caching)
COPY requirements.txt /app/requirements.txt

WORKDIR /app

# Install dependencies
RUN pip3 install -r requirements.txt

# Copy service code
COPY . /app

# Run as non-root user
RUN chown -R nobody:nogroup /app
USER nobody

# Service port
EXPOSE 8080

ENTRYPOINT ["python3"]
CMD ["app.py"]