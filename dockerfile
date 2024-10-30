# Use an official Python image
FROM python:3.11

WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && apt-get install -y librdkafka-dev

# Install dependencies
COPY requirements.txt /usr/src/app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . /usr/src/app

# Give executable permission to app.py (if necessary)
RUN chmod +x app.py

# Run the Faust application
CMD ["python", "app.py"]

