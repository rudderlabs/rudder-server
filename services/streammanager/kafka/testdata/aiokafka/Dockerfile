# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

ENV KAFKA_BROKER=kafka1:9094
ENV EXPECTED_MESSAGE_COUNT=3

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["tail", "-f", "/dev/null"]