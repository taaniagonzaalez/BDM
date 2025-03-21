# Use a base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose a port (if needed)
EXPOSE 5000

# Define the command to run the application
CMD ["python", "app.py"]