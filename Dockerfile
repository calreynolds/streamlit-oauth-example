# Use a base image with Python 3.10 and the necessary libraries
FROM python:3.10-slim

# Set the working directory inside the container (it's already root)
WORKDIR /

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire content of your Dash app into the container
COPY . .

# Expose the port your Dash app will run on (change this to your app's port)
EXPOSE 8050

# Command to start your Dash app when the container starts
CMD ["python", "app.py"]
