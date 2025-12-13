FROM python:3.11-slim

# Create a non-root user for safety
RUN useradd -ms /bin/bash sandbox
USER sandbox
WORKDIR /home/sandbox

# Install any dependencies you want inside the runner
RUN pip install --no-cache-dir numpy pandas mlxtend 

# Copy the execution script
COPY runner.py .

# Run the script when the container starts
CMD ["python", "runner.py"]
