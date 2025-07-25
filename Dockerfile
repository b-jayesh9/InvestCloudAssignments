# --- Stage 1: Base Image ---
# Use an official, slim Python image as our starting point.
# 'bullseye' is a stable Debian release with good package support. It's a great balance
# between small size and having the tools we need (like apt-get).
FROM python:3.11-slim-bullseye

# --- Stage 2: Environment Setup ---
# Set environment variables for best practices when running Python in a container.
ENV PYTHONDONTWRITEBYTECODE 1  # Prevents Python from writing .pyc files, keeping the filesystem clean.
ENV PYTHONUNBUFFERED 1         # Ensures that logs and print statements are sent straight to the terminal without being buffered.

# Set the working directory inside the container. All subsequent commands will run from here.
WORKDIR /app

# --- Stage 3: Install System Dependencies ---
# PySpark requires a Java Runtime Environment (JRE) to function.
# We install only the "headless" JRE, which is smaller because it excludes GUI components.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    # Clean up the apt cache to keep the final image as small as possible.
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- Stage 4: Install Python Dependencies ---
# Copy only the requirements file first. This is a key optimization.
# Docker caches this layer, so it will only re-run 'pip install' if requirements.txt changes.
COPY requirements.txt .

# Install the Python packages from the requirements file.
# The --no-cache-dir flag prevents pip from storing a cache, which also helps keep the image slim.
RUN pip install --no-cache-dir -r requirements.txt

# --- Stage 5: Copy Application Code ---
# Now that dependencies are installed, copy the rest of our application source code.
# Because this is a separate layer, any changes to our Python code will be very fast to
# rebuild, as Docker will use the cached dependency layer from above.
COPY . .

# --- Stage 6: Define Execution Command ---
# Specify the default command to run when the container starts.
# This will execute the main entry point of our application.
CMD ["python", "main.py"]