# Use lightweight Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file into the container
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project files
COPY . .

# Expose port (Dash defaults to 8050, Cloud Run requires 8080)
EXPOSE 8080

# Run the app with Gunicorn (recommended for production)
# "app:server" means "from app.py, take the variable 'server'"
CMD ["gunicorn", "-b", ":8080", "dash_app:server"]
