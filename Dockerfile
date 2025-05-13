ARG BUILD_FROM
FROM $BUILD_FROM

# Install required packages
RUN apk add --no-cache python3 py3-pip

# Create app directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Run script
CMD [ "python3", "-u", "kenter_energy.py" ] 