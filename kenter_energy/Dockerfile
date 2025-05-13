ARG BUILD_FROM
FROM $BUILD_FROM

# Install required packages
RUN apk add --no-cache \
    python3 \
    py3-requests \
    py3-dateutil \
    py3-paho-mqtt \
    py3-pip \
    build-base \
    python3-dev

# Install bashio
RUN pip3 install --no-cache-dir bashio

# Create app directory
WORKDIR /app

# Copy application
COPY . .

# Make run script executable
RUN chmod a+x /app/run

# Run script
CMD [ "/app/run" ] 