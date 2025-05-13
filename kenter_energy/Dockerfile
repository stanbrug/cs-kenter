ARG BUILD_FROM
FROM $BUILD_FROM

# Install required packages
RUN apk add --no-cache \
    python3 \
    py3-requests \
    py3-dateutil \
    py3-paho-mqtt

# Create app directory
WORKDIR /app

# Copy application
COPY . .

# Make run script executable
RUN chmod a+x /app/run

# Run script
CMD [ "/app/run" ] 