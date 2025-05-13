#!/usr/bin/env python3
import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
from dateutil import parser

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KENTER_API_URL = os.getenv('KENTER_API_URL', 'https://api.kenter.nu')
KENTER_CLIENT_ID = os.getenv('KENTER_CLIENT_ID')
KENTER_CLIENT_SECRET = os.getenv('KENTER_CLIENT_SECRET')
KENTER_CONNECTION_ID = os.getenv('KENTER_CONNECTION_ID')
KENTER_METERING_POINT = os.getenv('KENTER_METERING_POINT')
MQTT_HOST = os.getenv('MQTT_HOST', 'core-mosquitto')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '3600'))

class KenterEnergyMonitor:
    def __init__(self):
        self.mqtt_client = mqtt.Client()
        self.setup_mqtt()
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        
    def setup_mqtt(self):
        """Setup MQTT connection"""
        if MQTT_USER and MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        
        self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        self.mqtt_client.loop_start()

    def get_jwt_token(self):
        """Get JWT token from Kenter API"""
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token

        try:
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'client_id': KENTER_CLIENT_ID,
                'client_secret': KENTER_CLIENT_SECRET,
                'grant_type': 'client_credentials',
                'scope': 'meetdata.read'
            }

            response = requests.post(
                'https://login.kenter.nu/connect/token',
                headers=headers,
                data=data
            )
            response.raise_for_status()
            data = response.json()
            
            self.access_token = data['access_token']
            if 'refresh_token' in data:
                self.refresh_token = data['refresh_token']
            # Set token expiry based on expires_in (with 5 minute buffer)
            self.token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300)
            return self.access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting JWT token: {e}")
            return None

    def refresh_jwt_token(self):
        """Refresh the JWT token using refresh token"""
        if not self.refresh_token:
            return self.get_jwt_token()

        try:
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'client_id': KENTER_CLIENT_ID,
                'client_secret': KENTER_CLIENT_SECRET,
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }

            response = requests.post(
                'https://login.kenter.nu/connect/token',
                headers=headers,
                data=data
            )
            response.raise_for_status()
            data = response.json()
            
            self.access_token = data['access_token']
            if 'refresh_token' in data:
                self.refresh_token = data['refresh_token']
            self.token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300)
            return self.access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"Error refreshing JWT token: {e}")
            # If refresh fails, try to get a new token
            return self.get_jwt_token()
        
    def fetch_kenter_data(self, date):
        """Fetch data from Kenter API for a specific date"""
        token = self.get_jwt_token()
        if not token:
            return None

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            # Format date components
            year = date.year
            month = str(date.month).zfill(2)
            day = str(date.day).zfill(2)

            # Construct URL for the specific date
            url = f"{KENTER_API_URL}/meetdata/v2/measurements/connections/{KENTER_CONNECTION_ID}/metering-points/{KENTER_METERING_POINT}/days/{year}/{month}/{day}"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Extract consumption and feed-in data
            consumption = 0
            feedin = 0

            # Process the measurements data
            if 'measurements' in data:
                for measurement in data['measurements']:
                    if measurement.get('type') == 'consumption':
                        consumption = measurement.get('value', 0)
                    elif measurement.get('type') == 'feedin':
                        feedin = measurement.get('value', 0)

            return {
                'consumption': consumption,
                'feedin': feedin
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Kenter API: {e}")
            return None

    def publish_sensor_data(self, data, date):
        """Publish sensor data to Home Assistant via MQTT"""
        if not data:
            return

        date_str = date.strftime('%Y-%m-%d')

        # Create sensor configuration
        consumption_config = {
            "name": "Kenter Energy Consumption",
            "unique_id": "kenter_energy_consumption",
            "device_class": "energy",
            "state_class": "total",
            "unit_of_measurement": "kWh",
            "state_topic": "kenter/consumption/state",
            "value_template": "{{ value_json.consumption }}",
            "json_attributes_topic": "kenter/consumption/attributes"
        }

        feedin_config = {
            "name": "Kenter Energy Feed-in",
            "unique_id": "kenter_energy_feedin",
            "device_class": "energy",
            "state_class": "total",
            "unit_of_measurement": "kWh",
            "state_topic": "kenter/feedin/state",
            "value_template": "{{ value_json.feedin }}",
            "json_attributes_topic": "kenter/feedin/attributes"
        }

        # Publish discovery configs
        self.mqtt_client.publish(
            "homeassistant/sensor/kenter_consumption/config",
            json.dumps(consumption_config),
            retain=True
        )
        self.mqtt_client.publish(
            "homeassistant/sensor/kenter_feedin/config",
            json.dumps(feedin_config),
            retain=True
        )

        # Publish states
        self.mqtt_client.publish(
            "kenter/consumption/state",
            json.dumps({"consumption": data.get('consumption', 0), "date": date_str}),
            retain=True
        )
        self.mqtt_client.publish(
            "kenter/feedin/state",
            json.dumps({"feedin": data.get('feedin', 0), "date": date_str}),
            retain=True
        )

        # Publish attributes
        self.mqtt_client.publish(
    def run(self):
        """Main loop"""
        while True:
            try:
                # Calculate yesterday's date
                yesterday = datetime.now() - timedelta(days=1)
                
                # Fetch and publish data
                data = self.fetch_kenter_data(yesterday)
                if data:
                    self.publish_sensor_data(data, yesterday)
                    logger.info(f"Published data for {yesterday.strftime('%Y-%m-%d')}")

                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    monitor = KenterEnergyMonitor()
    monitor.run() 