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
MQTT_RECONNECT_DELAY = 5  # seconds between reconnection attempts

class KenterEnergyMonitor:
    def __init__(self):
        self.mqtt_client = None
        self.mqtt_connected = False
        self.setup_mqtt()
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        
    def setup_mqtt(self):
        """Setup MQTT connection"""
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT broker successfully")
                self.mqtt_connected = True
            else:
                error_messages = {
                    1: "Incorrect protocol version",
                    2: "Invalid client identifier",
                    3: "Server unavailable",
                    4: "Bad username or password",
                    5: "Not authorized",
                }
                error_msg = error_messages.get(rc, f"Unknown error code: {rc}")
                logger.error(f"Failed to connect to MQTT broker: {error_msg}")
                if rc == 4:
                    logger.error("Please check your MQTT username and password configuration")
                elif rc == 5:
                    logger.error("Please check if your MQTT user has the correct permissions")
                self.mqtt_connected = False
        
        def on_disconnect(client, userdata, rc):
            self.mqtt_connected = False
            if rc != 0:
                logger.error(f"Unexpected MQTT disconnection. Error code: {rc}")
            else:
                logger.info("Disconnected from MQTT broker")

        # Create new client instance
        if self.mqtt_client:
            try:
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
            except:
                pass
        
        client_id = f"kenter-energy-{int(time.time())}"
        self.mqtt_client = mqtt.Client(client_id=client_id, clean_session=True)
        
        # Set callbacks
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_disconnect = on_disconnect

        # Setup MQTT authentication
        if MQTT_USER and MQTT_PASSWORD:
            logger.info(f"Configuring MQTT authentication for user: {MQTT_USER}")
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        else:
            logger.warning("No MQTT credentials provided. Please configure MQTT username and password in the addon configuration.")
            logger.warning("To configure MQTT credentials:")
            logger.warning("1. Go to Home Assistant addon configuration for Kenter Energy Monitor")
            logger.warning("2. Set mqtt_user and mqtt_password")
            logger.warning("3. Restart the addon")
            raise Exception("MQTT credentials required but not configured")

        # Enable MQTT logging
        self.mqtt_client.enable_logger(logger)

        self.connect_mqtt()
        
    def connect_mqtt(self):
        """Establish MQTT connection with retry logic"""
        while not self.mqtt_connected:
            try:
                logger.info(f"Attempting to connect to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
                self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
                self.mqtt_client.loop_start()
                
                # Wait for connection or failure
                timeout = time.time() + 10  # 10 second timeout
                while not self.mqtt_connected and time.time() < timeout:
                    time.sleep(0.1)
                
                if not self.mqtt_connected:
                    logger.error("MQTT connection attempt timed out")
                    raise Exception("Connection timeout")
                
                return True
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}")
                time.sleep(MQTT_RECONNECT_DELAY)
        
        return False

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

            # Initialize consumption and feed-in totals
            consumption_total = 0
            feedin_total = 0

            # Process the measurements data
            for channel in data:
                channel_id = channel.get('channelId')
                
                if channel_id == '16180':  # Consumption channel
                    # Sum up all valid measurements for consumption
                    for measurement in channel.get('Measurements', []):
                        if measurement.get('status') == 'Valid':
                            consumption_total += measurement.get('value', 0)
                
                elif channel_id == '16280':  # Feed-in channel
                    # Sum up all valid measurements for feed-in
                    for measurement in channel.get('Measurements', []):
                        if measurement.get('status') == 'Valid':
                            feedin_total += measurement.get('value', 0)

            logger.info(f"Processed data for {year}-{month}-{day}:")
            logger.info(f"Total consumption: {consumption_total:.3f} kWh")
            logger.info(f"Total feed-in: {feedin_total:.3f} kWh")

            return {
                'consumption': round(consumption_total, 3),
                'feedin': round(feedin_total, 3)
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Kenter API: {e}")
            return None

    def publish_with_retry(self, topic, payload, retain=True, retries=3):
        """Publish with retry logic"""
        if not self.mqtt_connected:
            logger.warning("MQTT not connected, attempting to reconnect...")
            if not self.connect_mqtt():
                logger.error("Failed to reconnect to MQTT broker")
                return False

        for attempt in range(retries):
            try:
                result = self.mqtt_client.publish(topic, payload, retain=retain)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    return True
                logger.error(f"Failed to publish to {topic} (attempt {attempt + 1}/{retries})")
                if not self.mqtt_connected:
                    if not self.connect_mqtt():
                        logger.error("Failed to reconnect to MQTT broker")
                        return False
                time.sleep(1)  # Wait before retry
            except Exception as e:
                logger.error(f"Error publishing to {topic}: {e}")
                if not self.connect_mqtt():
                    return False
        return False

    def publish_sensor_data(self, data, date):
        """Publish sensor data to Home Assistant via MQTT"""
        if not data:
            return

        date_str = date.strftime('%Y-%m-%d')

        # Create sensor configuration
        consumption_config = {
            "name": "Kenter Energy Consumption",
            "unique_id": "kenter_energy_consumption",
            "object_id": "kenter_energy_consumption",
            "device_class": "energy",
            "state_class": "total_increasing",
            "unit_of_measurement": "kWh",
            "state_topic": "kenter/sensor/consumption/state",
            "value_template": "{{ value }}",
            "device": {
                "identifiers": ["kenter_energy_monitor"],
                "name": "Kenter Energy Monitor",
                "model": "Energy Monitor",
                "manufacturer": "Kenter"
            }
        }

        feedin_config = {
            "name": "Kenter Energy Feed-in",
            "unique_id": "kenter_energy_feedin",
            "object_id": "kenter_energy_feedin",
            "device_class": "energy",
            "state_class": "total_increasing",
            "unit_of_measurement": "kWh",
            "state_topic": "kenter/sensor/feedin/state",
            "value_template": "{{ value }}",
            "device": {
                "identifiers": ["kenter_energy_monitor"],
                "name": "Kenter Energy Monitor",
                "model": "Energy Monitor",
                "manufacturer": "Kenter"
            }
        }

        logger.info("Publishing MQTT discovery configurations...")
        
        # Publish discovery configs
        success = self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/consumption/config",
            json.dumps(consumption_config)
        )
        logger.info(f"Published consumption config: {success}")

        success = self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/feedin/config",
            json.dumps(feedin_config)
        )
        logger.info(f"Published feedin config: {success}")

        # Publish states
        logger.info("Publishing sensor states...")
        
        success = self.publish_with_retry(
            "kenter/sensor/consumption/state",
            str(data.get('consumption', 0))
        )
        logger.info(f"Published consumption state: {success}")

        success = self.publish_with_retry(
            "kenter/sensor/feedin/state",
            str(data.get('feedin', 0))
        )
        logger.info(f"Published feedin state: {success}")

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