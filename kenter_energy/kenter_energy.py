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
        self.daily_consumption = 0  # Track daily total consumption
        self.daily_feedin = 0      # Track daily total feed-in
        self.last_reset_date = None  # Track when we last reset the counters
        self.setup_mqtt()
        self.cleanup_old_sensors()  # Run cleanup on startup
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
        
    def reset_daily_counters(self):
        """Reset daily counters and update last reset date"""
        self.daily_consumption = 0
        self.daily_feedin = 0
        self.last_reset_date = datetime.now().date()
        logger.info("Reset daily counters to 0")

    def fetch_kenter_data(self, timestamp):
        """Fetch data from Kenter API for the specific timestamp"""
        token = self.get_jwt_token()
        if not token:
            return None

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            # Format date components
            year = timestamp.year
            month = str(timestamp.month).zfill(2)
            day = str(timestamp.day).zfill(2)

            # Construct URL for the specific date
            url = f"{KENTER_API_URL}/meetdata/v2/measurements/connections/{KENTER_CONNECTION_ID}/metering-points/{KENTER_METERING_POINT}/days/{year}/{month}/{day}"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Find the measurement for the exact timestamp
            target_timestamp = int(timestamp.timestamp())
            consumption = 0
            feedin = 0

            for channel in data:
                channel_id = channel.get('channelId')
                
                if channel_id in ['16180', '16280']:  # Consumption or Feed-in channels
                    for measurement in channel.get('Measurements', []):
                        if (measurement.get('status') == 'Valid' and 
                            measurement.get('timestamp') == target_timestamp):
                            
                            if channel_id == '16180':  # Consumption
                                consumption = measurement.get('value', 0)
                            elif channel_id == '16280':  # Feed-in
                                feedin = measurement.get('value', 0)

            logger.info(f"Found data for {timestamp.strftime('%Y-%m-%d %H:%M')}:")
            logger.info(f"Interval values - Consumption: {consumption:.3f} kWh, Feed-in: {feedin:.3f} kWh")

            return {
                'consumption': round(consumption, 3),
                'feedin': round(feedin, 3)
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

    def publish_sensor_data(self, data):
        """Publish sensor data to Home Assistant via MQTT"""
        if not data:
            return

        # Check if we need to reset daily counters
        current_date = datetime.now().date()
        if self.last_reset_date is None or current_date > self.last_reset_date:
            self.reset_daily_counters()

        # Add new values to daily totals
        self.daily_consumption += data['consumption']
        self.daily_feedin += data['feedin']

        # Create sensor configurations
        consumption_config = {
            "name": "Kenter Daily Energy Consumption",
            "unique_id": "kenter_daily_energy_consumption",
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
            "name": "Kenter Daily Energy Production",
            "unique_id": "kenter_daily_energy_production",
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

        # Publish configurations
        self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/consumption/config",
            json.dumps(consumption_config)
        )
        self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/feedin/config",
            json.dumps(feedin_config)
        )

        # Publish states with daily totals
        self.publish_with_retry(
            "kenter/sensor/consumption/state",
            str(round(self.daily_consumption, 3))
        )
        self.publish_with_retry(
            "kenter/sensor/feedin/state",
            str(round(self.daily_feedin, 3))
        )

        logger.info(f"Daily totals - Consumption: {self.daily_consumption:.3f} kWh, Feed-in: {self.daily_feedin:.3f} kWh")

    def cleanup_old_sensors(self):
        """Remove old quarter-hourly sensors"""
        logger.info("Cleaning up old sensor configurations...")
        
        # Remove old quarter-hourly sensors
        for hour in range(24):
            for minute in [0, 15, 30, 45]:
                time_str = f"{hour:02d}_{minute:02d}"
                
                # Send empty configs to remove old sensors
                self.publish_with_retry(
                    f"homeassistant/sensor/kenter_energy_monitor/consumption_{time_str}/config",
                    ""  # Empty message removes the config
                )
                self.publish_with_retry(
                    f"homeassistant/sensor/kenter_energy_monitor/feedin_{time_str}/config",
                    ""
                )

    def run(self):
        """Main loop"""
        last_check = None
        
        while True:
            try:
                # Get current time and calculate target time (24 hours ago)
                now = datetime.now()
                target_time = now - timedelta(hours=24)
                
                # Round down to nearest 15 minutes to match Kenter's measurement intervals
                target_time = target_time.replace(
                    minute=(target_time.minute // 15) * 15,
                    second=0,
                    microsecond=0
                )
                
                # Only fetch if we haven't checked this 15-minute interval yet
                if last_check is None or target_time > last_check:
                    data = self.fetch_kenter_data(target_time)
                    if data:
                        self.publish_sensor_data(data)
                        logger.info(f"Published data for {target_time.strftime('%Y-%m-%d %H:%M')}")
                        last_check = target_time
                    else:
                        logger.error(f"No data available for {target_time.strftime('%Y-%m-%d %H:%M')}")
                
                # Sleep until next 15-minute interval
                sleep_time = 60  # Check every minute to not miss the 15-minute mark
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    monitor = KenterEnergyMonitor()
    monitor.run() 