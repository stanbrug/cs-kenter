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
        """Fetch data from Kenter API for a specific date and process quarter-hourly values"""
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

            # Initialize quarter-hourly data structure
            # Each day has 24 hours * 4 quarters = 96 quarters
            quarter_data = {}
            for hour in range(24):
                for quarter in range(4):
                    quarter_key = f"{hour:02d}:{quarter*15:02d}"
                    quarter_data[quarter_key] = {
                        'consumption': 0,
                        'feedin': 0,
                        'timestamp': None
                    }

            # Process the measurements data
            for channel in data:
                channel_id = channel.get('channelId')
                
                if channel_id in ['16180', '16280']:  # Consumption or Feed-in channels
                    for measurement in channel.get('Measurements', []):
                        if measurement.get('status') == 'Valid':
                            timestamp = measurement.get('timestamp')
                            value = measurement.get('value', 0)
                            
                            # Convert timestamp to hour and quarter
                            dt = datetime.fromtimestamp(timestamp)
                            quarter_key = f"{dt.hour:02d}:{dt.minute:02d}"
                            
                            if quarter_key in quarter_data:
                                # Add to appropriate counter
                                if channel_id == '16180':  # Consumption
                                    quarter_data[quarter_key]['consumption'] = value
                                elif channel_id == '16280':  # Feed-in
                                    quarter_data[quarter_key]['feedin'] = value
                                quarter_data[quarter_key]['timestamp'] = timestamp

            # Log quarter-hourly totals
            logger.info(f"Processed quarter-hourly data for {year}-{month}-{day}:")
            for quarter_key, values in quarter_data.items():
                if values['timestamp']:  # Only log if we have data for this quarter
                    logger.info(f"Time {quarter_key}: Consumption: {values['consumption']:.3f} kWh, Feed-in: {values['feedin']:.3f} kWh")

            return quarter_data

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

        # Create base sensor configurations
        base_config = {
            "device_class": "energy",
            "state_class": "total",
            "unit_of_measurement": "kWh",
            "device": {
                "identifiers": ["kenter_energy_monitor"],
                "name": "Kenter Energy Monitor",
                "model": "Energy Monitor",
                "manufacturer": "Kenter"
            }
        }

        # Publish quarter-hourly sensors
        for quarter_key, values in data.items():
            if not values['timestamp']:  # Skip if no data for this quarter
                continue

            # Format quarter key for MQTT topics (replace : with _)
            mqtt_quarter_key = quarter_key.replace(':', '_')
            
            # Consumption sensor for this quarter
            consumption_config = base_config.copy()
            consumption_config.update({
                "name": f"Kenter Energy Consumption {quarter_key}",
                "unique_id": f"kenter_energy_consumption_{mqtt_quarter_key}",
                "state_topic": f"kenter/sensor/consumption/{mqtt_quarter_key}/state",
                "value_template": "{{ value }}",
            })

            # Feed-in sensor for this quarter
            feedin_config = base_config.copy()
            feedin_config.update({
                "name": f"Kenter Energy Feed-in {quarter_key}",
                "unique_id": f"kenter_energy_feedin_{mqtt_quarter_key}",
                "state_topic": f"kenter/sensor/feedin/{mqtt_quarter_key}/state",
                "value_template": "{{ value }}",
            })

            # Publish configurations
            self.publish_with_retry(
                f"homeassistant/sensor/kenter_energy_monitor/consumption_{mqtt_quarter_key}/config",
                json.dumps(consumption_config)
            )
            self.publish_with_retry(
                f"homeassistant/sensor/kenter_energy_monitor/feedin_{mqtt_quarter_key}/config",
                json.dumps(feedin_config)
            )

            # Publish states
            self.publish_with_retry(
                f"kenter/sensor/consumption/{mqtt_quarter_key}/state",
                str(round(values['consumption'], 3))
            )
            self.publish_with_retry(
                f"kenter/sensor/feedin/{mqtt_quarter_key}/state",
                str(round(values['feedin'], 3))
            )

        # Calculate and publish daily totals
        daily_consumption = sum(values['consumption'] for values in data.values() if values['timestamp'])
        daily_feedin = sum(values['feedin'] for values in data.values() if values['timestamp'])

        daily_consumption_config = base_config.copy()
        daily_consumption_config.update({
            "name": "Kenter Energy Consumption Daily",
            "unique_id": "kenter_energy_consumption_daily",
            "state_topic": "kenter/sensor/consumption/daily/state",
            "value_template": "{{ value }}",
        })

        daily_feedin_config = base_config.copy()
        daily_feedin_config.update({
            "name": "Kenter Energy Feed-in Daily",
            "unique_id": "kenter_energy_feedin_daily",
            "state_topic": "kenter/sensor/feedin/daily/state",
            "value_template": "{{ value }}",
        })

        # Publish daily configurations and states
        self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/consumption_daily/config",
            json.dumps(daily_consumption_config)
        )
        self.publish_with_retry(
            "homeassistant/sensor/kenter_energy_monitor/feedin_daily/config",
            json.dumps(daily_feedin_config)
        )
        self.publish_with_retry(
            "kenter/sensor/consumption/daily/state",
            str(round(daily_consumption, 3))
        )
        self.publish_with_retry(
            "kenter/sensor/feedin/daily/state",
            str(round(daily_feedin, 3))
        )

        logger.info(f"Published data for {date_str}")
        logger.info(f"Daily totals - Consumption: {daily_consumption:.3f} kWh, Feed-in: {daily_feedin:.3f} kWh")

    def run(self):
        """Main loop"""
        while True:
            try:
                # Calculate exactly 24 hours ago
                target_time = datetime.now() - timedelta(hours=24)
                
                # Fetch and publish data
                data = self.fetch_kenter_data(target_time)
                if data:
                    self.publish_sensor_data(data, target_time)
                    logger.info(f"Published data for {target_time.strftime('%Y-%m-%d %H:%M')}")
                else:
                    logger.error(f"No data available for {target_time.strftime('%Y-%m-%d %H:%M')}")

                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    monitor = KenterEnergyMonitor()
    monitor.run() 