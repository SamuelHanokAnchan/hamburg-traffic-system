from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import math

class HamburgTrafficGenerator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Real Hamburg districts with traffic hotspots
        self.hamburg_zones = {
            'Altstadt': {'center': [53.5511, 9.9937], 'traffic_density': 0.9},
            'HafenCity': {'center': [53.5407, 9.9961], 'traffic_density': 0.7},
            'St. Pauli': {'center': [53.5497, 9.9603], 'traffic_density': 0.8},
            'Altona': {'center': [53.5525, 9.9355], 'traffic_density': 0.6},
            'Wandsbek': {'center': [53.5685, 10.0714], 'traffic_density': 0.5},
            'Harburg': {'center': [53.4609, 9.9872], 'traffic_density': 0.4},
            'Bergedorf': {'center': [53.4892, 10.2314], 'traffic_density': 0.3},
            'Blankenese': {'center': [53.5644, 9.7975], 'traffic_density': 0.3},
            'Rotherbaum': {'center': [53.5676, 9.9906], 'traffic_density': 0.7},
            'Winterhude': {'center': [53.5914, 9.9981], 'traffic_density': 0.6}
        }
        
        # Major roads and highways with higher traffic
        self.major_roads = [
            {'name': 'A1_North', 'coords': [53.6200, 10.0500], 'traffic_factor': 1.5},
            {'name': 'A7_West', 'coords': [53.5500, 9.8800], 'traffic_factor': 1.4},
            {'name': 'B4_Central', 'coords': [53.5600, 10.0200], 'traffic_factor': 1.3},
            {'name': 'Ring_3', 'coords': [53.5800, 10.0800], 'traffic_factor': 1.2},
            {'name': 'Elbchaussee', 'coords': [53.5500, 9.8500], 'traffic_factor': 1.1}
        ]
    
    def get_traffic_multiplier(self):
        """Get traffic density based on time of day and day of week"""
        now = datetime.now()
        hour = now.hour
        day_of_week = now.weekday()  # 0=Monday, 6=Sunday
        
        # Weekend traffic (lighter)
        if day_of_week >= 5:  # Saturday, Sunday
            if 10 <= hour <= 20:
                return 0.6  # Moderate weekend traffic
            else:
                return 0.3  # Low weekend traffic
        
        # Weekday traffic patterns
        if 7 <= hour <= 9:  # Morning rush
            return 1.8
        elif 17 <= hour <= 19:  # Evening rush
            return 1.6
        elif 12 <= hour <= 14:  # Lunch time
            return 1.2
        elif 22 <= hour <= 6:  # Night time
            return 0.2
        else:  # Regular daytime
            return 0.8
    
    def generate_coordinates(self, zone_data):
        """Generate realistic coordinates within a Hamburg zone"""
        center_lat, center_lon = zone_data['center']
        # Generate points within ~2km radius of zone center
        radius = 0.018  # Roughly 2km in decimal degrees
        
        # Random point in circle
        angle = random.uniform(0, 2 * math.pi)
        distance = random.uniform(0, radius) * math.sqrt(random.random())
        
        lat = center_lat + distance * math.cos(angle)
        lon = center_lon + distance * math.sin(angle)
        
        return round(lat, 6), round(lon, 6)
    
    def calculate_realistic_speed(self, zone_name, zone_data, is_major_road=False):
        """Calculate realistic speed based on area and traffic conditions"""
        base_speed = 50  # km/h
        traffic_multiplier = self.get_traffic_multiplier()
        density_factor = zone_data['traffic_density']
        
        # Adjust speed based on traffic density
        if density_factor > 0.8:  # Heavy traffic zones
            speed_range = (5, 30)
        elif density_factor > 0.6:  # Moderate traffic
            speed_range = (15, 50)
        elif density_factor > 0.4:  # Light traffic
            speed_range = (25, 60)
        else:  # Very light traffic
            speed_range = (35, 70)
        
        # Major roads typically have higher speeds
        if is_major_road:
            speed_range = (max(20, speed_range[0]), min(80, speed_range[1] + 20))
        
        # Apply traffic multiplier (rush hour = lower speeds)
        if traffic_multiplier > 1.2:  # Rush hour
            speed_range = (max(5, speed_range[0] - 15), max(20, speed_range[1] - 25))
        
        return random.randint(speed_range[0], speed_range[1])
    
    def calculate_vehicle_count(self, zone_data):
        """Calculate realistic vehicle count based on traffic density and time"""
        traffic_multiplier = self.get_traffic_multiplier()
        base_count = zone_data['traffic_density'] * 15  # Base vehicles per observation
        
        # Apply time-based multiplier
        actual_count = base_count * traffic_multiplier
        
        # Add some randomness
        variance = random.uniform(0.7, 1.3)
        final_count = max(1, int(actual_count * variance))
        
        return min(final_count, 50)  # Cap at 50 vehicles per point
    
    def generate_traffic_data(self):
        """Generate a single realistic traffic data point"""
        # 70% chance for regular zones, 30% for major roads
        if random.random() < 0.7:
            # Pick a random Hamburg zone
            zone_name = random.choice(list(self.hamburg_zones.keys()))
            zone_data = self.hamburg_zones[zone_name]
            lat, lon = self.generate_coordinates(zone_data)
            speed = self.calculate_realistic_speed(zone_name, zone_data)
            is_major = False
        else:
            # Pick a major road
            road = random.choice(self.major_roads)
            # Add some variance to major road coordinates
            lat = road['coords'][0] + random.uniform(-0.01, 0.01)
            lon = road['coords'][1] + random.uniform(-0.01, 0.01)
            # Use average density for major roads
            avg_zone_data = {'traffic_density': 0.6}
            speed = self.calculate_realistic_speed('major_road', avg_zone_data, is_major_road=True)
            zone_name = road['name']
            zone_data = avg_zone_data
            is_major = True
        
        vehicle_count = self.calculate_vehicle_count(zone_data)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'location': {
                'lat': lat,
                'lon': lon
            },
            'speed': speed,
            'vehicle_count': vehicle_count,
            'zone': zone_name,
            'is_major_road': is_major,
            'traffic_density': zone_data['traffic_density']
        }
    
    def start_streaming(self, interval=2):
        """Start streaming traffic data to Kafka"""
        print("🚦 Starting Hamburg Traffic Generator...")
        print(f"📊 Generating data every {interval} seconds")
        print(f"🕐 Current traffic multiplier: {self.get_traffic_multiplier():.1f}x")
        
        try:
            while True:
                data = self.generate_traffic_data()
                self.producer.send('hamburg_traffic', value=data)
                
                # Enhanced logging
                zone = data['zone']
                speed = data['speed']
                vehicles = data['vehicle_count']
                coords = f"[{data['location']['lat']:.4f}, {data['location']['lon']:.4f}]"
                
                print(f"📍 {zone}: {vehicles} vehicles @ {speed}km/h {coords}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🛑 Traffic generator stopped.")
        finally:
            self.producer.close()

if __name__ == "__main__":
    generator = HamburgTrafficGenerator()
    generator.start_streaming(interval=1)  # Generate data every 1 second