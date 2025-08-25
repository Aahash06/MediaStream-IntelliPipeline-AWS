import csv
import random
from datetime import datetime, timedelta

# -----------------------------
# Global Lists and Mappings
# -----------------------------
CHANNEL_NAMES = [
    "Star Plus", "Colors TV", "Sony Entertainment Television", "Zee TV", "Star Bharat",
    "Sony SAB", "Dangal TV", "Zee Anmol", "Colors Rishtey", "DD National",
    "Star Sports 1", "Star Sports 2", "Star Sports 3", "Star Sports Select 1", "Star Sports Select 2",
    "Sony Sports Ten 1", "Sony Sports Ten 2", "Sony Sports Ten 3", "DD Sports", "Eurosport India",
    "Aaj Tak", "ABP News", "India TV", "News18 India", "Republic Bharat",
    "Times Now", "CNN-News18", "NDTV India", "Zee News", "India Today",
    "Sun TV", "Star Vijay", "Colors Tamil", "Zee Tamil", "KTV",
    "Star Maa", "Zee Telugu", "ETV Telugu", "Gemini TV", "Asianet",
    "Surya TV", "Zee Kannada", "Colors Kannada", "Zee Marathi", "Zee Bangla",
    "ETV Marathi", "DD Sahyadri", "DD Bangla", "News18 Kerala", "News18 Tamil Nadu"
]

GENRES = ["Entertainment", "News", "Sports", "Kids", "Music", "Movies"]
LANGUAGES = ["Hindi", "English", "Tamil", "Telugu", "Malayalam", "Kannada", "Marathi", "Bengali"]
REGIONS = ["North", "South", "East", "West", "Central", "Northeast", "Pan-India"]
DEVICES = ["Mobile", "Tablet", "Smart TV", "Laptop"]
PLATFORMS = ["Android", "iOS", "Web", "FireTV", "Roku"]

CHANNEL_ATTRIBUTES = {
    # genre, language pairs
    "Star Sports 1": ("Sports", "Hindi"), "Sony Sports Ten 1": ("Sports", "English"),
    "DD Sports": ("Sports", "Hindi"), "Star Plus": ("Entertainment", "Hindi"),
    "Sony Entertainment Television": ("Entertainment", "Hindi"), "Zee TV": ("Entertainment", "Hindi"),
    "Colors TV": ("Entertainment", "Hindi"), "SAB TV": ("Entertainment", "Hindi"),
    "Aaj Tak": ("News", "Hindi"), "NDTV India": ("News", "English"),
    "Republic Bharat": ("News", "Hindi"), "Sun TV": ("Entertainment", "Tamil"),
    "ETV Telugu": ("Entertainment", "Telugu"), "Asianet": ("Entertainment", "Malayalam"),
    "Colors Marathi": ("Entertainment", "Marathi"), "Zee Bangla": ("Entertainment", "Bengali"),
    "Times Now": ("News", "English"), "CNN-News18": ("News", "English"),
    "Zee News": ("News", "Hindi"), "ABP News": ("News", "Hindi"),
    "Star Vijay": ("Entertainment", "Tamil"), "KTV": ("Movies", "Tamil"),
}

# -----------------------------
# Dataset Generators
# -----------------------------

def generate_channel_metadata():
    print("Writing channel meta data......")
    channel_metadata = []
    channel_id_map = {}
    for i, name in enumerate(CHANNEL_NAMES):
        channel_id = f"CH{i+1:03d}"
        genre, language = CHANNEL_ATTRIBUTES.get(name, (random.choice(GENRES), random.choice(LANGUAGES)))
        launch_year = random.randint(2000, 2022)
        channel_metadata.append([channel_id, name, genre, language, launch_year])
        channel_id_map[name] = channel_id
    with open("channel_metadata.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["channel_id", "channel_name", "genre", "language", "launch_year"])
        writer.writerows(channel_metadata)
    print("Writing channel meta data completed ......")
    return channel_id_map

def generate_demographics(num_users=5000):
    demographics = []
    user_ids = set()
    for _ in range(num_users):
        user_id = f"U{10000 + random.randint(0, 49999)}"
        gender = random.choice(["Male", "Female", "Other"])
        age_group = random.choice(["<18", "18-25", "26-35", "36-45", "46-60", "60+"])
        region = random.choice(REGIONS)
        subscription_type = random.choice(["Free", "Basic", "Premium"])
        demographics.append([user_id, gender, age_group, region, subscription_type])
        user_ids.add(user_id)
    with open("demographics.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "gender", "age_group", "region", "subscription_type"])
        writer.writerows(demographics)
    return user_ids

def generate_ad_revenue(channel_id_map, days=7):
    ad_revenue = []
    for name, cid in channel_id_map.items():
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            revenue = round(random.uniform(10000, 100000), 2)
            ad_revenue.append([cid, name, date, revenue])
    with open("ad_revenue.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["channel_id", "channel_name", "date", "ad_revenue"])
        writer.writerows(ad_revenue)

def generate_viewership_logs(channel_id_map, user_ids, num_records=10000):
    logs = []
    start_time = datetime.now() - timedelta(days=7)
    user_id_list = list(user_ids)
    for _ in range(num_records):
        user_id = random.choice(user_id_list)
        channel_name = random.choice(CHANNEL_NAMES)
        channel_id = channel_id_map[channel_name]
        timestamp = start_time + timedelta(seconds=random.randint(0, 604800))
        duration = random.randint(1, 180)
        region = random.choice(REGIONS)
        subscription = random.choice(["Free", "Basic", "Premium"])
        device = random.choice(DEVICES)
        platform = random.choice(PLATFORMS)
        is_live = random.choice([True, False])
        genre = CHANNEL_ATTRIBUTES.get(channel_name, (random.choice(GENRES),))[0]
        ads_watched = random.randint(0, 5)
        ad_revenue = round(ads_watched * random.uniform(10, 200), 2)
        engagement = round(random.uniform(0.2, 1.0), 2)
        buffer_count = random.randint(0, 3)
        completion_pct = round(random.uniform(20, 100), 2)
        session_id = f"SID{random.randint(100000, 999999)}"
        show_name = f"Show_{random.randint(1, 200)}"
        logs.append([
            session_id, user_id, channel_id, channel_name, show_name, genre, timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            duration, region, subscription, device, platform, is_live, ads_watched, ad_revenue,
            engagement, buffer_count, completion_pct
        ])
    with open("viewership_logs.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "session_id", "user_id", "channel_id", "channel_name", "show_name", "genre", "timestamp",
            "duration_minutes", "region", "subscription_type", "device", "platform", "is_live",
            "ads_watched", "ad_revenue", "engagement_score", "buffer_count", "completion_percentage"
        ])
        writer.writerows(logs)

# -----------------------------
# Master Execution
# -----------------------------
if __name__ == "__main__":
    channel_id_map = generate_channel_metadata()
    user_ids = generate_demographics()
    generate_ad_revenue(channel_id_map)
    generate_viewership_logs(channel_id_map, user_ids, num_records=10000)  # Adjust as needed
