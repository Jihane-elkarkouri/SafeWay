# config.py — Configuration centrale SafeRoute Data Pipeline
# Remplace les valeurs par tes vraies clés API

import os
from dotenv import load_dotenv

load_dotenv()

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
DATABASE = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "name": os.getenv("DB_NAME", "SafeWay"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgresql"),
}

DATABASE_URL = (
    f"postgresql://{DATABASE['user']}:{DATABASE['password']}"
    f"@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['name']}"
)

# ── APIs ────────────────────────────────────────────────────────────────────────
TOMTOM_API_KEY      = os.getenv("TOMTOM_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not TOMTOM_API_KEY:
    raise ValueError("TOMTOM_API_KEY manquant dans le .env")
if not OPENWEATHER_API_KEY:
    raise ValueError("OPENWEATHER_API_KEY manquant dans le .env")

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")
if not API_FOOTBALL_KEY:
    import warnings
    warnings.warn("API_FOOTBALL_KEY absent — FootballCollector désactivé (optionnel)")

TOMTOM_INCIDENTS_KEY = os.getenv("TOMTOM_INCIDENTS_KEY")
if not TOMTOM_INCIDENTS_KEY:
    raise ValueError("TOMTOM_INCIDENTS_KEY manquant dans le .env")

# Botola Pro league ID sur API-Football
BOTOLA_LEAGUE_ID = 200
BOTOLA_SEASON    = 2025

# Équipes Casablanca à surveiller (domicile = impact trafic)
CASA_TEAM_IDS = {
    "Raja CA": 1541,
    "WAC":     1542,
}

# ── Zone Casablanca ─────────────────────────────────────────────────────────────
CASABLANCA = {
    "lat":      33.5731,
    "lng":     -7.5898,
    "min_lat":  33.48,
    "max_lat":  33.65,
    "min_lng": -7.70,
    "max_lng": -7.48,
    "radius_m": 15000,   # rayon collecte trafic (15 km)
}

# ── Intervalles de collecte ─────────────────────────────────────────────────────
COLLECT_INTERVAL_MINUTES  = 11   # trafic TomTom — 18 appels × 130 cycles = 2340/jour (quota 2500 ✅)
ARCHIVE_INTERVAL_MINUTES   = 60   # archivage horaire → traffic_archive
WEATHER_INTERVAL_MINUTES   = 30   # météo OpenWeather
