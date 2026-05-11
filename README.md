# SafeRoute — Pipeline de Collecte de Données
## De l'API à PostgreSQL

---

## Architecture

```
APIs externes                  Pipeline Python              PostgreSQL
─────────────                  ──────────────              ──────────
OpenStreetMap (OSM) ──────►  OSMCollector       ──────►  road_segments
TomTom Traffic      ──────►  TomTomCollector    ──────►  traffic_snapshots
OpenWeatherMap      ──────►  WeatherCollector   ──────►  weather_snapshots
App Flutter         ──────►  (via FastAPI)      ──────►  incidents
Modèle ML           ──────►  RiskCalculator     ──────►  risk_scores
```

---

## Installation

### 1. PostgreSQL + PostGIS

```bash
# Ubuntu / Debian
sudo apt install postgresql postgresql-contrib postgis
```

Sur Windows, installe PostGIS avec **Stack Builder** :

1. Ouvre **Stack Builder** depuis le menu Demarrer.
2. Choisis ton installation **PostgreSQL 17**.
3. Dans **Spatial Extensions**, installe **PostGIS**.
4. Relance ensuite `python database.py`.

```bash
# Créer la base de données
sudo -u postgres psql
CREATE DATABASE saferoute;
CREATE USER saferoute_user WITH PASSWORD 'ton_mot_de_passe';
GRANT ALL PRIVILEGES ON DATABASE saferoute TO saferoute_user;
\c saferoute
CREATE EXTENSION postgis;
\q
```

### 2. Python

```bash
pip install requests psycopg2-binary sqlalchemy geopandas \
            shapely python-dotenv schedule
```

### 3. Variables d'environnement

Crée un fichier `.env` dans ce dossier :

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=saferoute
DB_USER=saferoute_user
DB_PASSWORD=ton_mot_de_passe

# APIs
TOMTOM_API_KEY=ta_cle_tomtom      # developer.tomtom.com (gratuit)
OPENWEATHER_API_KEY=ta_cle_ow     # openweathermap.org (gratuit)
```

---

## Clés API gratuites

| API | Lien | Quota gratuit |
|-----|------|--------------|
| TomTom Traffic | https://developer.tomtom.com | 2 500 req/jour |
| OpenWeatherMap | https://openweathermap.org/api | 1 000 req/jour |
| OpenStreetMap  | Gratuit, sans clé | Illimité |

---

## Démarrage

### Étape 1 — Créer les tables (une seule fois)

```bash
python database.py
```

### Étape 2 — Collecter les routes OSM (une seule fois)

```bash
python collectors.py osm
```
→ Remplit `road_segments` avec tous les segments de Casablanca

### Étape 3 — Lancer le scheduler (en continu)

```bash
python scheduler.py
```
→ Trafic toutes les 5 min + météo toutes les 30 min

---

## Requêtes utiles PostgreSQL

```sql
-- Nombre de segments par type
SELECT road_type, COUNT(*) FROM road_segments GROUP BY road_type;

-- Segments les plus congestionnés maintenant
SELECT s.name, t.current_speed, t.speed_ratio
FROM traffic_snapshots t
JOIN road_segments s ON s.id = t.segment_id
WHERE t.collected_at > NOW() - INTERVAL '10 minutes'
ORDER BY t.speed_ratio ASC
LIMIT 10;

-- Score de risque actuel (via la vue)
SELECT name, road_type, risk_score, risk_level
FROM current_risk
ORDER BY risk_score DESC
LIMIT 20;

-- Historique météo
SELECT collected_at, weather_main, temperature, risk_modifier
FROM weather_snapshots
ORDER BY collected_at DESC
LIMIT 24;

-- Segments autour d'un point GPS (PostGIS)
SELECT name, road_type, ST_Distance(geom::geography,
    ST_GeomFromText('POINT(-7.5898 33.5731)', 4326)::geography) AS dist_m
FROM road_segments
WHERE ST_DWithin(
    geom::geography,
    ST_GeomFromText('POINT(-7.5898 33.5731)', 4326)::geography,
    1000   -- 1 km de rayon
)
ORDER BY dist_m;
```

---

## Structure des fichiers

```
saferoute_data_pipeline/
├── config.py       # Configuration et clés API
├── database.py     # Création des tables PostgreSQL
├── collectors.py   # Collecteurs OSM + TomTom + OpenWeather
├── scheduler.py    # Scheduler automatique
├── .env            # Variables d'environnement (ne pas committer)
└── README.md       # Ce fichier
```

---

## Prochaine étape : FastAPI + Modèle ML

Une fois les données collectées, connecte FastAPI :

```python
# main.py FastAPI
@app.get("/risk/segments")
async def get_risk_segments(lat: float, lng: float):
    # 1. Requête PostGIS → segments dans rayon 5km
    # 2. Récupère derniers snapshots trafic + météo
    # 3. Appelle le modèle ML → score de risque
    # 4. Retourne JSON → Flutter l'affiche sur la carte
```
