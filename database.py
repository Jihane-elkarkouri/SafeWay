"""
database.py — Création des tables PostgreSQL pour SafeRoute
===========================================================
Tables :
  - road_segments     : segments routiers OSM (+ travaux + événements)
  - traffic_snapshots : historique trafic TomTom (toutes les 5 min)
  - weather_snapshots : historique météo OpenWeather
  - incidents         : signalements utilisateurs
  - risk_scores       : scores calculés par le modèle ML

Prérequis :
    pip install psycopg2-binary sqlalchemy
    -- Dans PostgreSQL : CREATE EXTENSION postgis;
"""

import psycopg2
from psycopg2 import sql
from config import DATABASE

# ── Connexion ──────────────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=DATABASE["host"],
        port=DATABASE["port"],
        dbname=DATABASE["name"],
        user=DATABASE["user"],
        password=DATABASE["password"],
    )


def ensure_database_exists():
    """Cree la base cible si elle n'existe pas encore."""
    db_name = DATABASE["name"]
    if db_name == "postgres":
        return

    conn = psycopg2.connect(
        host=DATABASE["host"],
        port=DATABASE["port"],
        dbname="postgres",
        user=DATABASE["user"],
        password=DATABASE["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
                )
                print(f"[OK] Base de donnees creee : {db_name}")
    finally:
        conn.close()


def ensure_postgis_available():
    """Verifie que PostGIS est installe sur le serveur PostgreSQL."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = %s)",
                ("postgis",),
            )
            is_available = cur.fetchone()[0]
    finally:
        conn.close()

    if not is_available:
        raise SystemExit(
            "[ERREUR] PostGIS n'est pas installe sur PostgreSQL.\n"
            "Installe PostGIS pour PostgreSQL 17 avec Stack Builder, puis relance :\n"
            "  python database.py\n"
            "Le projet utilise PostGIS pour les colonnes GEOMETRY et les requetes spatiales."
        )


# ── Création des tables ────────────────────────────────────────────────────────
CREATE_TABLES_SQL = """

-- Extension géographique (à activer une seule fois)
CREATE EXTENSION IF NOT EXISTS postgis;

-- ─── 1. Segments routiers (base OSM) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS road_segments (
    id              SERIAL PRIMARY KEY,
    osm_id          BIGINT UNIQUE,                  -- ID OpenStreetMap
    name            TEXT,                           -- Nom de la rue
    road_type       VARCHAR(30),                    -- primary, secondary, residential...
    start_lat       DOUBLE PRECISION NOT NULL,
    start_lng       DOUBLE PRECISION NOT NULL,
    end_lat         DOUBLE PRECISION NOT NULL,
    end_lng         DOUBLE PRECISION NOT NULL,
    length_m        DOUBLE PRECISION,               -- longueur en mètres
    speed_limit     INTEGER DEFAULT 50,             -- km/h
    has_school      BOOLEAN DEFAULT FALSE,          -- zone sensible
    has_hospital    BOOLEAN DEFAULT FALSE,
    has_market      BOOLEAN DEFAULT FALSE,
    accident_count  INTEGER DEFAULT 0,              -- accidents historiques
    has_construction BOOLEAN DEFAULT FALSE,          -- travaux actifs sur ce segment
    has_event_nearby BOOLEAN DEFAULT FALSE,          -- événement actif à proximité
    event_type       VARCHAR(50),                    -- match, aid, ramadan, market...
    -- Géométrie et caractéristiques physiques (OSM enrichi)
    lanes           SMALLINT DEFAULT 1,              -- nombre de voies
    is_lit          BOOLEAN DEFAULT NULL,            -- éclairage public (NULL=inconnu)
    surface         VARCHAR(30) DEFAULT 'asphalt',   -- asphalt, unpaved, concrete...
    is_oneway       BOOLEAN DEFAULT FALSE,           -- sens unique
    curvature       DOUBLE PRECISION DEFAULT 0.0,    -- déviation angulaire moyenne (0=droit, >30=virage)
    geom            GEOMETRY(LineString, 4326),     -- géométrie PostGIS
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Index spatial pour les requêtes de proximité
CREATE INDEX IF NOT EXISTS idx_road_segments_geom
    ON road_segments USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_road_segments_type
    ON road_segments(road_type);

-- Contrainte unique sur les coordonnées pour ON CONFLICT (évite doublons à la recollecte OSM)
CREATE UNIQUE INDEX IF NOT EXISTS idx_road_segments_coords
    ON road_segments(start_lat, start_lng, end_lat, end_lng);


-- ─── 2. Snapshots trafic TomTom (toutes les 5 min) ────────────────────────────
CREATE TABLE IF NOT EXISTS traffic_snapshots (
    id                  SERIAL PRIMARY KEY,
    segment_id          INTEGER REFERENCES road_segments(id) ON DELETE CASCADE,
    collected_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    current_speed       DOUBLE PRECISION,    -- km/h vitesse actuelle
    free_flow_speed     DOUBLE PRECISION,    -- km/h vitesse fluide
    speed_ratio         DOUBLE PRECISION,    -- current/free_flow (1.0 = fluide)
    confidence          DOUBLE PRECISION,    -- fiabilité TomTom (0-1)
    road_closure        BOOLEAN DEFAULT FALSE,
    hour_of_day         SMALLINT,           -- 0-23
    day_of_week         SMALLINT,           -- 0=lundi, 6=dimanche
    is_peak_hour        BOOLEAN             -- heure de pointe
);

-- Index pour les requêtes temporelles
CREATE INDEX IF NOT EXISTS idx_traffic_collected_at
    ON traffic_snapshots(collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_traffic_segment_time
    ON traffic_snapshots(segment_id, collected_at DESC);


-- ─── 3. Snapshots météo OpenWeather ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_snapshots (
    id              SERIAL PRIMARY KEY,
    collected_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    temperature     DOUBLE PRECISION,    -- °C
    weather_main    VARCHAR(50),         -- Rain, Clear, Clouds, Fog...
    weather_desc    TEXT,
    wind_speed      DOUBLE PRECISION,    -- m/s
    humidity        INTEGER,             -- %
    visibility      INTEGER,             -- mètres
    rain_1h         DOUBLE PRECISION DEFAULT 0,   -- mm pluie dernière heure
    is_rain         BOOLEAN DEFAULT FALSE,
    is_fog          BOOLEAN DEFAULT FALSE,
    risk_modifier   DOUBLE PRECISION DEFAULT 1.0  -- multiplicateur de risque météo
);

CREATE INDEX IF NOT EXISTS idx_weather_collected_at
    ON weather_snapshots(collected_at DESC);


-- ─── 4. Incidents signalés par les utilisateurs ────────────────────────────────
CREATE TABLE IF NOT EXISTS incidents (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER,                        -- utilisateur qui a signalé
    incident_type   VARCHAR(30) NOT NULL,           -- accident, traffic, police...
    severity        VARCHAR(10) DEFAULT 'moderate', -- low, moderate, high
    lat             DOUBLE PRECISION NOT NULL,
    lng             DOUBLE PRECISION NOT NULL,
    segment_id      INTEGER REFERENCES road_segments(id),
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    reported_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMP,                      -- auto-expiration après 2h
    confirmed_count INTEGER DEFAULT 1,              -- votes de confirmation
    geom            GEOMETRY(Point, 4326)
);

CREATE INDEX IF NOT EXISTS idx_incidents_geom
    ON incidents USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_incidents_active
    ON incidents(is_active, reported_at DESC);


-- ─── 5. Scores de risque calculés par le modèle ML ────────────────────────────
CREATE TABLE IF NOT EXISTS risk_scores (
    id              SERIAL PRIMARY KEY,
    segment_id      INTEGER REFERENCES road_segments(id) ON DELETE CASCADE,
    calculated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    hour_of_day     SMALLINT,
    day_of_week     SMALLINT,
    risk_score      DOUBLE PRECISION NOT NULL,    -- 0-100
    risk_level      VARCHAR(10) NOT NULL,         -- low/moderate/high/critical
    -- Features utilisées par le modèle ML
    feat_speed_ratio        DOUBLE PRECISION,
    feat_accident_count     INTEGER,
    feat_has_sensitive_zone BOOLEAN,
    feat_weather_modifier   DOUBLE PRECISION,
    feat_incident_nearby    BOOLEAN,
    model_version   VARCHAR(20) DEFAULT 'v1.0'
);

CREATE INDEX IF NOT EXISTS idx_risk_segment_time
    ON risk_scores(segment_id, calculated_at DESC);



-- ─── 6. Archive horaire — dataset ML dénormalisé (autosuffisant) ──────────────
-- Chaque ligne = 1 snapshot complet d'un segment à un instant donné.
-- Toutes les features sont copiées ici → aucun JOIN nécessaire à l'entraînement.
CREATE TABLE IF NOT EXISTS traffic_archive (
    id               SERIAL PRIMARY KEY,
    segment_id       INTEGER REFERENCES road_segments(id) ON DELETE CASCADE,
    archived_at      TIMESTAMP NOT NULL,   -- heure d'archivage (arrondie à l'heure)
    collected_at     TIMESTAMP NOT NULL,   -- heure de collecte TomTom originale

    -- ── Features trafic (TomTom) ──────────────────────────────────────────────
    current_speed    DOUBLE PRECISION,     -- km/h vitesse mesurée
    free_flow_speed  DOUBLE PRECISION,     -- km/h vitesse fluide de référence
    speed_ratio      DOUBLE PRECISION,     -- current/free_flow  [0..1]  ← target ML
    confidence       DOUBLE PRECISION,     -- fiabilité TomTom   [0..1]
    road_closure     BOOLEAN DEFAULT FALSE,

    -- ── Contexte temporel ─────────────────────────────────────────────────────
    hour_of_day      SMALLINT,             -- 0-23
    day_of_week      SMALLINT,             -- 0=lundi … 6=dimanche
    is_peak_hour     BOOLEAN,              -- heure de pointe

    -- ── Features météo (snapshot le plus récent au moment de l'archivage) ─────
    weather_main     VARCHAR(50),          -- Clear, Rain, Fog, Thunderstorm…
    temperature      DOUBLE PRECISION,     -- °C
    wind_speed       DOUBLE PRECISION,     -- m/s
    humidity         INTEGER,              -- %
    visibility       INTEGER,              -- mètres
    rain_1h          DOUBLE PRECISION DEFAULT 0,
    is_rain          BOOLEAN DEFAULT FALSE,
    is_fog           BOOLEAN DEFAULT FALSE,
    weather_modifier DOUBLE PRECISION DEFAULT 1.0,  -- multiplicateur de risque météo

    -- ── Features statiques OSM (copiées depuis road_segments) ─────────────────
    road_type        VARCHAR(30),          -- primary, secondary, residential…
    length_m         DOUBLE PRECISION,     -- longueur du segment en mètres
    speed_limit      INTEGER,              -- km/h limite légale
    lanes            SMALLINT,             -- nombre de voies
    is_lit           BOOLEAN,              -- éclairage public (NULL=inconnu)
    surface          VARCHAR(30),          -- asphalt, unpaved, concrete…
    is_oneway        BOOLEAN DEFAULT FALSE,
    curvature        DOUBLE PRECISION DEFAULT 0.0,  -- 0=droit, >30=virage notable

    -- ── Zones sensibles (OSM) ─────────────────────────────────────────────────
    has_school       BOOLEAN DEFAULT FALSE,
    has_hospital     BOOLEAN DEFAULT FALSE,
    has_market       BOOLEAN DEFAULT FALSE,

    -- ── Contexte dynamique au moment de l'archivage ───────────────────────────
    has_construction  BOOLEAN DEFAULT FALSE,
    has_event_nearby  BOOLEAN DEFAULT FALSE,
    event_type        VARCHAR(50),          -- match, aid, ramadan, market…

    -- ── Incidents TomTom actifs à proximité (<500 m) ──────────────────────────
    has_incident_nearby  BOOLEAN DEFAULT FALSE,
    incident_type        VARCHAR(30),       -- ACCIDENT, JAM, ROAD_WORK…
    incident_severity    SMALLINT,          -- 1=inconnu 2=mineur 3=modéré 4=majeur

    -- ── Incidents utilisateurs actifs à proximité ─────────────────────────────
    has_user_incident    BOOLEAN DEFAULT FALSE,
    user_incident_type   VARCHAR(30)        -- accident, traffic, police…
);

-- Index ML
CREATE INDEX IF NOT EXISTS idx_archive_segment
    ON traffic_archive(segment_id);

CREATE INDEX IF NOT EXISTS idx_archive_archived_at
    ON traffic_archive(archived_at DESC);

CREATE INDEX IF NOT EXISTS idx_archive_hour_dow
    ON traffic_archive(hour_of_day, day_of_week);

CREATE INDEX IF NOT EXISTS idx_archive_segment_hour
    ON traffic_archive(segment_id, hour_of_day, day_of_week);

CREATE INDEX IF NOT EXISTS idx_archive_road_type
    ON traffic_archive(road_type);

CREATE INDEX IF NOT EXISTS idx_archive_event
    ON traffic_archive(has_event_nearby, event_type);

CREATE INDEX IF NOT EXISTS idx_archive_construction
    ON traffic_archive(has_construction);

CREATE INDEX IF NOT EXISTS idx_archive_incident
    ON traffic_archive(has_incident_nearby, incident_type);


-- ─── 7. Matchs planifiés (API-Football) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS scheduled_matches (
    id              SERIAL PRIMARY KEY,
    fixture_id      INTEGER UNIQUE NOT NULL,   -- ID unique API-Football
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    match_date      TIMESTAMP NOT NULL,        -- date/heure du match
    venue_lat       DOUBLE PRECISION,          -- coordonnées du stade
    venue_lng       DOUBLE PRECISION,
    radius_km       DOUBLE PRECISION DEFAULT 3.0,
    status          VARCHAR(20) DEFAULT 'scheduled', -- scheduled/live/finished
    fetched_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matches_date
    ON scheduled_matches(match_date);

CREATE INDEX IF NOT EXISTS idx_matches_status
    ON scheduled_matches(status, match_date);

-- ─── 8. Incidents TomTom temps réel ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tomtom_incidents (
    id              SERIAL PRIMARY KEY,
    incident_id     TEXT UNIQUE NOT NULL,          -- ID unique TomTom
    incident_type   VARCHAR(30) NOT NULL,          -- ACCIDENT, JAM, ROAD_WORK, LANE_CLOSED, DISABLED_VEHICLE, OTHER
    severity        SMALLINT,                      -- 1=inconnu, 2=mineur, 3=modéré, 4=majeur
    description     TEXT,                          -- description textuelle TomTom
    lat             DOUBLE PRECISION NOT NULL,     -- latitude du point
    lng             DOUBLE PRECISION NOT NULL,     -- longitude du point
    from_point      TEXT,                          -- description du début
    to_point        TEXT,                          -- description de la fin
    road_name       TEXT,                          -- nom de la route
    delay_seconds   INTEGER DEFAULT 0,             -- délai estimé en secondes
    length_m        DOUBLE PRECISION DEFAULT 0,    -- longueur de l'incident en mètres
    is_active       BOOLEAN DEFAULT TRUE,
    first_seen_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    geom            GEOMETRY(Point, 4326)
);

CREATE INDEX IF NOT EXISTS idx_tomtom_incidents_geom
    ON tomtom_incidents USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_tomtom_incidents_active
    ON tomtom_incidents(is_active, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_tomtom_incidents_type
    ON tomtom_incidents(incident_type, is_active);



-- ─── Vue LIVE : toutes les features en temps réel (même structure que traffic_archive) ──
-- Utilisée par l'API FastAPI pour le calcul d'itinéraire et le dashboard temps réel.
-- Même colonnes que traffic_archive → le modèle ML s'entraîne sur archive, prédit sur live_segments.
CREATE OR REPLACE VIEW live_segments AS
SELECT
    ts.segment_id,
    ts.collected_at,

    -- ── Trafic TomTom ──────────────────────────────────────────────────────
    ts.current_speed,
    ts.free_flow_speed,
    ts.speed_ratio,
    ts.confidence,
    ts.road_closure,
    ts.hour_of_day,
    ts.day_of_week,
    ts.is_peak_hour,

    -- ── OSM statique ───────────────────────────────────────────────────────
    rs.name,
    rs.road_type,
    rs.length_m,
    rs.speed_limit,
    rs.lanes,
    rs.is_lit,
    rs.surface,
    rs.is_oneway,
    rs.curvature,
    rs.start_lat,
    rs.start_lng,
    rs.end_lat,
    rs.end_lng,
    rs.geom,

    -- ── Zones sensibles ────────────────────────────────────────────────────
    rs.has_school,
    rs.has_hospital,
    rs.has_market,

    -- ── Contexte dynamique ─────────────────────────────────────────────────
    rs.has_construction,
    rs.has_event_nearby,
    rs.event_type,

    -- ── Météo (dernier snapshot disponible) ───────────────────────────────
    w.weather_main,
    w.temperature,
    w.wind_speed,
    w.humidity,
    w.visibility,
    w.rain_1h,
    w.is_rain,
    w.is_fog,
    w.risk_modifier      AS weather_modifier,

    -- ── Incidents TomTom actifs dans un rayon de 500m ─────────────────────
    EXISTS (
        SELECT 1 FROM tomtom_incidents ti
        WHERE ti.is_active = TRUE
        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
    )                    AS has_incident_nearby,
    (
        SELECT ti.incident_type FROM tomtom_incidents ti
        WHERE ti.is_active = TRUE
        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
        ORDER BY ti.severity DESC LIMIT 1
    )                    AS incident_type,
    (
        SELECT ti.severity FROM tomtom_incidents ti
        WHERE ti.is_active = TRUE
        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
        ORDER BY ti.severity DESC LIMIT 1
    )                    AS incident_severity,

    -- ── Incidents utilisateurs actifs dans un rayon de 300m ───────────────
    EXISTS (
        SELECT 1 FROM incidents ui
        WHERE ui.is_active = TRUE
        AND ui.geom IS NOT NULL
        AND ST_DWithin(ui.geom::geography, rs.geom::geography, 300)
    )                    AS has_user_incident,
    (
        SELECT ui.incident_type FROM incidents ui
        WHERE ui.is_active = TRUE
        AND ui.geom IS NOT NULL
        AND ST_DWithin(ui.geom::geography, rs.geom::geography, 300)
        ORDER BY ui.reported_at DESC LIMIT 1
    )                    AS user_incident_type

FROM traffic_snapshots ts
JOIN road_segments rs ON rs.id = ts.segment_id
LEFT JOIN LATERAL (
    SELECT weather_main, temperature, wind_speed, humidity,
           visibility, rain_1h, is_rain, is_fog, risk_modifier
    FROM weather_snapshots
    ORDER BY collected_at DESC
    LIMIT 1
) w ON TRUE
-- Seulement le dernier snapshot de chaque segment
WHERE ts.collected_at = (
    SELECT MAX(collected_at) FROM traffic_snapshots
);

-- ─── Vue utile : score de risque actuel par segment ───────────────────────────
CREATE OR REPLACE VIEW current_risk AS
SELECT DISTINCT ON (rs.segment_id)
    rs.segment_id,
    seg.name,
    seg.road_type,
    seg.start_lat,
    seg.start_lng,
    seg.end_lat,
    seg.end_lng,
    rs.risk_score,
    rs.risk_level,
    rs.calculated_at,
    ts.current_speed,
    ts.speed_ratio
FROM risk_scores rs
JOIN road_segments seg ON seg.id = rs.segment_id
LEFT JOIN LATERAL (
    SELECT current_speed, speed_ratio
    FROM traffic_snapshots
    WHERE segment_id = rs.segment_id
    ORDER BY collected_at DESC
    LIMIT 1
) ts ON TRUE
ORDER BY rs.segment_id, rs.calculated_at DESC;
"""


def create_tables():
    """Crée toutes les tables dans PostgreSQL."""
    print("[INFO] Creation des tables SafeRoute...")
    ensure_database_exists()
    ensure_postgis_available()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        print("[OK] Tables creees avec succes :")
        print("   - road_segments")
        print("   - traffic_snapshots")
        print("   - weather_snapshots")
        print("   - incidents")
        print("   - risk_scores")
        print("   - traffic_archive")
        print("   - scheduled_matches")
        print("   - tomtom_incidents")
        print("   - Vue: current_risk")
        print("   - Vue: live_segments  (toutes les features temps réel)")
    except Exception as e:
        conn.rollback()
        print(f"[ERREUR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    create_tables()
