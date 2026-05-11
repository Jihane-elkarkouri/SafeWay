"""
scheduler.py — Lance la collecte automatique en boucle
=======================================================
Trafic   : toutes les 10 minutes (TomTom)   → traffic_snapshots (LIVE)
Météo    : toutes les 30 minutes (OpenWeather)
Archive  : toutes les heures                → traffic_archive (HISTORIQUE ML)

Usage :
    python scheduler.py
"""

import time
import logging
import schedule
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch

from collectors import TomTomCollector, WeatherCollector, ConstructionCollector, EventCollector, FootballCollector, TomTomIncidentCollector
from config import COLLECT_INTERVAL_MINUTES, WEATHER_INTERVAL_MINUTES, ARCHIVE_INTERVAL_MINUTES
from database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

traffic_collector      = TomTomCollector()
weather_collector      = WeatherCollector()
construction_collector = ConstructionCollector()
event_collector        = EventCollector()
football_collector     = FootballCollector()
incident_collector     = TomTomIncidentCollector()


def collect_traffic():
    log.info("── Collecte trafic TomTom ──")
    traffic_collector.run()


def collect_weather():
    log.info("── Collecte météo OpenWeather ──")
    weather_collector.run()


def collect_construction():
    log.info("── Collecte travaux OSM ──")
    construction_collector.run()


def collect_events():
    log.info("── Détection événements ──")
    event_collector.run()


def collect_football():
    log.info("── Collecte matchs API-Football ──")
    football_collector.run()


def collect_incidents():
    log.info("── Collecte incidents TomTom ──")
    incident_collector.run()


def archive_traffic():
    """
    Copie traffic_snapshots → traffic_archive (dataset ML dénormalisé) toutes les heures.
    Chaque ligne est autosuffisante : toutes les features sont copiées, aucun JOIN
    nécessaire au moment de l'entraînement ou du réentraînement.

    Corrections importantes :
      - WHERE ts.collected_at >= now - 1h  → copie uniquement la dernière heure,
        pas tout depuis le début (évite les doublons massifs).
      - LEFT JOIN LATERAL sur météo        → ne plante pas si weather_snapshots est vide.
      - DELETE traffic_snapshots > 48h     → évite une croissance infinie de la table LIVE.
    """
    conn = get_connection()
    now  = datetime.now().replace(minute=0, second=0, microsecond=0)
    try:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO traffic_archive (
                    segment_id, archived_at, collected_at,

                    -- trafic
                    current_speed, free_flow_speed, speed_ratio,
                    confidence, road_closure,
                    hour_of_day, day_of_week, is_peak_hour,

                    -- météo
                    weather_main, temperature, wind_speed, humidity,
                    visibility, rain_1h, is_rain, is_fog, weather_modifier,

                    -- OSM statique
                    road_type, length_m, speed_limit,
                    lanes, is_lit, surface, is_oneway, curvature,

                    -- zones sensibles
                    has_school, has_hospital, has_market,

                    -- contexte dynamique
                    has_construction, has_event_nearby, event_type,

                    -- incidents TomTom
                    has_incident_nearby, incident_type, incident_severity,

                    -- incidents utilisateurs
                    has_user_incident, user_incident_type
                )
                SELECT
                    ts.segment_id,
                    %s,
                    ts.collected_at,

                    -- trafic
                    ts.current_speed, ts.free_flow_speed, ts.speed_ratio,
                    ts.confidence, ts.road_closure,
                    ts.hour_of_day, ts.day_of_week, ts.is_peak_hour,

                    -- météo : LEFT JOIN LATERAL → NULL si table vide (pas de crash)
                    w.weather_main, w.temperature, w.wind_speed, w.humidity,
                    w.visibility, w.rain_1h, w.is_rain, w.is_fog, w.risk_modifier,

                    -- OSM statique
                    rs.road_type, rs.length_m, rs.speed_limit,
                    rs.lanes, rs.is_lit, rs.surface, rs.is_oneway, rs.curvature,

                    -- zones sensibles
                    rs.has_school, rs.has_hospital, rs.has_market,

                    -- contexte dynamique
                    rs.has_construction, rs.has_event_nearby, rs.event_type,

                    -- incidents TomTom actifs dans un rayon de 500 m
                    EXISTS (
                        SELECT 1 FROM tomtom_incidents ti
                        WHERE ti.is_active = TRUE
                        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
                    ),
                    (
                        SELECT ti.incident_type FROM tomtom_incidents ti
                        WHERE ti.is_active = TRUE
                        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
                        ORDER BY ti.severity DESC LIMIT 1
                    ),
                    (
                        SELECT ti.severity FROM tomtom_incidents ti
                        WHERE ti.is_active = TRUE
                        AND ST_DWithin(ti.geom::geography, rs.geom::geography, 500)
                        ORDER BY ti.severity DESC LIMIT 1
                    ),

                    -- incidents utilisateurs actifs dans un rayon de 300 m
                    -- ST_DWithin sur geom NULL retourne NULL → EXISTS = FALSE (safe)
                    EXISTS (
                        SELECT 1 FROM incidents ui
                        WHERE ui.is_active = TRUE
                        AND ui.geom IS NOT NULL
                        AND ST_DWithin(ui.geom::geography, rs.geom::geography, 300)
                    ),
                    (
                        SELECT ui.incident_type FROM incidents ui
                        WHERE ui.is_active = TRUE
                        AND ui.geom IS NOT NULL
                        AND ST_DWithin(ui.geom::geography, rs.geom::geography, 300)
                        ORDER BY ui.reported_at DESC LIMIT 1
                    )

                FROM traffic_snapshots ts
                JOIN road_segments rs ON rs.id = ts.segment_id
                LEFT JOIN LATERAL (
                    -- LEFT JOIN : retourne une ligne de NULLs si weather_snapshots est vide
                    SELECT weather_main, temperature, wind_speed, humidity,
                           visibility, rain_1h, is_rain, is_fog, risk_modifier
                    FROM weather_snapshots
                    ORDER BY collected_at DESC
                    LIMIT 1
                ) w ON TRUE
                WHERE ts.collected_at >= %s - INTERVAL '1 hour'
                  AND ts.collected_at <  %s
            """, (now, now, now))

            count = cur.rowcount

            # Nettoyage archive : garder 3 mois
            cur.execute("""
                DELETE FROM traffic_archive
                WHERE archived_at < NOW() - INTERVAL '3 months'
            """)
            deleted_archive = cur.rowcount

            # Nettoyage traffic_snapshots : garder 48h (table LIVE, pas d'historique long)
            cur.execute("""
                DELETE FROM traffic_snapshots
                WHERE collected_at < NOW() - INTERVAL '48 hours'
            """)
            deleted_snap = cur.rowcount

        conn.commit()
        log.info(f"📦 Archive ML : {count:,} lignes à {now:%H:%M} (météo + OSM + incidents inclus)")
        if deleted_archive > 0:
            log.info(f"🧹 Archive : {deleted_archive:,} lignes supprimées (>3 mois)")
        if deleted_snap > 0:
            log.info(f"🧹 Snapshots : {deleted_snap:,} lignes supprimées (>48h)")

    except Exception as e:
        conn.rollback()
        log.error(f"❌ Erreur archivage : {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  SafeWay Data Pipeline démarré  (6 collecteurs)")
    log.info(f"  Trafic  LIVE    : toutes les {COLLECT_INTERVAL_MINUTES} min  → traffic_snapshots")
    log.info(f"  Incidents TomTom: toutes les {COLLECT_INTERVAL_MINUTES} min  → tomtom_incidents")
    log.info(f"  Météo           : toutes les {WEATHER_INTERVAL_MINUTES} min  → weather_snapshots")
    log.info(f"  Archive ML      : toutes les {ARCHIVE_INTERVAL_MINUTES} min  → traffic_archive")
    log.info(f"  Travaux OSM     : toutes les {ARCHIVE_INTERVAL_MINUTES} min  → road_segments")
    log.info(f"  Événements      : toutes les {ARCHIVE_INTERVAL_MINUTES} min  → road_segments")
    log.info(  "  Matchs football : 1 fois/jour à 06:00       → scheduled_matches")
    log.info("=" * 55)

    # Première collecte immédiate au démarrage
    collect_weather()
    collect_football()
    collect_construction()
    collect_events()
    collect_traffic()
    collect_incidents()

    # Planification
    schedule.every(COLLECT_INTERVAL_MINUTES).minutes.do(collect_traffic)
    schedule.every(COLLECT_INTERVAL_MINUTES).minutes.do(collect_incidents)
    schedule.every(WEATHER_INTERVAL_MINUTES).minutes.do(collect_weather)
    schedule.every(ARCHIVE_INTERVAL_MINUTES).minutes.do(archive_traffic)
    schedule.every(ARCHIVE_INTERVAL_MINUTES).minutes.do(collect_construction)
    schedule.every(ARCHIVE_INTERVAL_MINUTES).minutes.do(collect_events)
    schedule.every(1).days.at("06:00").do(collect_football)  # 1 appel/jour à 6h

    while True:
        schedule.run_pending()
        time.sleep(10)
