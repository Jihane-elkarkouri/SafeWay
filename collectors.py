"""
collectors.py — Collecte des données APIs → PostgreSQL
=======================================================
7 collecteurs :
  1. OSMCollector            : graphe routier + zones sensibles (une fois)
  2. TomTomCollector         : trafic temps réel (6 zones × 3 types, 11 min)
  3. WeatherCollector        : météo Casablanca (30 min)
  4. ConstructionCollector   : travaux OSM temps réel (1h)
  5. EventCollector          : fêtes + matchs + événements (1h)
  6. FootballCollector       : matchs Botola Pro Raja/WAC (1/jour)
  7. TomTomIncidentCollector : accidents/incidents temps réel (11 min)
"""

import math
import time
import logging
from datetime import datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_batch

from config import (
    TOMTOM_API_KEY, OPENWEATHER_API_KEY,
    TOMTOM_INCIDENTS_KEY,
    API_FOOTBALL_KEY, BOTOLA_LEAGUE_ID, BOTOLA_SEASON, CASA_TEAM_IDS,
    CASABLANCA,
)
from database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# 1. COLLECTEUR OSM — Graphe routier Casablanca
# ══════════════════════════════════════════════════════════════════
class OSMCollector:
    """
    Collecte les segments routiers depuis l'API Overpass (OpenStreetMap).
    Collecte aussi les zones sensibles (écoles, hôpitaux, marchés).
    À exécuter UNE SEULE FOIS pour initialiser la base.
    """

    OVERPASS_URL = "https://overpass-api.de/api/interpreter"

    ROAD_TYPES = [
        "motorway", "trunk", "primary", "secondary",
        "tertiary", "residential", "unclassified", "living_street"
    ]

    SPEED_LIMITS = {
        "motorway": 120, "trunk": 90, "primary": 60,
        "secondary": 50, "tertiary": 40,
        "residential": 30, "unclassified": 30, "living_street": 20,
    }

    def _overpass_query(self, query: str) -> dict:
        """Exécute une requête Overpass et retourne le JSON."""
        log.info("📡 Requête Overpass API...")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "SafeWay/1.0 (projet etudiant)",
        }
        for attempt in range(3):
            try:
                resp = requests.post(
                    self.OVERPASS_URL,
                    data={"data": query},
                    headers=headers,
                    timeout=90,
                )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    wait = 60 * (attempt + 1)
                    log.warning(f"Rate limit Overpass, attente {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError("Overpass API inaccessible après 3 tentatives")

    def _haversine_m(self, lat1, lng1, lat2, lng2) -> float:
        """Calcule la distance en mètres entre deux coordonnées."""
        R = 6371000
        dlat = math.radians(lat2 - lat1)
        dlng = math.radians(lng2 - lng1)
        a = (math.sin(dlat/2)**2 +
             math.cos(math.radians(lat1)) *
             math.cos(math.radians(lat2)) *
             math.sin(dlng/2)**2)
        return R * 2 * math.asin(math.sqrt(a))

    def _compute_curvature(self, geometry: list) -> float:
        """
        Calcule la courbure moyenne d'un segment à partir de sa géométrie OSM.
        Retourne l'angle de déviation moyen en degrés :
          0   = ligne parfaitement droite
          >30 = virage notable
          >90 = virage serré (dangereux)
        """
        if len(geometry) < 3:
            return 0.0
        angles = []
        for i in range(1, len(geometry) - 1):
            dx1 = geometry[i]["lon"]   - geometry[i-1]["lon"]
            dy1 = geometry[i]["lat"]   - geometry[i-1]["lat"]
            dx2 = geometry[i+1]["lon"] - geometry[i]["lon"]
            dy2 = geometry[i+1]["lat"] - geometry[i]["lat"]
            norm1 = math.hypot(dx1, dy1)
            norm2 = math.hypot(dx2, dy2)
            if norm1 < 1e-9 or norm2 < 1e-9:
                continue
            cos_a = (dx1*dx2 + dy1*dy2) / (norm1 * norm2)
            cos_a = max(-1.0, min(1.0, cos_a))   # clamp pour éviter erreur acos
            angle = math.degrees(math.acos(cos_a))
            angles.append(angle)
        return round(sum(angles) / len(angles), 2) if angles else 0.0

    def _parse_lanes(self, tags: dict) -> int:
        """Extrait le nombre de voies depuis les tags OSM (défaut 1)."""
        raw = tags.get("lanes") or tags.get("lanes:forward", "1")
        try:
            return max(1, int(str(raw).strip()))
        except (ValueError, TypeError):
            return 1

    def _parse_surface(self, tags: dict) -> str:
        """
        Normalise le tag OSM 'surface' en catégories utiles pour le scoring.
        Groupes :
          asphalt  → revêtement standard (risque normal)
          concrete → béton (risque légèrement plus élevé par temps humide)
          unpaved  → non revêtu (risque élevé)
          unknown  → non renseigné
        """
        raw = tags.get("surface", "").lower().strip()
        ASPHALT  = {"asphalt", "paved", "tarmac", "bituminous"}
        CONCRETE = {"concrete", "paving_stones", "sett", "cobblestone"}
        UNPAVED  = {"unpaved", "gravel", "dirt", "ground", "sand",
                    "grass", "mud", "compacted", "fine_gravel"}
        if raw in ASPHALT:  return "asphalt"
        if raw in CONCRETE: return "concrete"
        if raw in UNPAVED:  return "unpaved"
        return "unknown"

    def _parse_lit(self, tags: dict):
        """
        Extrait le tag OSM 'lit'.
        Retourne True, False ou None (inconnu).
        """
        raw = tags.get("lit", "").lower().strip()
        if raw in ("yes", "24/7", "automatic"): return True
        if raw in ("no",):                      return False
        return None   # tag absent ou valeur non standard

    def collect_roads(self):
        """Collecte tous les segments routiers de Casablanca avec tags géométriques."""
        road_filter = "|".join(self.ROAD_TYPES)
        query = f"""
        [out:json][timeout:90];
        (
          way["highway"~"^({road_filter})$"]
             ({CASABLANCA['min_lat']},{CASABLANCA['min_lng']},
              {CASABLANCA['max_lat']},{CASABLANCA['max_lng']});
        );
        out body geom qt;
        """
        # Note : 'out body geom' retourne déjà tous les tags OSM (lanes, lit,
        # surface, oneway, name...) + la géométrie complète (liste de nodes).
        # Aucun appel supplémentaire nécessaire — tout est dans la même réponse.
        data = self._overpass_query(query)
        ways = data.get("elements", [])
        log.info(f"🗺  {len(ways)} ways OSM récupérés")
        return ways

    def collect_sensitive_zones(self):
        """Collecte les zones sensibles (écoles, hôpitaux, marchés)."""
        query = f"""
        [out:json][timeout:60];
        (
          node["amenity"~"school|hospital|marketplace|clinic|pharmacy"]
             ({CASABLANCA['min_lat']},{CASABLANCA['min_lng']},
              {CASABLANCA['max_lat']},{CASABLANCA['max_lng']});
        );
        out body;
        """
        data = self._overpass_query(query)
        pois = data.get("elements", [])
        log.info(f"🏫 {len(pois)} zones sensibles récupérées")
        return pois

    def _has_sensitive_zone(self, mid_lat, mid_lng, pois, radius_m=300):
        """Vérifie si une zone sensible est dans le rayon du segment."""
        school = hospital = market = False
        for poi in pois:
            dist = self._haversine_m(mid_lat, mid_lng, poi["lat"], poi["lon"])
            if dist <= radius_m:
                amenity = poi.get("tags", {}).get("amenity", "")
                if amenity == "school":        school   = True
                if amenity in ("hospital", "clinic", "pharmacy"):
                                               hospital = True
                if amenity == "marketplace":   market   = True
        return school, hospital, market

    def save_to_db(self, ways, pois):
        """Sauvegarde les segments dans PostgreSQL avec features géométriques enrichies."""
        conn = get_connection()
        saved = skipped = 0

        try:
            with conn.cursor() as cur:
                for way in ways:
                    osm_id    = way.get("id")
                    tags      = way.get("tags", {})
                    road_type = tags.get("highway", "unclassified")
                    name      = tags.get("name") or tags.get("name:fr") or f"Rue OSM {osm_id}"
                    speed     = self.SPEED_LIMITS.get(road_type, 40)
                    geometry  = way.get("geometry", [])

                    if len(geometry) < 2:
                        skipped += 1
                        continue

                    # ── Features géométriques extraites une fois par way ───────
                    lanes     = self._parse_lanes(tags)
                    surface   = self._parse_surface(tags)
                    is_lit    = self._parse_lit(tags)
                    is_oneway = tags.get("oneway", "no").lower() in ("yes", "1", "true")
                    # Courbure calculée sur la géométrie complète du way
                    curvature = self._compute_curvature(geometry)

                    # Découper en sous-segments consécutifs
                    for i in range(len(geometry) - 1):
                        s = geometry[i]
                        e = geometry[i + 1]
                        s_lat, s_lng = s["lat"], s["lon"]
                        e_lat, e_lng = e["lat"], e["lon"]
                        mid_lat = (s_lat + e_lat) / 2
                        mid_lng = (s_lng + e_lng) / 2
                        length  = self._haversine_m(s_lat, s_lng, e_lat, e_lng)

                        school, hospital, market = self._has_sensitive_zone(
                            mid_lat, mid_lng, pois
                        )

                        # Géométrie PostGIS WKT
                        geom_wkt = (
                            f"SRID=4326;LINESTRING("
                            f"{s_lng} {s_lat},{e_lng} {e_lat})"
                        )

                        # osm_id unique seulement pour le premier sous-segment
                        seg_osm_id = osm_id if i == 0 else None

                        cur.execute("""
                            INSERT INTO road_segments
                                (osm_id, name, road_type, start_lat, start_lng,
                                 end_lat, end_lng, length_m, speed_limit,
                                 has_school, has_hospital, has_market,
                                 lanes, is_lit, surface, is_oneway, curvature,
                                 geom)
                            VALUES
                                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                 %s,%s,%s,%s,%s,
                                 ST_GeomFromEWKT(%s))
                            ON CONFLICT (start_lat, start_lng, end_lat, end_lng) DO UPDATE SET
                                lanes      = EXCLUDED.lanes,
                                is_lit     = EXCLUDED.is_lit,
                                surface    = EXCLUDED.surface,
                                is_oneway  = EXCLUDED.is_oneway,
                                curvature  = EXCLUDED.curvature,
                                updated_at = NOW()
                        """, (
                            seg_osm_id, name, road_type,
                            s_lat, s_lng, e_lat, e_lng,
                            round(length, 1), speed,
                            school, hospital, market,
                            lanes, is_lit, surface, is_oneway, curvature,
                            geom_wkt
                        ))
                        saved += 1

                conn.commit()
                log.info(f"✅ OSM sauvegardé : {saved} segments, {skipped} ignorés")

                # Stats qualité des données géométriques
                cur.execute("SELECT COUNT(*) FROM road_segments WHERE is_lit IS NOT NULL")
                lit_known = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM road_segments WHERE surface != 'unknown'")
                surface_known = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM road_segments WHERE curvature > 5")
                curved = cur.fetchone()[0]
                log.info(f"   ├─ Éclairage renseigné : {lit_known:,} segments")
                log.info(f"   ├─ Surface renseignée  : {surface_known:,} segments")
                log.info(f"   └─ Segments avec virage: {curved:,} segments (courbure > 5°)")

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur sauvegarde OSM : {e}")
            raise
        finally:
            conn.close()

    def run(self):
        """Point d'entrée principal."""
        log.info("=== Collecte OSM Casablanca ===")
        ways = self.collect_roads()
        pois = self.collect_sensitive_zones()
        self.save_to_db(ways, pois)


# ══════════════════════════════════════════════════════════════════
# 2. COLLECTEUR TOMTOM — Trafic temps réel
# ══════════════════════════════════════════════════════════════════
class TomTomCollector:
    """
    Stratégie optimale : 6 zones géographiques × 3 types de route = 18 appels/cycle.
    - 1 appel TomTom par (zone × type) sur un point GPS représentatif
    - Propagation PostGIS à tous les segments de même type dans la zone
    - Types non interrogés (tertiary, residential) → vitesse dérivée de secondary
    → 18 × 144 cycles/jour = 2 592 req/jour (quota gratuit 2 500 ✅ avec marge)
    """

    BASE_URL   = "https://api.tomtom.com/traffic/services/4/flowSegmentData"
    ZOOM       = 12
    PEAK_HOURS = {7, 8, 9, 12, 13, 17, 18, 19}

    # ── 6 zones géographiques de Casablanca ───────────────────────────────────
    # Chaque zone : (nom, lat_centre, lng_centre, rayon_km)
    ZONES = [
        ("nord_ouest", 33.6200, -7.6500, 3.0),   # Ain Sebaa, Sidi Bernoussi
        ("nord_est",   33.6100, -7.5400, 3.0),   # Sidi Moumen, Ain Chock nord
        ("centre",     33.5900, -7.6100, 2.5),   # Centre-ville, Maarif
        ("ouest",      33.5700, -7.6800, 3.0),   # Oulfa, Hay Hassani
        ("est",        33.5600, -7.5500, 3.0),   # Ain Chock, Sidi Maarouf
        ("sud",        33.5300, -7.6200, 3.5),   # Moulay Rachid, Lissasfa
    ]

    # ── Points GPS représentatifs par (zone × type) ───────────────────────────
    # Coordonnées de routes réelles à Casablanca pour chaque type
    ZONE_POINTS = {
        # (zone_name, road_group) → (lat, lng)
        ("nord_ouest", "motorway"):  (33.6150, -7.5800),  # Autoroute A3 nord
        ("nord_ouest", "primary"):   (33.6100, -7.6300),  # Blvd Lalla Yacout nord
        ("nord_ouest", "secondary"): (33.6050, -7.6550),  # Ave Ain Sebaa

        ("nord_est",   "motorway"):  (33.6200, -7.5500),  # Rocade nord-est
        ("nord_est",   "primary"):   (33.6000, -7.5600),  # Blvd Sidi Moumen
        ("nord_est",   "secondary"): (33.5950, -7.5450),  # Ave Ain Chock

        ("centre",     "motorway"):  (33.5950, -7.5950),  # Autoroute urbaine
        ("centre",     "primary"):   (33.5900, -7.6050),  # Blvd Hassan II
        ("centre",     "secondary"): (33.5850, -7.6150),  # Ave Mohammed V

        ("ouest",      "motorway"):  (33.5700, -7.6700),  # A7 Oulfa
        ("ouest",      "primary"):   (33.5750, -7.6600),  # Blvd Hay Hassani
        ("ouest",      "secondary"): (33.5650, -7.6750),  # Ave Oulfa

        ("est",        "motorway"):  (33.5600, -7.5600),  # Rocade est
        ("est",        "primary"):   (33.5650, -7.5500),  # Blvd Sidi Maarouf
        ("est",        "secondary"): (33.5550, -7.5650),  # Ave Ain Chock sud

        ("sud",        "motorway"):  (33.5300, -7.6100),  # A7 sud
        ("sud",        "primary"):   (33.5400, -7.6200),  # Blvd Moulay Rachid
        ("sud",        "secondary"): (33.5250, -7.6300),  # Ave Lissasfa
    }

    # Coefficients de dérivation pour les types non interrogés
    DERIVE = {
        "tertiary":     0.85,
        "residential":  0.60,
        "living_street": 0.40,
        "unclassified": 0.70,
    }

    def _road_group(self, road_type: str) -> str:
        """Regroupe motorway+trunk → 'motorway' pour la clé de zone."""
        if road_type in ("motorway", "trunk"):
            return "motorway"
        if road_type in ("primary",):
            return "primary"
        return "secondary"

    def _fetch_segment_traffic(self, lat: float, lng: float) -> dict | None:
        """Appelle TomTom pour un point GPS et retourne les données trafic."""
        url    = f"{self.BASE_URL}/absolute/{self.ZOOM}/json"
        params = {"key": TOMTOM_API_KEY, "point": f"{lat},{lng}", "unit": "KMPH"}
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("flowSegmentData")
            elif resp.status_code == 404:
                return None
            else:
                log.warning(f"TomTom {resp.status_code} pour ({lat},{lng})")
                return None
        except requests.RequestException as e:
            log.error(f"Erreur TomTom : {e}")
            return None

    def _zone_of_segment(self, mid_lat: float, mid_lng: float) -> str:
        """Retourne la zone la plus proche d'un segment (distance euclidienne)."""
        best_zone, best_dist = "centre", float("inf")
        for zone_name, z_lat, z_lng, _ in self.ZONES:
            dist = ((mid_lat - z_lat) ** 2 + (mid_lng - z_lng) ** 2) ** 0.5
            if dist < best_dist:
                best_dist, best_zone = dist, zone_name
        return best_zone

    def collect_and_save(self):
        """
        Stratégie 6 zones × 3 types :
        1. 18 appels TomTom sur des points GPS représentatifs (zone × type)
        2. Affectation de chaque segment à sa zone la plus proche
        3. Propagation de la vitesse (zone × type) → segment
        4. Dérivation pour tertiary/residential/living_street
        """
        conn    = get_connection()
        now     = datetime.now()
        hour    = now.hour
        dow     = now.weekday()
        is_peak = hour in self.PEAK_HOURS

        try:
            with conn.cursor() as cur:

                # ── Étape 1 : 18 appels TomTom (6 zones × 3 types) ────────────
                log.info("🚗 Collecte TomTom : 6 zones × 3 types = 18 appels...")
                zone_traffic: dict[tuple, dict] = {}

                for (zone_name, road_group), (lat, lng) in self.ZONE_POINTS.items():
                    data = self._fetch_segment_traffic(lat, lng)

                    if data:
                        curr_speed = data.get("currentSpeed", 0)
                        free_speed = data.get("freeFlowSpeed", 50)
                        confidence = data.get("confidence", 0.5)
                        closure    = data.get("roadClosure", False)
                        ratio      = curr_speed / free_speed if free_speed > 0 else 1.0
                    else:
                        # Fallback par type si TomTom ne répond pas
                        defaults   = {"motorway": 90, "primary": 50, "secondary": 35}
                        curr_speed = free_speed = defaults.get(road_group, 40)
                        confidence = 0.3
                        closure    = False
                        ratio      = 1.0

                    zone_traffic[(zone_name, road_group)] = {
                        "current_speed":   curr_speed,
                        "free_flow_speed": free_speed,
                        "confidence":      confidence,
                        "road_closure":    closure,
                        "speed_ratio":     round(ratio, 3),
                    }
                    log.debug(f"  Zone {zone_name:12s} × {road_group:10s} → {curr_speed} km/h")
                    time.sleep(0.05)

                log.info(f"✅ {len(zone_traffic)} couples (zone × type) collectés")

                # ── Étape 2 : récupérer tous les segments ──────────────────────
                cur.execute("""
                    SELECT id, road_type,
                           (start_lat + end_lat) / 2 AS mid_lat,
                           (start_lng + end_lng) / 2 AS mid_lng
                    FROM road_segments
                    ORDER BY id
                """)
                all_segments = cur.fetchall()

                # ── Étape 3 : propagation + dérivation ────────────────────────
                batch      = []
                real_count = 0
                deriv_count = 0

                for seg_id, road_type, mid_lat, mid_lng in all_segments:
                    zone      = self._zone_of_segment(mid_lat, mid_lng)
                    grp       = self._road_group(road_type)
                    t         = zone_traffic.get((zone, grp))

                    if t:
                        # Type directement interrogé (motorway, primary, secondary)
                        curr_speed = t["current_speed"]
                        free_speed = t["free_flow_speed"]
                        confidence = t["confidence"]
                        closure    = t["road_closure"]
                        ratio      = t["speed_ratio"]
                        real_count += 1
                    else:
                        # Type dérivé (tertiary, residential, living_street...)
                        coeff      = self.DERIVE.get(road_type, 0.70)
                        base       = zone_traffic.get((zone, "secondary"), {})
                        base_speed = base.get("current_speed", 35)
                        base_free  = base.get("free_flow_speed", 35)
                        curr_speed = round(base_speed * coeff, 1)
                        free_speed = round(base_free  * coeff, 1)
                        confidence = round(base.get("confidence", 0.3) * 0.8, 3)
                        closure    = False
                        ratio      = round(curr_speed / free_speed, 3) if free_speed > 0 else 1.0
                        deriv_count += 1

                    batch.append((
                        seg_id, now,
                        curr_speed, free_speed, ratio,
                        confidence, closure,
                        hour, dow, is_peak
                    ))

                execute_batch(cur, """
                    INSERT INTO traffic_snapshots
                        (segment_id, collected_at, current_speed, free_flow_speed,
                         speed_ratio, confidence, road_closure,
                         hour_of_day, day_of_week, is_peak_hour)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, batch)

                conn.commit()
                log.info(f"✅ Trafic sauvegardé : {len(batch)} snapshots à {now:%H:%M}")
                log.info(f"   ├─ {real_count:,} segments  → vitesse TomTom réelle (motorway/primary/secondary)")
                log.info(f"   └─ {deriv_count:,} segments → vitesse dérivée (tertiary/residential/living_street)")

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur collecte trafic : {e}")
        finally:
            conn.close()

    def run(self):
        self.collect_and_save()


# ══════════════════════════════════════════════════════════════════
# 3. COLLECTEUR MÉTÉO — OpenWeather
# ══════════════════════════════════════════════════════════════════
class WeatherCollector:
    """
    Collecte la météo de Casablanca depuis OpenWeatherMap.
    Gratuit : 1 000 appels/jour.
    Appel toutes les 30 min = 48 appels/jour.
    """

    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def _risk_modifier(self, weather_main: str, visibility: int,
                       rain_1h: float, wind: float) -> float:
        """
        Calcule un multiplicateur de risque selon les conditions météo.
        1.0 = conditions normales, 2.0 = risque doublé.
        """
        modifier = 1.0
        if weather_main in ("Rain", "Drizzle"):   modifier += 0.4
        if weather_main == "Thunderstorm":         modifier += 0.8
        if weather_main == "Fog":                  modifier += 0.6
        if weather_main == "Snow":                 modifier += 0.5
        if visibility < 1000:                      modifier += 0.3
        if rain_1h > 5:                            modifier += 0.2
        if wind > 15:                              modifier += 0.1
        return round(min(modifier, 2.5), 2)

    def collect_and_save(self):
        """Collecte la météo et sauvegarde dans PostgreSQL."""
        params = {
            "lat":   CASABLANCA["lat"],
            "lon":   CASABLANCA["lng"],
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
            "lang":  "fr",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            weather_main = data["weather"][0]["main"]
            weather_desc = data["weather"][0]["description"]
            temp         = data["main"]["temp"]
            humidity     = data["main"]["humidity"]
            wind_speed   = data["wind"]["speed"]
            visibility   = data.get("visibility", 10000)
            rain_1h      = data.get("rain", {}).get("1h", 0.0)

            is_rain = weather_main in ("Rain", "Drizzle", "Thunderstorm")
            is_fog  = weather_main in ("Fog", "Mist", "Haze")
            modifier = self._risk_modifier(weather_main, visibility, rain_1h, wind_speed)

            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO weather_snapshots
                            (temperature, weather_main, weather_desc,
                             wind_speed, humidity, visibility,
                             rain_1h, is_rain, is_fog, risk_modifier)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        temp, weather_main, weather_desc,
                        wind_speed, humidity, visibility,
                        rain_1h, is_rain, is_fog, modifier
                    ))
                conn.commit()
                log.info(f"🌤  Météo : {weather_desc}, {temp}°C, "
                         f"modificateur risque={modifier}")
            finally:
                conn.close()

        except Exception as e:
            log.error(f"❌ Erreur météo : {e}")

    def run(self):
        self.collect_and_save()



# ══════════════════════════════════════════════════════════════════
# 4. COLLECTEUR TRAVAUX — Overpass API (OSM Construction)
# ══════════════════════════════════════════════════════════════════
class ConstructionCollector:
    """
    Détecte les travaux actifs à Casablanca via Overpass API (OSM).
    Marque les segments affectés dans road_segments.has_construction.
    Gratuit, déjà dans le stack.
    Fréquence recommandée : toutes les heures.
    """

    def _fetch_constructions(self) -> list[dict]:
        """Récupère les voies en travaux depuis OSM via Overpass."""
        bbox = (
            f"{CASABLANCA['min_lat']},{CASABLANCA['min_lng']},"
            f"{CASABLANCA['max_lat']},{CASABLANCA['max_lng']}"
        )
        query = f"""
        [out:json][timeout:30];
        (
          way["highway"]["construction"]({bbox});
          way["highway"]["planned"]({bbox});
          way["construction"~"yes|road"]({bbox});
        );
        out geom;
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "SafeWay/1.0",
        }
        try:
            resp = requests.post(
                "https://overpass-api.de/api/interpreter",
                data={"data": query},
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json().get("elements", [])
        except Exception as e:
            log.error(f"❌ Overpass travaux : {e}")
            return []

    def collect_and_save(self):
        """
        1. Récupère les travaux OSM
        2. Pour chaque travaux, marque les segments road_segments proches
           (within 50m) avec has_construction=TRUE
        3. Remet à FALSE les segments qui ne sont plus en travaux
        """
        log.info("🚧 Collecte des travaux OSM...")
        constructions = self._fetch_constructions()
        log.info(f"   {len(constructions)} zones de travaux trouvées dans OSM")

        conn = get_connection()
        try:
            with conn.cursor() as cur:

                # Remettre tous les segments à has_construction=FALSE
                cur.execute("UPDATE road_segments SET has_construction = FALSE")

                if constructions:
                    marked = 0
                    for way in constructions:
                        geometry = way.get("geometry", [])
                        if len(geometry) < 2:
                            continue

                        # Calculer le centre du tronçon en travaux
                        lats = [p["lat"] for p in geometry]
                        lngs = [p["lon"] for p in geometry]
                        center_lat = sum(lats) / len(lats)
                        center_lng = sum(lngs) / len(lngs)

                        # Marquer les segments dans un rayon de 100m
                        cur.execute("""
                            UPDATE road_segments
                            SET has_construction = TRUE
                            WHERE ST_DWithin(
                                geom::geography,
                                ST_GeomFromText(
                                    'POINT(' || %s || ' ' || %s || ')',
                                    4326
                                )::geography,
                                100
                            )
                        """, (center_lng, center_lat))
                        marked += cur.rowcount

                    log.info(f"   ✅ {marked} segments marqués has_construction=TRUE")

            conn.commit()

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur collecte travaux : {e}")
        finally:
            conn.close()

    def run(self):
        self.collect_and_save()


# ══════════════════════════════════════════════════════════════════
# 5. COLLECTEUR ÉVÉNEMENTS — Calendrier + Google Places
# ══════════════════════════════════════════════════════════════════
class EventCollector:
    """
    Détecte deux types d'événements :
    A) Calendrier manuel : événements récurrents prévisibles
       (matchs Raja/WAC, Aid, Ramadan, marchés hebdo...)
    B) Google Places Nearby Search : événements ponctuels
       autour des stades, salles de concert, mosquées (optionnel)

    Résultat : table events_active avec les événements en cours,
    et flag has_event_nearby sur traffic_archive.
    """

    # ══════════════════════════════════════════════════════════════════
    # CALENDRIER COMPLET DES ÉVÉNEMENTS MAROC
    # Sources : Upsilon Consulting, Humantal, JoursFeries.fr (mai 2026)
    # ══════════════════════════════════════════════════════════════════

    # ── A. Fêtes nationales fixes (calendrier grégorien) ─────────────────────
    # Impactent TOUTE la ville (city_wide) — fort trafic familial/cérémoniel
    FETES_NATIONALES = [
        # (mois, jour, nom, event_type)
        (1,  1,  "Nouvel An",                      "national_holiday"),
        (1,  11, "Manifeste de l'Indépendance",   "national_holiday"),
        (1,  14, "Yennayer — Nouvel An Amazigh",   "national_holiday"),  # férié depuis 2023
        (5,  1,  "Fête du Travail",                "national_holiday"),
        (7,  30, "Fête du Trône",                  "national_holiday"),  # très fort trafic
        (8,  14, "Allégeance Oued Eddahab",        "national_holiday"),
        (8,  20, "Révolution du Roi et du Peuple", "national_holiday"),
        (8,  21, "Fête de la Jeunesse",            "national_holiday"),
        (10, 31, "Aïd Al Wahda — Fête de l'Unité","national_holiday"),  # nouveau depuis nov 2025
        (11, 6,  "Marche Verte",                   "national_holiday"),
        (11, 18, "Fête de l'Indépendance",        "national_holiday"),
    ]

    # ── B. Fêtes religieuses variables (calendrier hégirien) ─────────────────
    # Dates confirmées ou prévues 2025-2028
    # Source : Ministère des Habous + calculs astronomiques
    FETES_RELIGIEUSES = {
        "aid_fitr": [
            (2025, 3, 30),  # Aid El Fitr 2025
            (2026, 3, 19),  # Aid El Fitr 2026 (date officielle)
            (2027, 3,  9),  # Aid El Fitr 2027
            (2028, 2, 27),  # Aid El Fitr 2028
        ],
        "aid_adha": [
            (2025, 6,  6),  # Aid El Adha 2025
            (2026, 5, 26),  # Aid El Adha 2026
            (2027, 5, 16),  # Aid El Adha 2027
            (2028, 5,  4),  # Aid El Adha 2028
        ],
        "nouvel_an_hegirien": [
            (2025, 6, 27),  # 1er Muharram 1447
            (2026, 6, 16),  # 1er Muharram 1448
            (2027, 6,  6),  # 1er Muharram 1449
        ],
        "mawlid": [
            (2025, 9,  4),  # Mawlid Annabawi 2025
            (2026, 8, 25),  # Mawlid Annabawi 2026
            (2027, 8, 15),  # Mawlid Annabawi 2027
        ],
    }

    # ── C. Ramadan — nuits très chargées ─────────────────────────────────────
    RAMADAN_STARTS = [
        (2025, 3,  1),
        (2026, 2, 18),
        (2027, 2,  8),
        (2028, 1, 28),
    ]

    # ── D. Événements localisés récurrents ───────────────────────────────────
    # Stade Mohammed V + Grand Stade de Casablanca
    STADE_MOHAMMED_V    = (33.5582, -7.6114)
    STADE_GRAND_CASA    = (33.5608, -7.6331)

    RECURRING = [
        # ── Matchs Raja CA ──────────────────────────────────────────────────
        # Domicile : Stade Mohammed V (principalement le dimanche)
        {
            "name":       "Match Raja CA",
            "type":       "football_api",   # remplacé par API-Football si connecté
            "weekday":    [6],              # dimanche par défaut
            "hours":      [14, 15, 16, 17, 18, 19, 20, 21],
            "location":   STADE_MOHAMMED_V,
            "radius_km":  3.0,
            "event_type": "match",
        },
        # ── Matchs WAC ──────────────────────────────────────────────────────
        # Domicile : Grand Stade / Stade Mohammed V (principalement samedi)
        {
            "name":       "Match WAC",
            "type":       "football_api",
            "weekday":    [5],              # samedi par défaut
            "hours":      [14, 15, 16, 17, 18, 19, 20, 21],
            "location":   STADE_GRAND_CASA,
            "radius_km":  3.0,
            "event_type": "match",
        },
        # ── Souk El Had ─────────────────────────────────────────────────────
        {
            "name":       "Souk El Had",
            "type":       "weekly",
            "weekday":    [6],
            "hours":      list(range(7, 16)),
            "location":   (33.5650, -7.5900),
            "radius_km":  1.5,
            "event_type": "market",
        },
        # ── Marché Derb Ghallef (électronique) — vendredi/samedi ─────────────
        {
            "name":       "Marché Derb Ghallef",
            "type":       "weekly",
            "weekday":    [4, 5],           # vendredi + samedi
            "hours":      list(range(9, 20)),
            "location":   (33.5780, -7.6420),
            "radius_km":  1.0,
            "event_type": "market",
        },
        # ── Prière du vendredi — mosquées principales ─────────────────────────
        {
            "name":       "Prière du vendredi",
            "type":       "weekly",
            "weekday":    [4],              # vendredi
            "hours":      [12, 13, 14],
            "location":   None,             # city_wide (toutes les mosquées)
            "radius_km":  None,
            "event_type": "prayer",
        },
        # ── Ramadan — nuits festives ──────────────────────────────────────────
        {
            "name":       "Ramadan nuit",
            "type":       "ramadan",
            "hours":      [20, 21, 22, 23, 0, 1],
            "location":   None,
            "radius_km":  None,
            "event_type": "ramadan",
        },
        # ── Ramadan — rush ftour (rupture du jeûne) ───────────────────────────
        {
            "name":       "Ramadan ftour",
            "type":       "ramadan_ftour",  # ~30 min avant le coucher du soleil
            "hours":      [17, 18, 19, 20], # variable selon la saison
            "location":   None,
            "radius_km":  None,
            "event_type": "ramadan_ftour",
        },
    ]

    # ── Tolérance ±1 jour pour les fêtes religieuses (croissant lunaire) ─────
    AID_TOLERANCE_DAYS = 1

    def _is_ramadan(self, dt: datetime) -> bool:
        """Retourne True si la date est dans une période de Ramadan (~30 jours)."""
        for y, m, d in self.RAMADAN_STARTS:
            start = datetime(y, m, d)
            end   = start + timedelta(days=30)
            if start <= dt <= end:
                return True
        return False

    def _is_ramadan_ftour(self, dt: datetime) -> bool:
        """Retourne True si on est dans la fenêtre ftour du Ramadan."""
        return self._is_ramadan(dt)

    def _is_fete_nationale(self, dt: datetime) -> bool:
        """Retourne True si la date est une fête nationale fixe."""
        for month, day, name, etype in self.FETES_NATIONALES:
            if dt.month == month and dt.day == day:
                return True, name, etype
        return False, None, None

    def _is_fete_religieuse(self, dt: datetime) -> tuple:
        """Retourne (True, nom, type) si la date est une fête religieuse (±1j tolérance)."""
        labels = {
            "aid_fitr":           ("Aïd El Fitr",         "aid_fitr"),
            "aid_adha":           ("Aïd El Adha",         "aid_adha"),
            "nouvel_an_hegirien": ("Nouvel An Hégirien",  "nouvel_an_hegirien"),
            "mawlid":             ("Mawlid Annabawi",     "mawlid"),
        }
        for key, dates in self.FETES_RELIGIEUSES.items():
            name, etype = labels[key]
            for y, m, d in dates:
                fete = datetime(y, m, d)
                if abs((dt.date() - fete.date()).days) <= self.AID_TOLERANCE_DAYS:
                    return True, name, etype
        return False, None, None

    def _is_aid(self, dt: datetime) -> bool:
        """Retourne True si la date est un jour d'Aid (fitr ou adha)."""
        for key in ("aid_fitr", "aid_adha"):
            for y, m, d in self.FETES_RELIGIEUSES[key]:
                aid_date = datetime(y, m, d)
                if abs((dt.date() - aid_date.date()).days) <= self.AID_TOLERANCE_DAYS:
                    return True
        return False

    def get_active_events(self, dt: datetime = None) -> list[dict]:
        """
        Retourne la liste des événements actifs à l'instant dt.
        Combine :
          - Fêtes nationales fixes
          - Fêtes religieuses variables (Aid, Mawlid, Nouvel An hégirien)
          - Ramadan (nuits + ftour)
          - Événements récurrents localisés (matchs, marchés, prière)
        """
        if dt is None:
            dt = datetime.now()

        active = []

        # ── 1. Fêtes nationales fixes ────────────────────────────────────────
        is_fn, fn_name, fn_type = self._is_fete_nationale(dt)
        if is_fn:
            active.append({
                "name":       fn_name,
                "event_type": fn_type,
                "location":   None,
                "radius_km":  None,
                "city_wide":  True,
            })

        # ── 2. Fêtes religieuses variables ───────────────────────────────────
        is_fr, fr_name, fr_type = self._is_fete_religieuse(dt)
        if is_fr:
            active.append({
                "name":       fr_name,
                "event_type": fr_type,
                "location":   None,
                "radius_km":  None,
                "city_wide":  True,
            })

        # ── 3. Matchs réels depuis scheduled_matches (API-Football) ────────────
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                # Match actif = match_date dans [-1h, +3h] par rapport à maintenant
                cur.execute("""
                    SELECT home_team, away_team, venue_lat, venue_lng, radius_km
                    FROM scheduled_matches
                    WHERE status != 'finished'
                    AND match_date BETWEEN NOW() - INTERVAL '1 hour'
                                      AND NOW() + INTERVAL '3 hours'
                """)
                live_matches = cur.fetchall()
            conn.close()

            for home, away, vlat, vlng, radius in live_matches:
                active.append({
                    "name":       f"{home} vs {away}",
                    "event_type": "match",
                    "location":   (vlat, vlng),
                    "radius_km":  radius,
                    "city_wide":  False,
                })
                log.info(f"⚽ Match actif détecté : {home} vs {away}")

        except Exception as e:
            log.warning(f"Impossible de lire scheduled_matches : {e}")
            # Fallback sur le calendrier fixe si DB inaccessible
            for ev in self.RECURRING:
                if ev["type"] == "football_api":
                    hours = ev.get("hours", list(range(24)))
                    if dt.hour in hours and dt.weekday() in ev["weekday"]:
                        active.append({
                            "name":       ev["name"],
                            "event_type": ev["event_type"],
                            "location":   ev["location"],
                            "radius_km":  ev["radius_km"],
                            "city_wide":  False,
                        })

        # ── 4. Autres événements récurrents localisés ────────────────────────
        for ev in self.RECURRING:
            ev_type = ev["type"]
            if ev_type == "football_api":
                continue   # déjà géré via scheduled_matches

            hours = ev.get("hours", list(range(24)))
            if dt.hour not in hours:
                continue

            if ev_type == "weekly":
                if dt.weekday() not in ev["weekday"]:
                    continue
            elif ev_type == "ramadan":
                if not self._is_ramadan(dt):
                    continue
            elif ev_type == "ramadan_ftour":
                if not self._is_ramadan_ftour(dt):
                    continue

            active.append({
                "name":       ev["name"],
                "event_type": ev["event_type"],
                "location":   ev["location"],
                "radius_km":  ev["radius_km"],
                "city_wide":  ev["location"] is None,
            })

        return active

    def _segment_has_event(
        self,
        mid_lat: float,
        mid_lng: float,
        active_events: list[dict]
    ) -> tuple[bool, str | None]:
        """
        Retourne (has_event, event_type) pour un segment donné.
        Un événement city_wide affecte tous les segments.
        Un événement localisé affecte les segments dans son rayon.
        """
        for ev in active_events:
            if ev["city_wide"]:
                return True, ev["event_type"]

            e_lat, e_lng = ev["location"]
            radius_deg   = ev["radius_km"] / 111.0   # ~111 km par degré
            dist = ((mid_lat - e_lat) ** 2 + (mid_lng - e_lng) ** 2) ** 0.5
            if dist <= radius_deg:
                return True, ev["event_type"]

        return False, None

    def collect_and_save(self):
        """
        1. Détermine les événements actifs maintenant
        2. Met à jour has_event_nearby + event_type dans road_segments
        """
        now    = datetime.now()
        active = self.get_active_events(now)

        if active:
            names = [e["name"] for e in active]
            log.info(f"🎉 Événements actifs : {', '.join(names)}")
        else:
            log.info("🎉 Aucun événement actif en ce moment")

        conn = get_connection()
        try:
            with conn.cursor() as cur:

                # Remettre tous les segments à has_event_nearby=FALSE
                cur.execute("""
                    UPDATE road_segments
                    SET has_event_nearby = FALSE,
                        event_type       = NULL
                """)

                if active:
                    # Récupérer tous les segments pour les évaluer
                    cur.execute("""
                        SELECT id,
                               (start_lat + end_lat) / 2,
                               (start_lng + end_lng) / 2
                        FROM road_segments
                    """)
                    segments = cur.fetchall()

                    affected = []
                    for seg_id, mid_lat, mid_lng in segments:
                        has_ev, ev_type = self._segment_has_event(
                            mid_lat, mid_lng, active
                        )
                        if has_ev:
                            affected.append((ev_type, seg_id))

                    if affected:
                        execute_batch(cur, """
                            UPDATE road_segments
                            SET has_event_nearby = TRUE,
                                event_type       = %s
                            WHERE id = %s
                        """, affected)
                        log.info(f"   ✅ {len(affected):,} segments marqués has_event_nearby=TRUE")

            conn.commit()

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur collecte événements : {e}")
        finally:
            conn.close()

    def run(self):
        self.collect_and_save()

# ══════════════════════════════════════════════════════════════════
# 7. COLLECTEUR INCIDENTS TOMTOM — Incidents trafic temps réel
# ══════════════════════════════════════════════════════════════════
class TomTomIncidentCollector:
    """
    Collecte les incidents de trafic temps réel depuis l'API TomTom Incidents v5.
    Couvre la bounding box de Casablanca.
    Quota gratuit : 2 500 req/jour → 1 appel/cycle = 144 appels/jour (10 min)

    Types d'incidents retournés par TomTom :
      - ACCIDENT           : accident de la route
      - JAM                : embouteillage
      - ROAD_WORK          : travaux signalés par TomTom (≠ OSM)
      - LANE_CLOSED        : voie fermée
      - DISABLED_VEHICLE   : véhicule en panne
      - OTHER              : autre incident

    Niveaux de sévérité :
      1 = Inconnu / 2 = Mineur / 3 = Modéré / 4 = Majeur
    """

    BASE_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"

    # Correspondance des magnitudes TomTom → libellés
    MAGNITUDE_LABELS = {1: "unknown", 2: "minor", 3: "moderate", 4: "major"}

    # Correspondance des codes iconCategory → type lisible
    ICON_CATEGORY = {
        1:  "ACCIDENT",
        2:  "JAM",
        3:  "ROAD_WORK",
        4:  "LANE_CLOSED",
        5:  "DISABLED_VEHICLE",
        6:  "ROAD_WORK",
        7:  "ROAD_CLOSED",
        8:  "JAM",
        9:  "JAM",
        10: "JAM",
        11: "ACCIDENT",
        14: "DISABLED_VEHICLE",
    }

    def _fetch_incidents(self) -> list[dict]:
        """
        Appelle l'API TomTom Incidents v5 sur la bounding box de Casablanca.
        Retourne la liste brute des incidents (features GeoJSON).
        """
        bbox = (
            f"{CASABLANCA['min_lng']},{CASABLANCA['min_lat']},"
            f"{CASABLANCA['max_lng']},{CASABLANCA['max_lat']}"
        )
        params = {
            "key":      TOMTOM_API_KEY,
            "bbox":     bbox,
            "fields":   "{incidents{type,geometry,properties{id,iconCategory,magnitudeOfDelay,"
                        "events{description,code,iconCategory},"
                        "startPoint,endPoint,from,to,length,delay,roadNumbers,timeValidity}}}",
            "language": "fr-FR",
            "t":        "1111",          # toutes les catégories d'incidents
            "expandCluster": "true",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                incidents = data.get("incidents", [])
                log.info(f"🚨 TomTom Incidents : {len(incidents)} incidents récupérés")
                return incidents
            elif resp.status_code == 403:
                log.error("TomTom Incidents : clé API invalide ou incidents non activés (403)")
                return []
            elif resp.status_code == 404:
                log.warning("TomTom Incidents : aucun incident dans la zone (404)")
                return []
            else:
                log.warning(f"TomTom Incidents : HTTP {resp.status_code}")
                return []
        except requests.RequestException as e:
            log.error(f"❌ Erreur réseau TomTom Incidents : {e}")
            return []

    def _parse_incident(self, feature: dict) -> dict | None:
        """
        Extrait les champs utiles d'un incident GeoJSON TomTom.
        Retourne un dict prêt pour l'INSERT, ou None si données insuffisantes.
        """
        props    = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        # ID unique TomTom
        incident_id = props.get("id")
        if not incident_id:
            return None

        # Coordonnées : TomTom retourne soit un Point soit un LineString
        geom_type   = geometry.get("type", "")
        coordinates = geometry.get("coordinates", [])

        if geom_type == "Point" and len(coordinates) >= 2:
            lng, lat = coordinates[0], coordinates[1]
        elif geom_type == "LineString" and len(coordinates) > 0:
            # Prendre le point médian de la ligne
            mid = len(coordinates) // 2
            lng, lat = coordinates[mid][0], coordinates[mid][1]
        else:
            return None

        # Type d'incident via iconCategory
        icon_cat      = props.get("iconCategory", 0)
        incident_type = self.ICON_CATEGORY.get(icon_cat, "OTHER")

        # Sévérité (magnitudeOfDelay : 1-4)
        severity = props.get("magnitudeOfDelay", 1)

        # Description depuis events[0] si disponible
        events      = props.get("events", [])
        description = events[0].get("description", "") if events else ""

        # Points de début/fin (texte)
        from_point = props.get("from", "")
        to_point   = props.get("to", "")

        # Route concernée
        road_numbers = props.get("roadNumbers", [])
        road_name    = ", ".join(road_numbers) if road_numbers else ""

        # Métriques
        delay_s  = props.get("delay", 0) or 0
        length_m = props.get("length", 0) or 0

        return {
            "incident_id":   incident_id,
            "incident_type": incident_type,
            "severity":      severity,
            "description":   description,
            "lat":           lat,
            "lng":           lng,
            "from_point":    from_point,
            "to_point":      to_point,
            "road_name":     road_name,
            "delay_seconds": int(delay_s),
            "length_m":      float(length_m),
        }

    def collect_and_save(self):
        """
        1. Récupère les incidents TomTom
        2. Upsert dans tomtom_incidents (INSERT ou mise à jour last_seen_at)
        3. Marque comme inactifs les incidents disparus depuis > 30 min
        4. Met à jour road_segments.has_incident_nearby (optionnel si colonne présente)
        """
        raw_incidents = self._fetch_incidents()

        # Parser tous les incidents valides
        parsed = []
        for feature in raw_incidents:
            result = self._parse_incident(feature)
            if result:
                parsed.append(result)

        log.info(f"   {len(parsed)} incidents valides à persister")

        conn = get_connection()
        try:
            with conn.cursor() as cur:

                # ── Étape 1 : Upsert des incidents actifs ─────────────────────
                active_ids = []
                for inc in parsed:
                    geom_wkt = f"SRID=4326;POINT({inc['lng']} {inc['lat']})"
                    cur.execute("""
                        INSERT INTO tomtom_incidents
                            (incident_id, incident_type, severity, description,
                             lat, lng, from_point, to_point, road_name,
                             delay_seconds, length_m,
                             is_active, first_seen_at, last_seen_at, geom)
                        VALUES
                            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                             TRUE, NOW(), NOW(), ST_GeomFromEWKT(%s))
                        ON CONFLICT (incident_id) DO UPDATE SET
                            incident_type  = EXCLUDED.incident_type,
                            severity       = EXCLUDED.severity,
                            description    = EXCLUDED.description,
                            delay_seconds  = EXCLUDED.delay_seconds,
                            length_m       = EXCLUDED.length_m,
                            is_active      = TRUE,
                            last_seen_at   = NOW()
                    """, (
                        inc["incident_id"], inc["incident_type"], inc["severity"],
                        inc["description"], inc["lat"], inc["lng"],
                        inc["from_point"], inc["to_point"], inc["road_name"],
                        inc["delay_seconds"], inc["length_m"],
                        geom_wkt,
                    ))
                    active_ids.append(inc["incident_id"])

                # ── Étape 2 : Désactiver les incidents disparus (>30 min) ──────
                cur.execute("""
                    UPDATE tomtom_incidents
                    SET is_active = FALSE
                    WHERE is_active = TRUE
                    AND last_seen_at < NOW() - INTERVAL '30 minutes'
                """)
                expired = cur.rowcount

                # ── Étape 3 : Nettoyer les incidents inactifs de plus de 24h ──
                cur.execute("""
                    DELETE FROM tomtom_incidents
                    WHERE is_active = FALSE
                    AND last_seen_at < NOW() - INTERVAL '24 hours'
                """)
                deleted = cur.rowcount

            conn.commit()

            # Résumé par type
            if parsed:
                from collections import Counter
                type_counts = Counter(inc["incident_type"] for inc in parsed)
                summary = ", ".join(f"{t}:{n}" for t, n in type_counts.most_common())
                log.info(f"   ✅ Incidents actifs : {summary}")
            if expired > 0:
                log.info(f"   ⏱  {expired} incident(s) expiré(s) marqué(s) inactifs")
            if deleted > 0:
                log.info(f"   🧹 {deleted} ancien(s) incident(s) supprimé(s)")

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur collecte incidents TomTom : {e}")
        finally:
            conn.close()

    def run(self):
        self.collect_and_save()


# ══════════════════════════════════════════════════════════════════
# 6. COLLECTEUR MATCHS — API-Football (Botola Pro)
# ══════════════════════════════════════════════════════════════════
class FootballCollector:
    """
    Récupère les matchs Raja CA et WAC à domicile depuis API-Football.
    1 appel/jour → stocke dans scheduled_matches.
    EventCollector consulte cette table au lieu du calendrier fixe.
    Quota gratuit : 100 req/jour → 1 utilisée ici, 99 de marge.
    """

    BASE_URL     = "https://v3.football.api-sports.io"
    STADES = {
        "Raja CA": {"lat": 33.5582, "lng": -7.6114, "name": "Stade Mohammed V"},
        "WAC":     {"lat": 33.5608, "lng": -7.6331, "name": "Grand Stade Casablanca"},
    }

    def _fetch_upcoming_fixtures(self) -> list[dict]:
        """Récupère les matchs des 7 prochains jours pour Raja + WAC."""
        headers = {
            "x-apisports-key": API_FOOTBALL_KEY,
        }
        all_fixtures = []

        for team_name, team_id in CASA_TEAM_IDS.items():
            try:
                resp = requests.get(
                    f"{self.BASE_URL}/fixtures",
                    headers=headers,
                    params={
                        "team":   team_id,
                        "league": BOTOLA_LEAGUE_ID,
                        "season": BOTOLA_SEASON,
                        "next":   7,        # 7 prochains matchs
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()

                if data.get("errors"):
                    log.error(f"API-Football erreur : {data['errors']}")
                    continue

                fixtures = data.get("response", [])
                log.info(f"  ⚽ {team_name} : {len(fixtures)} matchs à venir")

                for f in fixtures:
                    fixture    = f["fixture"]
                    teams      = f["teams"]
                    home_team  = teams["home"]["name"]
                    away_team  = teams["away"]["name"]

                    # Seulement les matchs à DOMICILE à Casablanca
                    is_home_casa = any(
                        t in home_team
                        for t in ["Raja", "Wydad", "WAC"]
                    )
                    if not is_home_casa:
                        log.debug(f"    Ignoré (extérieur) : {home_team} vs {away_team}")
                        continue

                    stade = self.STADES.get(team_name, self.STADES["Raja CA"])
                    all_fixtures.append({
                        "fixture_id": fixture["id"],
                        "home_team":  home_team,
                        "away_team":  away_team,
                        "match_date": fixture["date"],
                        "venue_lat":  stade["lat"],
                        "venue_lng":  stade["lng"],
                        "status":     fixture["status"]["short"],
                    })

                time.sleep(0.5)  # pause entre les 2 appels

            except requests.RequestException as e:
                log.error(f"❌ API-Football erreur réseau ({team_name}) : {e}")

        return all_fixtures

    def collect_and_save(self):
        """
        1. Récupère les prochains matchs Raja/WAC à domicile
        2. Upsert dans scheduled_matches
        3. Marque les matchs terminés comme 'finished'
        """
        log.info("⚽ Collecte matchs API-Football (Botola Pro)...")
        fixtures = self._fetch_upcoming_fixtures()

        if not fixtures:
            log.warning("   Aucun match à domicile trouvé dans les 7 prochains jours")
            return

        conn = get_connection()
        try:
            with conn.cursor() as cur:

                # Upsert des matchs
                for f in fixtures:
                    cur.execute("""
                        INSERT INTO scheduled_matches
                            (fixture_id, home_team, away_team, match_date,
                             venue_lat, venue_lng, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (fixture_id) DO UPDATE SET
                            status     = EXCLUDED.status,
                            fetched_at = NOW()
                    """, (
                        f["fixture_id"], f["home_team"], f["away_team"],
                        f["match_date"], f["venue_lat"], f["venue_lng"],
                        f["status"],
                    ))

                # Marquer les vieux matchs comme finished
                cur.execute("""
                    UPDATE scheduled_matches
                    SET status = 'finished'
                    WHERE match_date < NOW() - INTERVAL '3 hours'
                    AND status != 'finished'
                """)

                # Nettoyer les matchs de plus de 30 jours
                cur.execute("""
                    DELETE FROM scheduled_matches
                    WHERE match_date < NOW() - INTERVAL '30 days'
                """)

            conn.commit()
            log.info(f"   ✅ {len(fixtures)} matchs sauvegardés dans scheduled_matches")
            for f in fixtures:
                log.info(f"      🏟  {f['home_team']} vs {f['away_team']} — {f['match_date']}")

        except Exception as e:
            conn.rollback()
            log.error(f"❌ Erreur sauvegarde matchs : {e}")
        finally:
            conn.close()

    def run(self):
        self.collect_and_save()


# ══════════════════════════════════════════════════════════════════
# TEST RAPIDE
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python collectors.py [osm|traffic|weather|construction|events|football|incidents|all]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd in ("osm", "all"):
        OSMCollector().run()

    if cmd in ("traffic", "all"):
        TomTomCollector().run()

    if cmd in ("weather", "all"):
        WeatherCollector().run()

    if cmd in ("construction", "all"):
        ConstructionCollector().run()

    if cmd in ("events", "all"):
        EventCollector().run()

    if cmd in ("football", "all"):
        FootballCollector().run()

    if cmd in ("incidents", "all"):
        TomTomIncidentCollector().run()


