import requests
import psycopg2
from datetime import datetime
import logging
import os

logging.basisConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

#Llegeix les credencials de les variables d'entorn de Render
#Mai les posis hardcodejas al codi
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': os.environ.get('DB_PORT', '5432')
}


FEEDS = {
    'LD': 'https://gtfsrt.renfe.com/trip_updates_LD.json',
    'RC': 'https://gtfsrt.renfe.com/trip_updates.json'
}

def fetch_feed(url):
    """Descarrega el feed JSON de Renfe. Retorna None si hi ha error."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Error fetching {url}: {e}")
        return None

def parse_observations(data, source):
    """
    Extreu els retards i cancel·laciones del feed.
    Només guarda registres amb retard o cancel·lació —
    els trens puntuals no aporten informació per reliability.
    """
    observations = []
    if not data or 'entity' not in data:
        return observations

    observed_at = datetime.utcnow()

    for entity in data['entity']:
        trip_update = entity.get('tripUpdate', {})
        trip = trip_update.get('trip', {})
        trip_id = trip.get('tripId')

        if not trip_id:
            continue

        schedule_rel = trip.get('scheduleRelationship', 'SCHEDULED')
        is_cancelled = schedule_rel == 'CANCELED'
        delay_seconds = trip_update.get('delay', None)

        # Només guarda si hi ha retard o cancel·lació
        if delay_seconds is not None or is_cancelled:
            observations.append((
                observed_at,
                trip_id,
                delay_seconds,
                is_cancelled,
                source
            ))

    return observations

def save_observations(observations):
    """Inserta les observacions a Supabase."""
    if not observations:
        log.info("No observations to save")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO delay_observations
            (observed_at, trip_id, delay_seconds, is_cancelled, source)
            VALUES (%s, %s, %s, %s, %s)
        """, observations)
        conn.commit()
        log.info(f"Saved {len(observations)} observations")
    except Exception as e:
        conn.rollback()
        log.error(f"Error saving: {e}")
    finally:
        cursor.close()
        conn.close()

def cleanup_old_data():
    """
    Borra observacions de més de 30 dies per no saturar
    els 500MB de Supabase. L'aggregator ja haurà processat
    aquestes dades abans de que s'esborrin.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM delay_observations
            WHERE observed_at < NOW() - INTERVAL '30 days'
        """)
        deleted = cursor.rowcount
        conn.commit()
        log.info(f"Deleted {deleted} old observations")
    except Exception as e:
        conn.rollback()
        log.error(f"Error cleaning up: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    log.info("Starting delay collection")
    all_observations = []

    for source, url in FEEDS.items():
        data = fetch_feed(url)
        obs = parse_observations(data, source)
        all_observations.extend(obs)
        log.info(f"{source}: {len(obs)} observations")

    save_observations(all_observations)
    cleanup_old_data()
    log.info("Done")

if __name__ == "__main__":
    main()
