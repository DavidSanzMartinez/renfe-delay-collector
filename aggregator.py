import psycopg2
import numpy as np
from datetime import datetime
from collections import defaultdict
import logging
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': os.environ.get('DB_PORT', '5432')
}

def aggregate():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()

        # Obté totes les dades brutes creuades amb trips
        # per saber a quina route_id pertany cada trip_id
        # Nota: això assumeix que tens accés a la taula trips
        # Si no, pots extreure el route_id del trip_id si segueix
        # un patró predicitble, o guardar el route_id a delay_observations
        cursor.execute("""
            SELECT
                d.trip_id,
                d.trip_id as route_id,
                EXTRACT(DOW FROM d.observed_at) as day_of_week,
                EXTRACT(HOUR FROM d.observed_at) as hour_of_day,
                d.delay_seconds,
                d.is_cancelled
            FROM delay_observations d
            WHERE d.observed_at > NOW() - INTERVAL '30 days'
        """)
        rows = cursor.fetchall()

        if not rows:
            log.info("No data to aggregate")
            return

        # Agrupa per route_id, day_of_week, hour_of_day
        groups = defaultdict(list)
        cancellations = defaultdict(list)

        for trip_id, route_id, dow, hour, delay, cancelled in rows:
            key = (route_id, int(dow), int(hour))
            if cancelled:
                cancellations[key].append(1)
            else:
                cancellations[key].append(0)
                if delay is not None:
                    groups[key].append(delay)

        # Calcula estadístiques i guarda a delay_stats
        all_keys = set(groups.keys()) | set(cancellations.keys())
        updated_at = datetime.utcnow()

        for key in all_keys:
            route_id, dow, hour = key
            delays = groups.get(key, [])
            cancelled = cancellations.get(key, [])

            mean_delay = float(np.mean(delays)) if delays else 0.0
            std_delay = float(np.std(delays)) if delays else 0.0
            p50 = float(np.percentile(delays, 50)) if delays else 0.0
            p95 = float(np.percentile(delays, 95)) if delays else 0.0
            cancel_rate = float(np.mean(cancelled)) if cancelled else 0.0
            sample_count = len(delays) + sum(cancelled)

            cursor.execute("""
                INSERT INTO delay_stats
                (route_id, day_of_week, hour_of_day, mean_delay, std_delay,
                 percentile_50, percentile_95, cancellation_rate,
                 sample_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (route_id, day_of_week, hour_of_day)
                DO UPDATE SET
                    mean_delay = EXCLUDED.mean_delay,
                    std_delay = EXCLUDED.std_delay,
                    percentile_50 = EXCLUDED.percentile_50,
                    percentile_95 = EXCLUDED.percentile_95,
                    cancellation_rate = EXCLUDED.cancellation_rate,
                    sample_count = EXCLUDED.sample_count,
                    updated_at = EXCLUDED.updated_at
            """, (route_id, dow, hour, mean_delay, std_delay,
                  p50, p95, cancel_rate, sample_count, updated_at))

        conn.commit()
        log.info(f"Aggregated {len(all_keys)} combinations")

    except Exception as e:
        conn.rollback()
        log.error(f"Error aggregating: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    aggregate()
