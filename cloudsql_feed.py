import logging
import threading
import time
from typing import Callable

from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import sqlalchemy

from config import CloudSQLConfig
from scheduler import Delivery

log = logging.getLogger(__name__)

class CloudSQLDeliveryFeed:
    def __init__(self, config: CloudSQLConfig, on_delivery: Callable[[Delivery], None]):
        self.config = config
        self.on_delivery = on_delivery
        self._stop_event = threading.Event()
        self._connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = self._connector.connect(
                self.config.instance_connection_name,
                "pg8000",
                user=self.config.db_user,
                password=self.config.db_pass,
                db=self.config.db_name,
                ip_type=IPTypes.PUBLIC,
            )
            return conn

        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )

    def start(self):
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()
        self._connector.close()

    def _poll_loop(self):
        log.info(f"Starting Cloud SQL polling for match_id: {self.config.match_id}")
        last_id = 0
        
        while not self._stop_event.is_set():
            try:
                with self.engine.connect() as conn:
                    query = sqlalchemy.text(
                        "SELECT deliveries_id, audio_path "
                        "FROM deliveries "
                        "WHERE deliveries_id > :last_id AND audio_path IS NOT NULL "
                        "ORDER BY deliveries_id ASC"
                    )
                    
                    result = conn.execute(query, {"last_id": last_id})
                    rows = result.fetchall()
                    
                    for row in rows:
                        deliveries_id, audio_path = row
                        
                        delivery = Delivery(
                            delivery_id=str(deliveries_id),
                            timestamp=0.0,  # Play immediately
                            audio_path=str(audio_path),
                            duration=5.0,   # Default duration
                        )
                        log.info(f"Polled new delivery: {deliveries_id}")
                        self.on_delivery(delivery)
                        last_id = max(last_id, deliveries_id)
                        
            except Exception as e:
                log.error(f"Error polling Cloud SQL: {e}")
                
            time.sleep(self.config.poll_interval_sec)
