import logging
from config import CloudSQLConfig
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import sqlalchemy

config = CloudSQLConfig(
    instance_connection_name="sportsanalytics-495612:europe-west2:sportsdb",
    db_user="postgres",
    db_pass="Admin1234!",
    db_name="postgres"
)

connector = Connector()
def getconn():
    return connector.connect(
        config.instance_connection_name,
        "pg8000",
        user=config.db_user,
        password=config.db_pass,
        db=config.db_name,
        ip_type=IPTypes.PUBLIC,
    )

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)

try:
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT column_name FROM information_schema.columns WHERE table_name = 'deliveries';"))
        columns = [row[0] for row in result.fetchall()]
        print("\n\nCOLUMNS IN DELIVERIES TABLE:", columns, "\n\n")
finally:
    connector.close()
