import os
import sys
import logging
from datetime import datetime
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
import psycopg

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class DataInjectionError(Exception):
    """Exception pour les erreurs d'injection de données."""
    pass


def validate_date(date: str) -> bool:
    if len(date) != 8 or not date.isdigit():
        return False
    try:
        datetime.strptime(date, "%Y%m%d")
        return True
    except ValueError:
        return False

class DataInjection:
    def __init__(self, bucket:str, file:str, client:Minio, database_url:str):
        self.bucket = bucket
        self.file = file
        self.client = client
        self.database_url = database_url
    
    def extract_data_from_minio(self, date: str) -> pd.DataFrame:
        formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        try:
            with self.client.get_object(self.bucket, self.file) as response:
                df = pd.read_csv(BytesIO(response.read()))
            return df[df["sale_date"] == formatted_date].drop_duplicates()
        except S3Error as e:
            logger.error("Erreur Minio: %s", e)
            raise DataInjectionError(f"Impossible de lire {self.file}") from e
    
    def check_date_exists(self, date: str) -> bool:
        formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT EXISTS(SELECT 1 FROM sale WHERE sale_date = %s)",
                        (formatted_date,)
                    )
                    return cur.fetchone()[0]
        except psycopg.Error as e:
            logger.error("Erreur PostgreSQL: %s", e)
            raise DataInjectionError("Impossible de vérifier la date en base") from e

    def load_data_to_postgres(self, df: pd.DataFrame):
        try:
            df = df.where(pd.notnull(df), None)
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    
                    inserted_count = 0;
                    for _, row in df.iterrows():
                        try:
                            cur.execute("""
                                INSERT INTO client (customer_id, first_name, last_name, email, country, signup_date, gender, age_range)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (customer_id) DO NOTHING
                            """, (
                                row['customer_id'], row['first_name'], row['last_name'],
                                row['email'], row['country'], row['signup_date'],
                                row['gender'], row['age_range']
                            ))
                            cur.execute("""
                                INSERT INTO product (product_id, product_name, brand, category, cost_price, color, size, catalog_price)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (product_id) DO NOTHING
                            """, (
                                row['product_id'], row['product_name'], row['brand'],
                                row['category'], row['cost_price'], row['color'],
                                row['size'], row['catalog_price']
                            ))
                            cur.execute("""
                                INSERT INTO sale (sale_id, sale_date, channel, channel_campaigns, customer_id)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (sale_id) DO NOTHING
                            """, (
                                row['sale_id'], row['sale_date'], row['channel'],
                                row['channel_campaigns'], row['customer_id']
                            ))
                            cur.execute("""
                                INSERT INTO sale_product (item_id, sale_id, product_id, quantity, discount_applied)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (item_id) DO NOTHING
                            """, (
                                row['item_id'], row['sale_id'], row['product_id'],
                                row['quantity'], row['discount_applied']
                            ))
                        
                            inserted_count += 1
                        except psycopg.Error as e:
                            logger.error("Erreur PostgreSQL lors de l'insertion d'une ligne: %s", e)
                            continue
                        
                conn.commit()
        except psycopg.Error as e:
            logger.error("Erreur PostgreSQL lors de l'insertion: %s", e)
            raise DataInjectionError("Impossible d'insérer les données") from e

    def start_workflow(self, date: str) -> bool:
        logger.info("Démarrage du workflow pour la date: %s", date)

        try:
            if self.check_date_exists(date):
                logger.warning("Les données pour %s existent déjà en base", date)
                return False

            logger.info("Récupération des données depuis Minio...")
            df = self.extract_data_from_minio(date)

            if df.empty:
                logger.warning("Aucune donnée trouvée pour la date %s", date)
                return False

            logger.info("Insertion de %d lignes en base...", len(df))
            self.load_data_to_postgres(df)
            logger.info("Workflow terminé avec succès, %d lignes insérées.", len(df))
            return True

        except DataInjectionError:
            return False
        except Exception as e:
            logger.exception("Erreur inattendue: %s", e)
            return False


def main():
    database_url = os.getenv("DATABASE_URL")
    minio_root_user = os.getenv("MINIO_ROOT_USER")
    minio_root_password = os.getenv("MINIO_ROOT_PASSWORD")
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    file_name = os.getenv("FILE_NAME", "fashion_store_sales.csv")
    
    if len(sys.argv) > 1:

        date = sys.argv[1]
        
        
        if validate_date(date):
            
            if not all([database_url, minio_root_user, minio_root_password, bucket_name, file_name]):
                logger.error("Variables d'environnement manquantes")
                return

            client = Minio(
                "localhost:9000",
                access_key=minio_root_user,
                secret_key=minio_root_password,
                secure=False
            )

            injection = DataInjection(bucket_name, file_name, client, database_url)
            injection.start_workflow(date)
            
        else :
            logger.error("Format de date invalide: %s. Attendu: YYYYMMDD", date)
    
    else:
        logger.error('Argument attendu, date au format YYYYMMDD ex: "20230615"')

        
    


if __name__ == "__main__":
    main()
