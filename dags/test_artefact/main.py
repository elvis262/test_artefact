import logging
from datetime import datetime, timedelta
from io import BytesIO, StringIO

import pandas as pd

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

logger = logging.getLogger(__name__)


class DataInjectionError(Exception):
    """Exception pour les erreurs d'injection de données."""
    pass


def validate_date(date: str) -> bool:
    """Valide le format de date YYYYMMDD"""
    if len(date) != 8 or not date.isdigit():
        return False
    try:
        datetime.strptime(date, "%Y%m%d")
        return True
    except ValueError:
        return False


@dag(
    dag_id='fashion_store_data_injection',
    description='Injection des données de vente depuis MinIO vers PostgreSQL',
    schedule=None,
    start_date=datetime(2026, 1, 29),
    default_args={
        'owner': 'data-team',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['etl', 'fashion_store', 'minio', 'postgres'],
    params={
        'date': '{{ ds_nodash }}'
    },
)
def fashion_store_etl():
    
    @task
    def validate_execution_date(**context) -> str:
        date = context['params'].get('date', context['ds_nodash'])
        
        if not validate_date(date):
            raise ValueError(f"Format de date invalide: {date}. Attendu: YYYYMMDD")
        
        logger.info(f"Date validée: {date}")
        return date
    
    
    @task
    def check_date_exists(date: str) -> bool:
        formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_external')
        
        try:
            result = pg_hook.get_first(
                sql="SELECT EXISTS(SELECT 1 FROM sale WHERE sale_date = %s)",
                parameters=(formatted_date,)
            )
            
            exists = result[0]
            
            if exists:
                logger.warning(f"Les données pour {formatted_date} existent déjà en base")
            else:
                logger.info(f"Aucune donnée existante pour {formatted_date}")
            
            return exists
            
        except Exception as e:
            logger.error(f"Erreur lors de la vérification de la date: {e}")
            raise DataInjectionError("Impossible de vérifier la date en base") from e
    
    
    @task
    def extract_data_from_minio(date: str, date_exists: bool) -> str:
      
      if date_exists:
          logger.info("Données déjà présentes, extraction ignorée")
          return None
      
      formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
      bucket_name = Variable.get("minio_bucket_name")
      file_name = Variable.get("file_name")
      
      # Utiliser S3Hook pour MinIO
      s3_hook = S3Hook(aws_conn_id='minio_s3')
      
      try:
          logger.info(f"Récupération des données depuis MinIO: {bucket_name}/{file_name}")
          
          obj = s3_hook.get_key(key=file_name, bucket_name=bucket_name)
          csv_content = obj.get()['Body'].read()
          
          df = pd.read_csv(BytesIO(csv_content))
          
          df_filtered = df[df["sale_date"] == formatted_date].drop_duplicates()
          
          if df_filtered.empty:
              logger.warning(f"Aucune donnée trouvée pour la date {formatted_date}")
              return None
          
          logger.info(f"{len(df_filtered)} lignes extraites pour {formatted_date}")
          
          # Retourner les données au format JSON pour XCom
          return df_filtered.to_json(orient='records')
          
      except Exception as e:
          logger.error(f"Erreur MinIO: {e}")
          raise DataInjectionError(f"Impossible de lire {file_name}") from e
    
    
    @task
    def load_data_to_postgres(data_json: str):
        
        if data_json is None:
          logger.info("SKIPPED: Pas de données à charger")
          return {
              'status': 'skipped',
              'rows_inserted': 0,
              'message': 'Pas de données à charger'
          }
        
        # Reconvertir JSON en DataFrame
        df = pd.read_json(StringIO(data_json))
        
        # Remplacer NaN par None pour PostgreSQL
        df = df.where(pd.notnull(df), None)
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_external')
        
        try:
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            
            logger.info(f"Insertion de {len(df)} lignes en base...")
            
            inserted_count = 0
            
            for _, row in df.iterrows():

                try:

                  # Insert Client
                  cur.execute("""
                      INSERT INTO client (customer_id, first_name, last_name, email, country, signup_date, gender, age_range)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                      ON CONFLICT (customer_id) DO NOTHING
                  """, (
                      row['customer_id'], row['first_name'], row['last_name'],
                      row['email'], row['country'], row['signup_date'],
                      row['gender'], row['age_range']
                  ))
                  
                  # Insert Product
                  cur.execute("""
                      INSERT INTO product (product_id, product_name, brand, category, cost_price, color, size, catalog_price)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                      ON CONFLICT (product_id) DO NOTHING
                  """, (
                      row['product_id'], row['product_name'], row['brand'],
                      row['category'], row['cost_price'], row['color'],
                      row['size'], row['catalog_price']
                  ))
                  
                  # Insert Sale
                  cur.execute("""
                      INSERT INTO sale (sale_id, sale_date, channel, channel_campaigns, customer_id)
                      VALUES (%s, %s, %s, %s, %s)
                      ON CONFLICT (sale_id) DO NOTHING
                  """, (
                      row['sale_id'], row['sale_date'], row['channel'],
                      row['channel_campaigns'], row['customer_id']
                  ))
                  
                  # Insert Sale_Product
                  cur.execute("""
                      INSERT INTO sale_product (item_id, sale_id, product_id, quantity, discount_applied)
                      VALUES (%s, %s, %s, %s, %s)
                      ON CONFLICT (item_id) DO NOTHING
                  """, (
                      row['item_id'], row['sale_id'], row['product_id'],
                      row['quantity'], row['discount_applied']
                  ))
                  
                  inserted_count += 1
              
                except Exception as e:
                  logger.error(f"Erreur lors de l'insertion de la ligne {row['sale_id']}: {e}")
                  continue
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Workflow terminé avec succès, {inserted_count} / {len(df)} lignes insérées.")
            
            return {
                'status': 'success',
                'rows_inserted': inserted_count,
                'message': f'{inserted_count} lignes insérées avec succès'
            }
            
        except Exception as e:
            logger.error(f"Erreur PostgreSQL lors de l'insertion: {e}")
            raise DataInjectionError("Impossible d'insérer les données") from e
    
    
    @task
    def send_notification(result: dict, date: str):
        
        status = result.get('status', 'unknown')
        rows = result.get('rows_inserted', 0)
        message = result.get('message', '')
        
        formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        
        if status == 'success':
            logger.info(f"SUCCESS: {rows} lignes insérées pour {formatted_date}")
        elif status == 'skipped':
            logger.info(f"SKIPPED: {message} pour {formatted_date}")
        else:
            logger.warning(f"UNKNOWN STATUS: {message}")
    
    
    # Définir le flux du DAG
    execution_date = validate_execution_date()
    date_exists = check_date_exists(execution_date)
    data = extract_data_from_minio(execution_date, date_exists)
    result = load_data_to_postgres(data)
    send_notification(result, execution_date)


# Instancier le DAG
fashion_store_dag = fashion_store_etl()