# Fashion Store ETL

Pipeline ETL qui extrait des donnees de vente depuis MinIO (stockage S3), les normalise et les charge dans PostgreSQL, orchestrable via Airflow ou en standalone.

**Repo GitHub: [github](https://github.com/elvis262/test_artefact.git)**

## Prerequis

- Docker et Docker Compose
- [uv](https://docs.astral.sh/uv/) (pour l'execution standalone)

## Pourquoi uv

`uv` remplace `pip`, `pip-tools`, `virtualenv` et `pyenv` par un seul binaire ecrit en Rust. Il resout les dependances et installe les packages en une fraction du temps de pip. Le fichier `uv.lock` garantit des installations reproductibles sans configuration supplementaire. Contrairement a Poetry ou pipenv, uv ne necessite pas de runtime Python pre-installe pour fonctionner.

### Installation de uv

Linux / macOS :

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell) :

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Alternativement, sur tous les OS via pip :

```bash
pip install uv
```

### Lancer le projet en standalone

```bash
uv sync
uv run main.py 20250415
```

Le script `main.py` attend une date au format `YYYYMMDD` en argument. Il se connecte directement a MinIO et PostgreSQL via les variables definies dans `.env`.

## Docker Compose

### Fichier `.env`

Toutes les variables d'environnement sont centralisees dans `.env` (ignore par git). Le docker-compose y accede via la directive `env_file` et la syntaxe `${VAR:-default}` qui fournit une valeur par defaut si la variable n'est pas definie.

Exemple de `.env` a creer :

```env
FILE_NAME=fashion_store_sales.csv

# PostgreSQL externe (BDD metier)
POSTGRES_USER=artefact
POSTGRES_PASSWORD=password
POSTGRES_DB=fashion_store
POSTGRES_HOST=postgres
POSTGRES_PORT=5433
POSTGRES_HOST_EXTERNAL=localhost

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET_NAME=fashion-store-bucket
MINIO_ENDPOINT=http://s3:9000
MINIO_ENDPOINT_EXTERNAL=http://localhost:9000
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001

# Airflow
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.6
AIRFLOW_UID=1000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW__CORE__FERNET_KEY=

# Airflow variables (prefixe AIRFLOW_VAR_ = auto-importees comme Variables Airflow)
AIRFLOW_VAR_MINIO_BUCKET_NAME=${MINIO_BUCKET_NAME}
AIRFLOW_VAR_FILE_NAME=${FILE_NAME}

# Airflow connections (prefixe AIRFLOW_CONN_ = auto-importees comme Connections Airflow)
AIRFLOW_CONN_POSTGRES_EXTERNAL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@external-db:5432/${POSTGRES_DB}
AIRFLOW_CONN_MINIO_S3=<json de connexion AWS>

# Dependencies installees au demarrage dans les conteneurs Airflow
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-amazon apache-airflow-providers-postgres pandas boto3 psycopg2-binary

# URL de connexion pour le script standalone
DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST_EXTERNAL}:5433/${POSTGRES_DB}
```

### Services applicatifs

#### `external-db` (PostgreSQL metier)

Base de donnees cible ou sont chargees les donnees normalisees.

| Variable | Utilite |
|---|---|
| `POSTGRES_USER` | Utilisateur PostgreSQL |
| `POSTGRES_PASSWORD` | Mot de passe PostgreSQL |
| `POSTGRES_DB` | Nom de la base de donnees |
| `POSTGRES_PORT` | Port expose sur l'hote (defaut: 5433, pour eviter le conflit avec le PostgreSQL d'Airflow) |

Ce service utilise un **Dockerfile custom** (`docker/postgres/Dockerfile`) :

```dockerfile
FROM postgres:17.6-alpine3.22
COPY docker/postgres/init.sql /docker-entrypoint-initdb.d/init.sql
```

L'image de base `postgres` execute automatiquement les scripts places dans `/docker-entrypoint-initdb.d/` lors du **premier demarrage** du conteneur (quand le volume de donnees est vide). Cela permet de creer les tables `client`, `product`, `sale` et `sale_product` sans intervention manuelle. Le choix de l'image `alpine` reduit la taille de l'image.

Le `shm_size: 128mb` augmente la memoire partagee disponible pour PostgreSQL, ce qui evite des erreurs lors de requetes complexes (la valeur par defaut Docker de 64MB est souvent insuffisante).

Le `healthcheck` avec `pg_isready` permet aux services dependants (Airflow) d'attendre que PostgreSQL soit reellement pret a accepter des connexions avant de demarrer.

#### `s3` (MinIO)

Stockage objet compatible S3 qui contient le fichier CSV source.

| Variable | Utilite |
|---|---|
| `MINIO_ROOT_USER` | Identifiant administrateur MinIO |
| `MINIO_ROOT_PASSWORD` | Mot de passe administrateur MinIO |
| `MINIO_API_PORT` | Port de l'API S3 (defaut: 9000) |
| `MINIO_CONSOLE_PORT` | Port de l'interface web MinIO (defaut: 9001) |

#### `s3-init`

Conteneur ephemere qui initialise MinIO : il cree le bucket et y copie les fichiers CSV depuis `./data/`. Il depend de `s3` via `condition: service_healthy` pour s'assurer que MinIO est pret.

| Variable | Utilite |
|---|---|
| `MINIO_ROOT_USER` | Credentials pour la connexion au serveur MinIO |
| `MINIO_ROOT_PASSWORD` | Credentials pour la connexion au serveur MinIO |
| `MINIO_BUCKET_NAME` | Nom du bucket a creer (defaut: `fashion-store-bucket`) |

### Services Airflow

Tous les services Airflow heritent du bloc `x-airflow-common` (ancre YAML `&airflow-common`) qui definit l'image, les variables d'environnement et les volumes montes. Cela evite la duplication de configuration.

Les variables communes chargees depuis `.env` dans tous les services Airflow :

| Variable | Utilite |
|---|---|
| `AIRFLOW_IMAGE_NAME` | Image Docker Airflow utilisee |
| `AIRFLOW_UID` | UID Unix pour les fichiers crees par Airflow (evite les problemes de permissions sur Linux) |
| `AIRFLOW_PROJ_DIR` | Racine du projet, utilisee pour monter les volumes `dags/`, `logs/`, `config/`, `plugins/`, `data/` |
| `AIRFLOW__CORE__FERNET_KEY` | Cle de chiffrement pour les connexions stockees en base. Vous pouvez générer une clé avec la commande ```bash python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"```|
| `_PIP_ADDITIONAL_REQUIREMENTS` | Packages Python installes au demarrage dans chaque conteneur |
| `_AIRFLOW_WWW_USER_USERNAME` | Login de l'admin Airflow (utilise par `airflow-init`) |
| `_AIRFLOW_WWW_USER_PASSWORD` | Mot de passe de l'admin Airflow (utilise par `airflow-init`) |

Variables Airflow (prefixe `AIRFLOW_VAR_`) -- automatiquement importees comme Variables dans l'interface Airflow, accessibles via `Variable.get("nom")` dans le code du DAG :

| Variable | Cle Airflow | Utilite |
|---|---|---|
| `AIRFLOW_VAR_MINIO_BUCKET_NAME` | `minio_bucket_name` | Nom du bucket MinIO utilise par le DAG pour lire le CSV |
| `AIRFLOW_VAR_FILE_NAME` | `file_name` | Nom du fichier CSV a extraire depuis MinIO |
| `AIRFLOW_VAR_ENV` | `env` | Environnement d'execution (`development`, `production`) |
| `AIRFLOW_VAR_S3_BUCKET` | `s3_bucket` | Alias du bucket S3/MinIO |
| `AIRFLOW_VAR_MAX_RETRIES` | `max_retries` | Nombre maximal de tentatives pour les operations |
| `AIRFLOW_VAR_DATA_PATH` | `data_path` | Chemin des donnees dans le conteneur Airflow |
| `AIRFLOW_VAR_MINIO_ENDPOINT` | `minio_endpoint` | URL interne du serveur MinIO (`http://s3:9000`) |

Connexions Airflow (prefixe `AIRFLOW_CONN_`) -- automatiquement importees comme Connections dans Airflow, referencees par leur `conn_id` dans les Hooks :

| Variable | conn_id | Utilite |
|---|---|---|
| `AIRFLOW_CONN_POSTGRES_EXTERNAL` | `postgres_external` | Connexion vers la BDD metier. Utilisee par `PostgresHook(postgres_conn_id='postgres_external')` dans le DAG |
| `AIRFLOW_CONN_MINIO_S3` | `minio_s3` | Connexion vers MinIO au format JSON. Utilisee par `S3Hook(aws_conn_id='minio_s3')` dans le DAG |

#### `airflow-postgres`

Base de donnees interne d'Airflow. Stocke les metadonnees : etat des DAGs, historique des executions, connexions, variables. Separee de `external-db` pour isoler les donnees d'orchestration des donnees metier.

#### `redis`

Broker de messages pour le `CeleryExecutor`. Il fait transiter les taches entre le scheduler et les workers. Sans lui, Airflow ne pourrait pas distribuer l'execution des taches.

#### `airflow-init`

Conteneur d'initialisation execute une seule fois (`service_completed_successfully`). Il cree les repertoires, applique les migrations de la BDD Airflow, cree l'utilisateur admin et verifie les ressources systeme (RAM, CPU, disque). Il tourne en `user: "0:0"` (root) pour pouvoir modifier les permissions des volumes.

#### `airflow-apiserver`

Serveur API et interface web (port 8080). Point d'entree pour declencher des DAGs, consulter les logs et gerer les connexions/variables.

#### `airflow-scheduler`

Planifie l'execution des DAGs selon leur `schedule`. Il surveille en continu les DAGs et cree des DagRuns quand les conditions sont remplies.

#### `airflow-dag-processor`

Parse les fichiers Python dans `dags/` pour en extraire la definition des DAGs. Separe du scheduler dans Airflow 3 pour des raisons de securite et de performance.

#### `airflow-worker`

Execute les taches via Celery. C'est ici que le code des tasks du DAG tourne reellement. La variable `DUMB_INIT_SETSID: "0"` permet un arret propre du worker sans tuer ses processus enfants.

#### `airflow-triggerer`

Gere les taches asynchrones (deferrable operators). Il surveille les triggers en attente sans bloquer de slot worker.

#### `airflow-cli` (profil `debug`)

Conteneur utilitaire pour executer des commandes `airflow` manuellement. Active uniquement avec `docker compose --profile debug`.

#### `flower` (profil `flower`)

Interface web de monitoring Celery (port 5555). Permet de visualiser l'etat des workers et des taches en cours. Active avec `docker compose --profile flower`.

### Lancement

```bash
docker compose up --build -d
```

L'interface Airflow est accessible sur `http://localhost:8080` (login: valeurs de `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`).

L'interface de Minio est accessible sur `http://localhost:9001` (login: valeurs de `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).

## DAG : `fashion_store_data_injection`

Le DAG (`dags/test_artefact/main.py`) orchestre l'injection quotidienne des donnees de vente.

### Flux

1. `validate_execution_date` -- valide le format de la date
2. `check_date_exists` -- verifie si les donnees existent deja en base (idempotence)
3. `extract_data_from_minio` -- extrait et filtre le CSV depuis MinIO
4. `load_data_to_postgres` -- insere les donnees normalisees en base
5. `send_notification` -- log le resultat

### Retour de valeur et XCom

Dans Airflow, chaque task est un processus isole. Elles ne partagent pas de memoire. Pour communiquer entre elles, Airflow utilise les **XCom** (cross-communication) : quand une task retourne une valeur (`return`), Airflow la serialise et la stocke en base. La task suivante peut alors la recuperer.

C'est analogue au modele requete/reponse HTTP : un client envoie une requete a un serveur et attend une reponse. Si le serveur ne renvoie rien, le client n'a rien a exploiter. De la meme maniere, si une task ne retourne pas de valeur, la task suivante n'a pas de donnees d'entree. C'est pourquoi chaque task du DAG retourne systematiquement une valeur, y compris `None` pour signaler qu'il n'y a rien a traiter (plutot que de lever une exception ou de ne rien retourner).

### Passage de la date via `context`

Le DAG accepte un parametre `date` via `params`. La task `validate_execution_date` le recupere depuis `context['params']` avec fallback sur `context['ds_nodash']` (date d'execution Airflow au format YYYYMMDD). Cela permet de declencher le DAG manuellement avec une date arbitraire ou de laisser Airflow fournir la date courante lors d'une execution planifiee.

## Exploration des donnees

L'analyse exploratoire est documentee dans `exploration.ipynb`. Elle aboutit a un schema normalise en 4 tables. Voir le notebook pour le detail des champs exclus (champs calcules et violations de formes normales).
