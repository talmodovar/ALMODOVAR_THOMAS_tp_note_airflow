## Auteur
- Nom : ALMODOVAR
- Prénom : Thomas
- Formation : MIA 26.2
- Date : 13 avril 26

## Prérequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11 (pour les tests locaux)

## Instructions de déploiement

### 1. Démarrage de la stack
```bash
cd ars-epidemio
docker compose up -d
docker compose ps
```

### 2. Configuration des connexions
Airflow sur 'http://localhost:8080' (admin/admin).
Flower sur 'http://localhost:5555/'

### 3. Démarrage du pipeline
 DAG `ars_epidemio_dag`.


## Architecture des données
Les données brutes sont archivées dans un volume Docker
BDD :  PostgreSQL 


## Difficultés rencontrées et solutions
Ajustement des retries car trop long 1 retry avec un waiting de 30sec
