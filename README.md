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

### LOG collecter donnéesursaud : PROBLEME DE DROITS
PermissionError: [Errno 13] Permission denied: '/data/ars/raw'
[2026-04-13, 15:22:14 UTC] {taskinstance.py:1138} INFO - Marking task as UP_FOR_RETRY. dag_id=ars_epidemio_dag, task_id=collecte.collecter_donnees_sursaud, execution_date=20240122T060000, start_date=20260413T152212, end_date=20260413T152214
[2026-04-13, 15:22:14 UTC] {standard_task_runner.py:107} ERROR - Failed to execute job 16 for task collecte.collecter_donnees_sursaud ([Errno 13] Permission denied: '/data/ars/raw'; 1068)
[2026-04-13, 15:22:14 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 1
[2026-04-13, 15:22:14 UTC] {taskinstance.py:3281} INFO - 0 downstream tasks scheduled from follow-on schedule check
