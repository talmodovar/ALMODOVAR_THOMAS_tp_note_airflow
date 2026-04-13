import logging
import os
import shutil
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Configuration du dossier des scripts
import sys
sys.path.append("/opt/airflow/scripts")
try:
    from collecte_ias import get_semaine_iso
except ImportError:
    logging.error("Import get_semaine_iso échoué")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline de surveillance épidémiologique ARS Occitanie (Conformité Section 6)",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:

    def collecter_donnees_ias(**context) -> str:
        """Télécharge les CSV IAS® et retourne le chemin du fichier JSON créé."""
        semaine = (
            f"{context['execution_date'].year}-"
            f"S{context['execution_date'].isocalendar()[1]:02d}"
        )
        archive_path = Variable.get("archive_base_path", default_var="/data/ars")
        output_dir = f"{archive_path}/raw"
        sys.path.insert(0, "/opt/airflow/scripts")
        from collecte_ias import (
            DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
            agreger_semaine, sauvegarder_donnees
        )
        resultats = {}
        for syndrome, url in DATASETS_IAS.items():
            rows_all = telecharger_csv_ias(url)
            rows_sem = filtrer_semaine(rows_all, semaine)
            resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
        return sauvegarder_donnees(resultats, semaine, output_dir)

    # 1. Initialisation Base de Données
    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql", # Fichier SQL séparé dans le dossier dags/sql/
        autocommit=True,
    )

    # Configurations Docker communes (conformité Sections 4 & 6)
    docker_common = {
        "image": "apache/airflow:2.8.0",
        "api_version": 'auto',
        "auto_remove": True,
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "ars-epidemio_default",
        "mounts": [
            Mount(source="c:\\Users\\Thomas\\Documents\\IPSSI\\Airflow\\ALMODOVAR_airflow_tp_note\\ars-epidemio\\scripts", target="/opt/airflow/scripts", type="bind"),
            Mount(source="ars-epidemio_ars-data", target="/data/ars", type="volume"),
            Mount(source="c:\\Users\\Thomas\\Documents\\IPSSI\\Airflow\\ALMODOVAR_airflow_tp_note\\ars-epidemio\\output", target="/opt/airflow/output", type="bind")
        ],
        "environment": {
            "AIRFLOW_CONN_POSTGRES_ARS": Variable.get("postgres_ars_url", default_var="postgresql://postgres:postgres@postgres-ars:5432/ars_epidemio"),
            "AIRFLOW_VAR_METEO_VAGUE_FROID": "{{ var.value.meteo_vague_froid | default('False') }}",
            "AIRFLOW_CTX_EXECUTION_DATE": "{{ ts }}"
        }
    }

    with TaskGroup("collecte") as tg_collecte:
        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias,
            provide_context=True,
        )

    # 3. Groupe Persistance Brute
    def archiver_local(**context) -> str:
        """Organise le fichier brut dans la structure d'archivage partitionnée."""
        semaine = (
            f"{context['execution_date'].year}-"
            f"S{context['execution_date'].isocalendar()[1]:02d}"
        )
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]
        # Récupération du chemin depuis XCom
        chemin_source = context["task_instance"].xcom_pull(
            task_ids="collecte.collecter_donnees_sursaud"
        )
        archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
        os.makedirs(archive_dir, exist_ok=True)
        chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"
        shutil.copy2(chemin_source, chemin_dest)
        print(f"ARCHIVE_OK:{chemin_dest}")
        return chemin_dest

    def verifier_archive(**context) -> bool:
        """Vérifie que le fichier d'archive existe et n'est pas vide."""
        semaine = (
            f"{context['execution_date'].year}-"
            f"S{context['execution_date'].isocalendar()[1]:02d}"
        )
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]
        chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
        if not os.path.exists(chemin):
            raise FileNotFoundError(f"Archive manquante : {chemin}")
        taille = os.path.getsize(chemin)
        if taille == 0:
            raise ValueError(f"Archive vide : {chemin}")
        print(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
        return True

    with TaskGroup("persistance_brute") as tg_persistance_brute:
        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
            provide_context=True,
        )
        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
            provide_context=True,
        )
        archiver >> verifier

    def calculer_indicateurs_ias(**context):
        """Calcule les indicateurs épidémiques à partir des données brutes."""
        semaine = (
            f"{context['execution_date'].year}-"
            f"S{context['execution_date'].isocalendar()[1]:02d}"
        )
        # Chemin du fichier brut (via XCom de l'archivage)
        raw_path = context["task_instance"].xcom_pull(task_ids="persistance_brute.archiver_local")
        output_dir = "/data/ars/indicateurs"
        output_path = f"{output_dir}/indicateurs_{semaine}.json"
        
        sys.path.insert(0, "/opt/airflow/scripts")
        from calcul_indicateurs import run_calcul_indicateurs
        run_calcul_indicateurs(semaine, raw_path, output_path)
        return output_path

    with TaskGroup("traitement") as tg_traitement:
        calculer_indicateurs_epidemiques = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_ias,
            provide_context=True,
        )

    def inserer_donnees_postgres(**context) -> None:
        """Lit les fichiers JSON (bruts et calculés) et les insère dans Postgres."""
        ti = context["task_instance"]
        raw_json_path = ti.xcom_pull(task_ids="collecte.collecter_donnees_sursaud")
        indicators_json_path = ti.xcom_pull(task_ids="traitement.calculer_indicateurs_epidemiques")
        
        pg_hook = PostgresHook(postgres_conn_id="postgres_ars")
        
        # 1. Insertion des données hebdomadaires (brutes)
        if raw_json_path and os.path.exists(raw_json_path):
            with open(raw_json_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            
            semaine = raw_data.get("semaine")
            for syndrome, m in raw_data.get("syndromes", {}).items():
                sql = """
                    INSERT INTO donnees_hebdomadaires 
                    (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (semaine, syndrome) DO UPDATE SET
                    valeur_ias = EXCLUDED.valeur_ias,
                    seuil_min_saison = EXCLUDED.seuil_min_saison,
                    seuil_max_saison = EXCLUDED.seuil_max_saison,
                    nb_jours_donnees = EXCLUDED.nb_jours_donnees,
                    updated_at = CURRENT_TIMESTAMP;
                """
                params = (semaine, syndrome, m.get("valeur_ias"), m.get("seuil_min"), m.get("seuil_max"), m.get("nb_jours"))
                pg_hook.run(sql, parameters=params)
            logger.info(f"Données hebdomadaires insérées dépuis {raw_json_path}")

        # 2. Insertion des indicateurs épidémiques (calculés)
        if indicators_json_path and os.path.exists(indicators_json_path):
            with open(indicators_json_path, "r", encoding="utf-8") as f:
                indicators = json.load(f)
            
            for ind in indicators:
                sql = """
                    INSERT INTO indicateurs_epidemiques 
                    (semaine, syndrome, valeur_ias, z_score, r0_estime, nb_saisons_reference, statut)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (semaine, syndrome) DO UPDATE SET
                    valeur_ias = EXCLUDED.valeur_ias,
                    z_score = EXCLUDED.z_score,
                    r0_estime = EXCLUDED.r0_estime,
                    nb_saisons_reference = EXCLUDED.nb_saisons_reference,
                    statut = EXCLUDED.statut,
                    updated_at = CURRENT_TIMESTAMP;
                """
                # Note: taux_incidence dans le JSON correspond à valeur_ias dans la table
                params = (
                    ind.get("semaine"), ind.get("syndrome"), ind.get("taux_incidence"),
                    ind.get("z_score"), ind.get("r0_estime"), ind.get("nb_annees_reference"),
                    ind.get("statut")
                )
                pg_hook.run(sql, parameters=params)
            logger.info(f"Indicateurs épidémiques insérés depuis {indicators_json_path}")

    with TaskGroup("persistance_operationnelle") as tg_persistance_op:
        inserer = PythonOperator(task_id="inserer_donnees_postgres", python_callable=inserer_donnees_postgres)

    # 6. Évaluation Situation (Branching)
    def evaluer_situation_epidemique(**context) -> List[str]:
        semaine = get_semaine_iso(context['execution_date'].date())
        pg_hook = PostgresHook(postgres_conn_id="POSTGRES_ARS")
        res = pg_hook.get_first("SELECT statut FROM indicateurs_epidemiques WHERE semaine = %s AND statut != 'NORMAL' LIMIT 1", (semaine,))
        if res:
            if res[0] == "URGENCE": return ["declencher_alerte_ars"]
            return ["envoyer_bulletin_surveillance"]
        return ["confirmer_situation_normale"]

    t_evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique
    )

    t_alerte = PythonOperator(task_id="declencher_alerte_ars", python_callable=lambda: logger.warning("ALERTE URGENCE!"))
    t_bulletin = PythonOperator(task_id="envoyer_bulletin_surveillance", python_callable=lambda: logger.info("Bulletin envoyé."))
    t_normale = PythonOperator(task_id="confirmer_situation_normale", python_callable=lambda: logger.info("Situation normale."))

    # 7. Rapport Hebdomadaire
    generer_rapport_hebdomadaire = DockerOperator(
        task_id="generer_rapport_hebdomadaire",
        command="python3 /opt/airflow/scripts/calcul_indicateurs.py rapport",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        **docker_common
    )

    # Définition des dépendances (Schéma 6.1)
    init_base_donnees >> tg_collecte >> tg_persistance_brute >> tg_traitement >> tg_persistance_op >> t_evaluer
    t_evaluer >> [t_alerte, t_bulletin, t_normale] >> generer_rapport_hebdomadaire
