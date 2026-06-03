# Documentation du Projet DataEng_ML_Spark

Ce projet vise à mettre en place un pipeline complet d'ingénierie de données utilisant Apache Spark.

## Objectifs du Projet
- **Ingestion de données** : Récupération de données brutes depuis diverses sources.
- **Transformation de données** : Nettoyage et agrégation à l'aide de Spark.
- **Déploiement** : Mise à disposition des résultats transformés.

## Structure du Projet
- `data/` : Contient les données (brutes, transformées, finales).
- `src/` : Code source des jobs Spark.
- `tests/` : Tests unitaires et d'intégration.
- `config/` : Fichiers de configuration (YAML, JSON).
- `docs/` : Documentation supplémentaire.

## Spécifications Techniques
- **Langage** : Python (PySpark).
- **Framework** : Apache Spark 3.x.
- **Gestion des dépendances** : `requirements.txt`.

## Feuille de Route des Tâches
Les tâches doivent être regroupées en phases:
### Phase 1 : Création du script Spark
-  dans un NB Jupyter 
-  sur un échantillon 
-  pour générer une table CSV de sortie 
### Phase 2 : Rendre le script paramétrable
- ajout des paramètres au script
  - période de DATE_START à DATE_STOP
- écriture de la table sur le Cloud
  - générer CSV sur DESTINATION
### Phase 3 : Test du script
  - lancer cluster Hadoop
  - exécuter job sur période de 2 semaines à cheval sur 2 mois
