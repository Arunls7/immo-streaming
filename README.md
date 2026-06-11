# Immo Streaming — Plateforme d'Analyse de Graphe Temps Réel

Projet d'ingénierie **Big Data & Analyse de Graphes Temps Réel**.
Simulation d'une plateforme immobilière type *LeBonCoin* : un flux infini
d'interactions utilisateurs est traité par **PySpark Structured Streaming**,
modélisé en **graphe de connexions** (GraphFrames) et visualisé en temps réel
via un **dashboard Streamlit**.

---

##  Architecture

```
┌──────────────┐    JSON     ┌────────────────────────┐   Parquet   ┌──────────────┐
│  generator   │ ─────────▶  │   PySpark Structured    │ ─────────▶  │  dashboard   │
│  (producteur)│   fichiers  │   Streaming + GraphFrames│   vertices  │  (Streamlit) │
└──────────────┘             └────────────────────────┘   edges     └──────────────┘
                                                            metrics
```

Le découplage par fichiers (JSON en entrée, Parquet en sortie) rend chaque
composant indépendant : on peut redémarrer le dashboard sans interrompre le
traitement de flux, et inversement.

---

## Structure du projet

```
immo_streaming/
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuration centralisée (chemins, fenêtres…)
├── src/
│   ├── __init__.py
│   ├── generator.py         # Producteur de flux JSON infini
│   ├── spark_pipeline.py    # Pipeline Structured Streaming + GraphFrames
│   └── dashboard.py         # Dashboard Streamlit dynamique
├── data/                    # Généré au runtime (non versionné)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Installation

```bash
# 1. Environnement virtuel (recommandé)
python -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\activate

# 2. Dépendances
pip install -r requirements.txt
```

> **Java requis** : PySpark nécessite un JDK (8, 11 ou 17). Vérifier avec
> `java -version`. Sous Ubuntu : `sudo apt install openjdk-17-jdk`.

---

## Lancement

Ouvrir **trois terminaux** à la racine du projet (`immo_streaming/`).

```bash
# Terminal 1 — Générateur de flux
python -m src.generator

# Terminal 2 — Pipeline Spark
python -m src.spark_pipeline

# Terminal 3 — Dashboard
streamlit run src/dashboard.py
```

Le dashboard s'ouvre sur <http://localhost:8501>. Le premier graphe apparaît
après le premier micro-batch (~5 s).

---

## Concepts PySpark mis en œuvre

| Concept | Implémentation | Fichier |
|---|---|---|
| **SparkSession** | Config mémoire + `shuffle.partitions=8` | `spark_pipeline.py` |
| **Schema Enforcement** | `EVENT_SCHEMA` imposé (pas d'inférence) | `spark_pipeline.py` |
| **Structured Streaming** | `readStream` sur source fichier | `spark_pipeline.py` |
| **Watermarking** | `withWatermark("timestamp", "2 minutes")` | `spark_pipeline.py` |
| **Fenêtre glissante** | `window(1 min, slide 30 s)` | `spark_pipeline.py` |
| **Output modes** | `Update` (agrégats) / `Append` (graphe) | `spark_pipeline.py` |
| **GraphFrames** | Vertices/Edges, degrés, composantes connectées | `spark_pipeline.py` |

Le générateur injecte volontairement **~5 % d'événements en retard** pour
démontrer concrètement le rôle du watermark.

---

## Modèle de graphe

- **Nœuds** : `USER` (🔵), `SELLER` (🟠), `PRODUCT` (🟢)
- **Arêtes orientées** :
  - `USER → PRODUCT` typée `AIME` / `VOUT` / `ACHAT`
  - `SELLER → PRODUCT` typée `PROPOSE`

---

##  Paramétrage

Tout se règle dans `config/settings.py` : débit du générateur, taille des
fenêtres, délai de watermark, cadence des micro-batches, etc.

---

##  Auteur
Kuganesan Arun
Projet réalisé dans le cadre du cours électif Spark & Big Data à CY Tech.
