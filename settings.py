"""
config/settings.py
==================
Configuration centralisée du projet.

Centraliser les paramètres ici (plutôt que de les disperser dans le code)
permet de modifier le comportement du pipeline sans toucher à la logique
métier — pratique pour ajuster le débit, les fenêtres ou les chemins lors
des tests de performance.
"""

from pathlib import Path

# ─── Arborescence du projet ───────────────────────────────────────────────────
# On résout les chemins à partir de la racine du projet pour que les scripts
# fonctionnent quel que soit le dossier depuis lequel ils sont lancés.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT    = PROJECT_ROOT / "data"

# Dossier surveillé par Spark (le générateur y dépose ses fichiers JSON)
STREAM_INPUT_DIR = DATA_ROOT / "stream_data"

# Sorties du pipeline (consommées par le dashboard)
GRAPH_DATA_DIR   = DATA_ROOT / "graph_data"
VERTICES_PATH    = GRAPH_DATA_DIR / "vertices"
EDGES_PATH       = GRAPH_DATA_DIR / "edges"
DEGREES_PATH     = GRAPH_DATA_DIR / "degrees"
COMPONENTS_PATH  = GRAPH_DATA_DIR / "components"
METRICS_PATH     = GRAPH_DATA_DIR / "metrics"     # métriques agrégées (fenêtres)

# Checkpoints Spark (obligatoires pour la reprise sur incident)
CHECKPOINT_DIR   = DATA_ROOT / "checkpoints"
CHECKPOINT_AGG   = CHECKPOINT_DIR / "agg"
CHECKPOINT_GRAPH = CHECKPOINT_DIR / "graph"
CHECKPOINT_GF    = CHECKPOINT_DIR / "graphframes"

# ─── Paramètres du générateur ─────────────────────────────────────────────────
EVENTS_PER_SECOND = 5        # débit simulé (événements/seconde)
MAX_FILES         = 300      # rotation : nombre max de fichiers conservés
LATE_EVENT_PROB   = 0.05     # proba d'injecter un événement "en retard"
LATE_EVENT_DELAY  = 90       # retard simulé (secondes) pour tester le watermark

# Volumétrie des entités simulées
NB_USERS    = 500
NB_SELLERS  = 80
NB_PRODUCTS = 300

# ─── Paramètres Spark Streaming ───────────────────────────────────────────────
WINDOW_DURATION   = "1 minute"      # taille de la fenêtre glissante
SLIDE_DURATION    = "30 seconds"    # pas de glissement
WATERMARK_DELAY   = "2 minutes"     # tolérance aux retards avant purge des états
TRIGGER_INTERVAL  = "5 seconds"     # cadence des micro-batches
MAX_FILES_PER_TRIGGER = 20          # back-pressure : limite le débit d'ingestion
SHUFFLE_PARTITIONS    = "8"         # adapté à un cluster local (≈ nb de cœurs)

# Version GraphFrames (doit correspondre à la version de Spark)
GRAPHFRAMES_PACKAGE = "graphframes:graphframes:0.8.3-spark3.5-s_2.12"

# ─── Paramètres du dashboard ──────────────────────────────────────────────────
REFRESH_SEC      = 5     # rafraîchissement automatique (secondes)
MAX_NODES_DISP   = 100   # plafond d'affichage des nœuds (fluidité du rendu)


def ensure_directories() -> None:
    """Crée tous les dossiers de données s'ils n'existent pas encore."""
    for path in (STREAM_INPUT_DIR, GRAPH_DATA_DIR, CHECKPOINT_DIR):
        path.mkdir(parents=True, exist_ok=True)
