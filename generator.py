"""
src/generator.py
================
Producteur de flux infini d'événements immobiliers (style LeBonCoin).

Le générateur écrit un fichier JSON par événement dans le dossier surveillé
par Spark Structured Streaming (source `file`). On a préféré la source fichier
au socket TCP car elle est plus robuste (pas de perte de connexion) et permet
à Spark de gérer nativement le checkpointing et la reprise sur incident.

Particularité : ~5 % des événements sont volontairement injectés "en retard"
(timestamp antérieur). Cela permet de démontrer concrètement l'utilité du
watermark côté pipeline.

Lancer AVANT le pipeline Spark :
    python -m src.generator
"""

import json
import random
import signal
import sys
import time
from datetime import datetime, timezone, timedelta

from config import settings

# ─── Données de référence ─────────────────────────────────────────────────────
VILLES = [
    "Paris", "Lyon", "Marseille", "Bordeaux", "Nantes",
    "Toulouse", "Lille", "Nice", "Strasbourg", "Montpellier",
]

CATEGORIES = [
    "Appartement", "Maison", "Studio", "Loft",
    "Terrain", "Parking", "Local commercial", "Colocation",
]

ACTIONS       = ["AIME", "VOUT", "ACHAT"]
POIDS_ACTIONS = [0.60, 0.30, 0.10]   # un achat est rare comparé à un like

# Fourchettes de prix réalistes par catégorie (en €)
PRIX_MIN_MAX = {
    "Appartement":      (80_000,    600_000),
    "Maison":           (120_000,   900_000),
    "Studio":           (50_000,    250_000),
    "Loft":             (100_000,   700_000),
    "Terrain":          (20_000,    400_000),
    "Parking":          (5_000,     40_000),
    "Local commercial": (60_000,    1_200_000),
    "Colocation":       (300,       1_500),     # loyer mensuel
}

# ─── Arrêt propre sur Ctrl+C ──────────────────────────────────────────────────
_RUNNING = True


def _handle_sigint(signum, frame):
    global _RUNNING
    _RUNNING = False
    print("\n[Générateur] Arrêt demandé, fermeture propre…")


signal.signal(signal.SIGINT, _handle_sigint)


# ══════════════════════════════════════════════════════════════════════════════
def generate_event() -> dict:
    """Génère un événement aléatoire cohérent (prix corrélé à la catégorie)."""
    cat          = random.choice(CATEGORIES)
    p_min, p_max = PRIX_MIN_MAX[cat]
    price        = round(random.uniform(p_min, p_max), 2)

    # Injection contrôlée d'événements en retard pour tester le watermark
    if random.random() < settings.LATE_EVENT_PROB:
        event_time = datetime.now(timezone.utc) - timedelta(
            seconds=settings.LATE_EVENT_DELAY
        )
    else:
        event_time = datetime.now(timezone.utc)

    return {
        "timestamp":   event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id":     f"usr_{random.randint(1, settings.NB_USERS):04d}",
        "user_city":   random.choice(VILLES),
        "product_id":  f"prod_{random.randint(1, settings.NB_PRODUCTS):04d}",
        "product_cat": cat,
        "seller_id":   f"sel_{random.randint(1, settings.NB_SELLERS):04d}",
        "action_type": random.choices(ACTIONS, weights=POIDS_ACTIONS, k=1)[0],
        "price":       price,
    }


def rotate_old_files(current_index: int) -> None:
    """Supprime les anciens fichiers pour ne pas saturer le disque."""
    old_index = current_index - settings.MAX_FILES
    if old_index < 0:
        return
    old_file = settings.STREAM_INPUT_DIR / f"events_{old_index:06d}.json"
    try:
        if old_file.exists():
            old_file.unlink()
    except OSError as exc:
        print(f"[Générateur] Avertissement : suppression de {old_file} échouée ({exc})")


def main() -> None:
    settings.ensure_directories()
    interval = 1 / settings.EVENTS_PER_SECOND
    file_index = 0

    print(f"[Générateur] Démarrage")
    print(f"  Dossier de sortie : {settings.STREAM_INPUT_DIR}")
    print(f"  Débit             : {settings.EVENTS_PER_SECOND} évt/s")
    print(f"  Ctrl+C pour arrêter.\n")

    while _RUNNING:
        filename = settings.STREAM_INPUT_DIR / f"events_{file_index:06d}.json"
        event    = generate_event()

        # Écriture atomique : on écrit dans un .tmp puis on renomme.
        # Cela évite que Spark lise un fichier à moitié écrit.
        tmp_file = filename.with_suffix(".tmp")
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
            tmp_file.rename(filename)
        except OSError as exc:
            print(f"[Générateur] Erreur d'écriture : {exc}", file=sys.stderr)
            time.sleep(interval)
            continue

        print(f"  → {event['action_type']:5s} | {event['user_id']} | "
              f"{event['product_cat']:18s} | {event['price']:>12.2f} €")

        file_index += 1
        rotate_old_files(file_index)
        time.sleep(interval)

    print("[Générateur] Terminé.")


if __name__ == "__main__":
    main()
