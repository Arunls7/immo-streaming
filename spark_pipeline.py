"""
src/spark_pipeline.py
=====================
Pipeline PySpark Structured Streaming — Projet Immobilier (style LeBonCoin).

Le pipeline lance DEUX requêtes streaming en parallèle :

  1. Agrégation par fenêtre temporelle  → persistée en Parquet (métriques)
  2. Construction incrémentale du graphe → GraphFrames + persistance Parquet

Concepts du cahier des charges couverts :
  - SparkSession configurée (mémoire, shuffle)
  - Schema Enforcement (schéma strict, pas d'inférence)
  - Structured Streaming (source fichier)
  - Watermarking (withWatermark) pour gérer les retards
  - Fenêtre glissante (Sliding Window)
  - Output modes justifiés (Update pour les agrégations)
  - GraphFrames (vertices/edges, degrés, composantes connectées)

Lancer APRÈS le générateur :
    python -m src.spark_pipeline
"""

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from config import settings

# ── GraphFrames : dépendance externe chargée via spark.jars.packages ─────────
# On gère son absence proprement pour que le pipeline reste fonctionnel
# (calcul des degrés en SQL pur) même si le package n'a pas pu être téléchargé.
try:
    from graphframes import GraphFrame
    GRAPHFRAMES_AVAILABLE = True
except ImportError:
    GRAPHFRAMES_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# 1. SparkSession
# ══════════════════════════════════════════════════════════════════════════════
def build_spark_session() -> SparkSession:
    """
    Initialise la SparkSession, point d'entrée unique de l'application.

    Choix de configuration :
      - shuffle.partitions réduit à 8 : la valeur par défaut (200) est
        surdimensionnée pour un cluster local et génère un overhead inutile
        de petites tâches sur un flux à faible volumétrie.
      - driver/executor memory à 2g : suffisant pour les états de fenêtre
        et les calculs GraphFrames sur la volumétrie simulée.
    """
    spark = (
        SparkSession.builder
        .appName("ImmoStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", settings.SHUFFLE_PARTITIONS)
        .config("spark.jars.packages", settings.GRAPHFRAMES_PACKAGE)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(str(settings.CHECKPOINT_GF))
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2. Schéma strict (Schema Enforcement)
# ══════════════════════════════════════════════════════════════════════════════
# Imposer le schéma évite à Spark de scanner les fichiers pour inférer les types
# (opération coûteuse et incompatible avec un flux infini). Tout fichier non
# conforme produira des valeurs nulles plutôt que de faire planter le job.
EVENT_SCHEMA = StructType([
    StructField("timestamp",   TimestampType(), False),
    StructField("user_id",     StringType(),    False),
    StructField("user_city",   StringType(),    True),
    StructField("product_id",  StringType(),    False),
    StructField("product_cat", StringType(),    True),
    StructField("seller_id",   StringType(),    False),
    StructField("action_type", StringType(),    False),
    StructField("price",       DoubleType(),    True),
])


# ══════════════════════════════════════════════════════════════════════════════
# 3. Lecture du flux
# ══════════════════════════════════════════════════════════════════════════════
def read_stream(spark: SparkSession) -> DataFrame:
    """
    Consomme les fichiers JSON déposés par le générateur.

    maxFilesPerTrigger agit comme un mécanisme de back-pressure : il borne
    le nombre de fichiers ingérés par micro-batch et évite qu'un retard
    d'accumulation ne sature la mémoire au redémarrage.
    """
    return (
        spark.readStream
        .schema(EVENT_SCHEMA)                       # schéma imposé
        .option("maxFilesPerTrigger", settings.MAX_FILES_PER_TRIGGER)
        .json(str(settings.STREAM_INPUT_DIR))
    )


# ══════════════════════════════════════════════════════════════════════════════
# 4. Agrégation : Watermark + Fenêtre glissante
# ══════════════════════════════════════════════════════════════════════════════
def build_aggregation(raw_df: DataFrame) -> DataFrame:
    """
    Agrège les volumes d'actions par fenêtre glissante, catégorie et type.

    withWatermark("timestamp", "2 minutes") :
        Spark accepte les événements arrivant jusqu'à 2 min après leur heure
        théorique, puis purge automatiquement les états de fenêtre plus anciens.
        Sans cela, l'état de streaming croîtrait indéfiniment (fuite mémoire
        sur un flux infini).

    Fenêtre glissante (1 min, pas de 30 s) :
        Chaque événement appartient à plusieurs fenêtres qui se chevauchent,
        ce qui lisse les courbes de tendance affichées sur le dashboard.
    """
    return (
        raw_df
        .withWatermark("timestamp", settings.WATERMARK_DELAY)
        .groupBy(
            F.window("timestamp", settings.WINDOW_DURATION, settings.SLIDE_DURATION),
            "product_cat",
            "action_type",
        )
        .agg(
            F.count("*").alias("nb_actions"),
            F.round(F.avg("price"), 2).alias("prix_moyen"),
            F.round(F.sum("price"), 2).alias("volume_total"),
            # NB : countDistinct() est interdit sur un DataFrame streaming.
            # Spark impose approx_count_distinct (algorithme HyperLogLog),
            # qui est de toute façon le bon choix à grande échelle.
            F.approx_count_distinct("user_id").alias("nb_users_uniques"),
            F.approx_count_distinct("seller_id").alias("nb_vendeurs_actifs"),
        )
        # On aplatit la struct window en deux colonnes lisibles
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_cat",
            "action_type",
            "nb_actions",
            "prix_moyen",
            "volume_total",
            "nb_users_uniques",
            "nb_vendeurs_actifs",
        )
    )


def write_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Persiste les métriques de fenêtre en Parquet pour le dashboard."""
    if batch_df.rdd.isEmpty():
        return
    (batch_df.coalesce(1)
     .write.mode("overwrite")
     .parquet(str(settings.METRICS_PATH)))


# ══════════════════════════════════════════════════════════════════════════════
# 5. Construction du graphe (foreachBatch)
# ══════════════════════════════════════════════════════════════════════════════
def build_graph_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Reconstruit le graphe à chaque micro-batch puis calcule ses indicateurs.

    On utilise foreachBatch car GraphFrames opère sur des DataFrames statiques
    (batch) et n'est pas directement compatible avec l'API streaming. Chaque
    micro-batch est donc traité comme un mini-job batch.
    """
    if batch_df.rdd.isEmpty():
        return

    # ── Vertices : trois types de nœuds (USER / SELLER / PRODUCT) ─────────────
    users = batch_df.select(
        F.col("user_id").alias("id"),
        F.lit("USER").alias("type"),
        F.col("user_city").alias("label"),
    ).distinct()

    sellers = batch_df.select(
        F.col("seller_id").alias("id"),
        F.lit("SELLER").alias("type"),
        F.col("seller_id").alias("label"),
    ).distinct()

    products = batch_df.select(
        F.col("product_id").alias("id"),
        F.lit("PRODUCT").alias("type"),
        F.col("product_cat").alias("label"),
    ).distinct()

    vertices = users.unionByName(sellers).unionByName(products).distinct()

    # ── Edges : User→Product (action) et Seller→Product (PROPOSE) ─────────────
    user_to_product = batch_df.select(
        F.col("user_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.col("action_type").alias("relationship"),
    )

    seller_to_product = batch_df.select(
        F.col("seller_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.lit("PROPOSE").alias("relationship"),
    ).distinct()

    edges = user_to_product.unionByName(seller_to_product)

    # ── Persistance pour le dashboard ─────────────────────────────────────────
    (vertices.coalesce(1).write.mode("overwrite")
     .parquet(str(settings.VERTICES_PATH)))
    (edges.coalesce(1).write.mode("overwrite")
     .parquet(str(settings.EDGES_PATH)))

    nb_v, nb_e = vertices.count(), edges.count()

    # ── Indicateurs de graphe ──────────────────────────────────────────────────
    if GRAPHFRAMES_AVAILABLE:
        gf = GraphFrame(vertices, edges)

        # Degré total de chaque nœud (centralité de degré)
        (gf.degrees.coalesce(1).write.mode("overwrite")
         .parquet(str(settings.DEGREES_PATH)))

        # Composantes connectées : identifie les sous-réseaux isolés
        components = gf.connectedComponents()
        (components.coalesce(1).write.mode("overwrite")
         .parquet(str(settings.COMPONENTS_PATH)))

        nb_components = components.select("component").distinct().count()
        print(f"[Batch {batch_id}] V={nb_v} E={nb_e} | "
              f"composantes connectées = {nb_components}")
    else:
        # Fallback : degré calculé en SQL pur si GraphFrames indisponible
        deg_out = edges.groupBy(F.col("src").alias("id")).count() \
                       .withColumnRenamed("count", "outDegree")
        deg_in  = edges.groupBy(F.col("dst").alias("id")).count() \
                       .withColumnRenamed("count", "inDegree")
        degrees = deg_out.join(deg_in, "id", "outer").fillna(0)
        (degrees.coalesce(1).write.mode("overwrite")
         .parquet(str(settings.DEGREES_PATH)))
        print(f"[Batch {batch_id}] V={nb_v} E={nb_e} | "
              f"(GraphFrames absent — degrés calculés en SQL)")


# ══════════════════════════════════════════════════════════════════════════════
# 6. Orchestration
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    settings.ensure_directories()
    spark = build_spark_session()

    if not GRAPHFRAMES_AVAILABLE:
        print("[Pipeline] GraphFrames non importable côté Python — "
              "le pipeline utilisera le fallback SQL pour les degrés.",
              file=sys.stderr)

    raw_df = read_stream(spark)

    # ── Requête 1 : agrégations de fenêtre → Parquet (mode Update) ───────────
    # Output mode UPDATE : seules les fenêtres dont l'agrégat a changé depuis
    # le dernier batch sont émises. C'est le mode adapté aux agrégations avec
    # watermark — plus économe que Complete (qui réémet tout l'état).
    query_metrics = (
        build_aggregation(raw_df).writeStream
        .outputMode("update")
        .foreachBatch(write_metrics_batch)
        .trigger(processingTime=settings.TRIGGER_INTERVAL)
        .option("checkpointLocation", str(settings.CHECKPOINT_AGG))
        .start()
    )

    # ── Requête 2 : graphe → foreachBatch (mode Append) ──────────────────────
    # Output mode APPEND : on traite chaque micro-batch comme un lot de
    # nouvelles lignes, sans agrégation d'état côté streaming (l'état est
    # reconstruit dans build_graph_batch).
    query_graph = (
        raw_df.writeStream
        .outputMode("append")
        .foreachBatch(build_graph_batch)
        .trigger(processingTime=settings.TRIGGER_INTERVAL)
        .option("checkpointLocation", str(settings.CHECKPOINT_GRAPH))
        .start()
    )

    print("[Pipeline] Deux requêtes streaming actives. Ctrl+C pour arrêter.\n")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[Pipeline] Arrêt demandé, fermeture des requêtes…")
        query_metrics.stop()
        query_graph.stop()
        spark.stop()
        print("[Pipeline] Terminé proprement.")


if __name__ == "__main__":
    main()
