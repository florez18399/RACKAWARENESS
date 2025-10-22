import os
from pyspark.sql import SparkSession
# ¡Importante! Añadimos 'lit' para crear una columna con un valor literal
from pyspark.sql.functions import col, year, month, dayofmonth, hour, lit

# --- Configuración ---
# Leemos la zona desde una variable de entorno que pasará Docker
ZONA_ID = os.environ.get("ZONA_ID", "zona_desconocida")

HDFS_PATH = "hdfs://namenode:9000"
OUTPUT_PATH = f"{HDFS_PATH}/data/trazas_parquet"
CHECKPOINT_PATH = f"{HDFS_PATH}/checkpoints/trazas_{ZONA_ID}" # Checkpoint único por zona

def main():
    print(f"Iniciando sesión de Spark en [ZONA: {ZONA_ID}]")
    spark = (
        SparkSession.builder.appName(f"StreamingTrazas-{ZONA_ID}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. GENERAR TRAZAS (Fuente de datos)
    df_trazas = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    # 2. PROCESAR (Añadir particiones Y LA ZONA)
    df_particionado = (
        df_trazas.withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        # ¡Aquí está la parametrización!
        .withColumn("zona", lit(ZONA_ID)) 
    )

    # 3. ESCRIBIR (Almacenamiento en HDFS)
    print(f"Iniciando stream para [ZONA: {ZONA_ID}]")
    print(f"  -> Output: {OUTPUT_PATH}")
    print(f"  -> Checkpoint: {CHECKPOINT_PATH}")
    
    query = (
        df_particionado.writeStream
        .format("parquet")
        .outputMode("append")
        .partitionBy("year", "month", "day", "hour", "zona")
        .option("path", OUTPUT_PATH)  # <--- ESTA ES LA FORMA CORRECTA
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print(f"Stream para [ZONA: {ZONA_ID}] iniciado.")
    query.awaitTermination()

if __name__ == "__main__":
    main()