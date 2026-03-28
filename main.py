import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from nasa_pipeline import NASADataPipeline

def main():
    # 1. Cargar configuración de entorno
    load_dotenv()
    api_key = os.getenv("NASA_API_KEY")

    if not api_key:
        print("ERROR: No se encontró la NASA_API_KEY en el archivo .env")
        sys.exit(1)

    # 2. Inicializar el componente de ingeniería
    pipeline = NASADataPipeline(api_key)

    # 3. Definir ventana de tiempo (Ejemplo: Hoy)
    # Puedes cambiar esto para que reciba argumentos por consola en el futuro
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Iniciando Data Pipeline Astronómico")
    print(f"Fecha: {today}")

    try:
        # FASE E: Extract
        print("Extrayendo datos desde NASA NeoWs API...")
        raw_data = pipeline.fetch_data(start_date=today, end_date=today)

        # FASE T: Transform
        print("Transformando datos crudos a formato tabular...")
        df = pipeline.transform_data(raw_data)

        # FASE L: Load (Persistencia)
        if not df.empty:
            output_file = f"asteroides_{today}.csv"
            pipeline.save_to_csv(df, filename=output_file)
            
            # Resumen técnico para la consola
            print(f"Objetos detectados: {len(df)}")
            print(f"Objetos potencialmente peligrosos: {df['hazard'].sum()}")
            print(f"Archivo generado: {output_file}")
            
            # Mostrar los 3 más cercanos (opcional)
            print("\nLos 3 objetos más cercanos (KM):")
            print(df.nsmallest(3, 'distance_km')[['name', 'distance_km']])
        else:
            print(" No se encontraron objetos cercanos para la fecha seleccionada.")

    except Exception as e:
        print(f"ERROR CRÍTICO durante la ejecución: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()