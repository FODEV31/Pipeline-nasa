import requests
import pandas as pd
from datetime import datetime

class NASADataPipeline:
    def __init__(self, api_key: str):
        self.base_url = "https://api.nasa.gov/neo/rest/v1/feed"
        self.api_key = api_key

    def fetch_data(self, start_date: str, end_date: str)-> dict:
        params = {
        "start_date": start_date,
        "end_date": end_date,
        "api_key": self.api_key
        }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()

    def transform_data(self, raw_data: dict) -> pd.DataFrame:
        processed_list = []
        columns = ['id', 'name', 'hazard', 'distance_km', 'velocity_kmh', 'diameter_max_m']
        near_earth_objects = raw_data.get("near_earth_objects",{})
        for date in near_earth_objects:
            for item in near_earth_objects[date]:
                processed_list.append({
                    "id": item["id"],
                    "name": item["name"],
                    "hazard": item["is_potentially_hazardous_asteroid"],
                    "distance_km": float(item["close_approach_data"][0]["miss_distance"]["kilometers"]),
                    "velocity_kmh": float(item["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]),
                    "diameter_max_m":item["estimated_diameter"]["meters"]["estimated_diameter_max"]
                })
            if not processed_list:
                return pd.DataFrame(columns=columns)
            return pd.DataFrame(processed_list)
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "asteroides_cercanos.csv"):
        """Guarda el DataFrame en un archivo CSV para análisis posterior."""
        try:
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Datos guardados exitosamente en {filename}")
        except Exception as e:
            print(f"❌ Error al guardar los datos: {e}")