import pytest
import os
from datetime import datetime
from dotenv import load_dotenv
from nasa_pipeline import NASADataPipeline
import pandas as pd
# Importamos tu clase (asegúrate de que el archivo se llame nasa_pipeline.py)
from nasa_pipeline import NASADataPipeline 
# 1. Definimos un "Fixture" (un objeto que usaremos en varios tests)
@pytest.fixture
def pipeline():
    return NASADataPipeline(api_key="fake_key")

# 2. Test de Transformación (El más importante)
def test_transform_data_structure(pipeline):
    # Simulamos un JSON de la NASA simplificado
    mock_raw_data = {
        "near_earth_objects": {
            "2026-03-26": [
                {
                    "id": "123",
                    "name": "Asteroide Test",
                    "is_potentially_hazardous_asteroid": True,
                    "close_approach_data": [{
                        "miss_distance": {"kilometers": "500000"},
                        "relative_velocity": {"kilometers_per_hour": "10000"}
                    }],
                    "estimated_diameter": {"meters": {"estimated_diameter_max": 100}}
                }
            ]
        }
    }
    
    df = pipeline.transform_data(mock_raw_data)
    
    # Verificaciones (Assertions)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]['name'] == "Asteroide Test"
    assert df.iloc[0]['hazard'] == True

# 3. Test de validación de columnas
def test_columns_presence(pipeline):
    mock_raw_data = {"near_earth_objects": {"2026-03-26": []}}
    df = pipeline.transform_data(mock_raw_data)
    
    expected_columns = ['id', 'name', 'hazard', 'distance_km', 'velocity_kmh', 'diameter_max_m']
    for col in expected_columns:
        assert col in df.columns
