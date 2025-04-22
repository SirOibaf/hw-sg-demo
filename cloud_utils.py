import pandas as pd

def cloud_data_source(path):
    return pd.read_csv(path)

def cloud_save_features(features, path):
    features.to_csv(path, index=False)