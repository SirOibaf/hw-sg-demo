def get_feature_store_handle():
    import hopsworks

    project = hopsworks.login()
    return project.get_feature_store()

def hopsworks_data_source():
    fs = get_feature_store_handle()
    connector = fs.get_storage_connector("demo_connector")

    return connector.read(path="transaction_raw/transactions.csv", data_format="csv")

def hopsworks_save_features(df):
    fs = get_feature_store_handle()

    fg = fs.get_or_create_feature_group(
        name="transaction_demo",
        version=1,
        primary_key=['cc_num', 'category'],
        event_time="event_time",
        online_enabled=True,
    )

    fg.save(df)
