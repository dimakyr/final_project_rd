from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from airflow.hooks.base_hook import BaseHook
import logging
import os


def get_airflow_connections(connection_id):
    connection = BaseHook.get_connection(connection_id)
    logging.info(
        f'Connection config: {connection.host}, DB {connection.schema}, Login {connection.login}, PWD {connection.password}')
    creds = {
        'host': connection.host,
        'port': connection.port,
        'database': connection.schema,
        'user': connection.login,
        'password': connection.password
    }
    return creds


def process_to_gold(**kwargs):
    logging.info(f'Current path: {os.path.abspath(os.getcwd())}')
    files = os.listdir(os.path.abspath(os.getcwd()))
    logging.info(f'Filessss: {files}')
    creds = get_airflow_connections(kwargs['connection_id'])
    today = kwargs['current_date']
    url = f"jdbc:postgresql://{creds['host']}:{creds['port']}/postgres"
    gp_properties = {"user": f"{creds['user']}", "password": f"{creds['password']}"}

    spark = SparkSession.builder \
        .config("spark.driver.extraClassPath", '/home/user/vm/final_project_rd/postgresql-42.2.23.jar') \
        .appName('final') \
        .master('local') \
        .getOrCreate()

    api_df = spark.read.parquet(f'silver/{today}/out_of_stock_api/{today}') \
        .withColumnRenamed('date', 'oos_date')
    aisles = spark.read.parquet(f'silver/{today}/dshop_bu/aisles')
    clients = spark.read.parquet(f'silver/{today}/dshop_bu/clients')
    departments = spark.read.parquet(f'silver/{today}/dshop_bu/departments')
    location_areas = spark.read.parquet(f'silver/{today}/dshop_bu/location_areas')
    orders = spark.read.parquet(f'silver/{today}/dshop_bu/orders')
    products = spark.read.parquet(f'silver/{today}/dshop_bu/products')
    store_types = spark.read.parquet(f'silver/{today}/dshop_bu/store_types')
    stores = spark.read.parquet(f'silver/{today}/dshop_bu/stores')

    gold_dates_df = orders \
        .sort(orders.order_date) \
        .select(orders.order_date
                , F.weekofyear(orders.order_date).alias('date_week')
                , F.month(orders.order_date).alias('date_month')
                , F.year(orders.order_date).alias('date_year')
                , F.dayofweek(orders.order_date).alias('date_weekday'))
    logging.info(f'Transform data for table: dates')

    gold_dates_df.write.jdbc(url, table='dates', properties=gp_properties, mode='overwrite')
    logging.info(f'Recorded in the table: dates')

    clients_to_gold = clients.join(location_areas, clients.location_area_id == location_areas.area_id) \
        .select(F.col('id').alias('client_id'), F.col('fullname'), F.col('area_id').alias('location_area_id'))
    logging.info(f'Transform data for table: clients')

    clients_to_gold.write.jdbc(url, table='clients', properties=gp_properties, mode='overwrite')
    logging.info(f'Recorded in the table: clients')

    stores_to_gold = stores.join(store_types, stores.store_type_id == store_types.store_type_id) \
        .join(location_areas, stores.location_area_id == location_areas.area_id) \
        .select(F.col('store_id'), F.col('area_id').alias('location_area_id'), F.col('type'))
    logging.info(f'Transform data for table: stores')

    stores_to_gold.write.jdbc(url, table='stores', properties=gp_properties, mode='overwrite')
    logging.info(f'Recorded in the table: stores')

    products_to_gold = products.join(aisles, aisles.aisle_id == products.aisle_id) \
        .join(departments, departments.department_id == products.department_id) \
        .select('product_id', 'product_name', 'aisle', 'department')
    logging.info(f'Transform data for table: products')

    products_to_gold.write.jdbc(url, table='products', properties=gp_properties, mode='overwrite')
    logging.info(f'Recorded in the table: products')

    out_of_stock = api_df.join(orders, orders.product_id == api_df.product_id) \
        .drop(orders.product_id) \
        .select(F.col('product_id'), F.col('order_date'), F.col('store_id'))
    logging.info(f'Transform data for table: out_of_stock')

    out_of_stock.write.jdbc(url, table='out_of_stock', properties=gp_properties, mode='overwrite')
    logging.info(f'Recorded in the table: out_of_stock')

    orders.write.jdbc(url, table='orders', properties=gp_properties, mode='overwrite')
    logging.info(f'Transform data for table: orders')
    logging.info(f'Recorded in the table: orders')

    location_areas.write.jdbc(url, table='location_areas', properties=gp_properties, mode='overwrite')
    logging.info(f'Transform data for table: location_areas')
    logging.info(f'Recorded in the table: location_areas')


