# =============================================================================
# beam_pipeline.py
# Brazilian E-Commerce Pipeline — Apache Beam transformation pipeline
# Compatible with: apache-beam==2.73.0
#
# USAGE — Local test (DirectRunner, 100 rows):
#   python ingestion/beam_pipeline.py \
#     --runner=DirectRunner \
#     --project=project-d251f75f-1a3c-44ee-82e \
#     --input_bucket=gcp-brazilian-pipeline-bucket \
#     --output_dataset=staging
#
# USAGE — Full run on Dataflow:
#   python ingestion/beam_pipeline.py \
#     --runner=DataflowRunner \
#     --project=project-d251f75f-1a3c-44ee-82e \
#     --region=us-central1 \
#     --temp_location=gs://gcp-brazilian-pipeline-bucket-dataflow-temp/tmp \
#     --staging_location=gs://gcp-brazilian-pipeline-bucket-dataflow-temp/staging \
#     --input_bucket=gcp-brazilian-pipeline-bucket \
#     --output_dataset=staging \
#     --job_name=brazilian-ecommerce-transform
# =============================================================================

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from transforms import (
    ParseOrdersFn,
    ParseItemsFn,
    ParseCustomersFn,
    ParseGeolocationFn,
    ParsePaymentsFn,
    ParseProductsFn,
    ParseSellersFn,
    ParseTranslationFn,
    EnrichOrdersFn,
    MergeOrderCustomerFn,
    MergeItemSellerFn,
    MergeItemProductTranslationFn,
    AverageGeoFn,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery table schemas for staging.*
# ─────────────────────────────────────────────────────────────────────────────

ORDERS_ENRICHED_SCHEMA = {
    'fields': [
        {'name': 'order_id',                      'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'customer_id',                   'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'order_status',                  'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'order_purchase_timestamp',      'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_approved_at',             'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_carrier_date',  'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_delivered_customer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'order_estimated_delivery_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'delivery_delay_days',           'type': 'INTEGER',   'mode': 'NULLABLE'},
        {'name': 'is_late_delivery',              'type': 'BOOLEAN',   'mode': 'NULLABLE'},
        {'name': 'customer_state',                'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'customer_city',                 'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'customer_zip_code_prefix',      'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'customer_unique_id',            'type': 'STRING',    'mode': 'NULLABLE'},
    ]
}

ORDER_ITEMS_ENRICHED_SCHEMA = {
    'fields': [
        {'name': 'order_id',                         'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'order_item_id',                    'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'product_id',                       'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'seller_id',                        'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'shipping_limit_date',              'type': 'TIMESTAMP','mode': 'NULLABLE'},
        {'name': 'price',                            'type': 'FLOAT',   'mode': 'NULLABLE'},
        {'name': 'freight_value',                    'type': 'FLOAT',   'mode': 'NULLABLE'},
        {'name': 'seller_state',                     'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'seller_city',                      'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'product_category_name',            'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'product_category_name_english',    'type': 'STRING',  'mode': 'NULLABLE'},
    ]
}

PAYMENTS_SCHEMA = {
    'fields': [
        {'name': 'order_id',             'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'payment_sequential',   'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_type',         'type': 'STRING',  'mode': 'NULLABLE'},
        {'name': 'payment_installments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_value',        'type': 'FLOAT',   'mode': 'NULLABLE'},
    ]
}

GEOLOCATION_DEDUPED_SCHEMA = {
    'fields': [
        {'name': 'zip_code_prefix',  'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'avg_lat',          'type': 'FLOAT',  'mode': 'NULLABLE'},
        {'name': 'avg_lng',          'type': 'FLOAT',  'mode': 'NULLABLE'},
        {'name': 'geolocation_state','type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'geolocation_city', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


# ─────────────────────────────────────────────────────────────────────────────
# BQ write helper
# ─────────────────────────────────────────────────────────────────────────────

def write_to_bq(pcollection, label, project, dataset, table, schema, temp_location):
    """Write a PCollection to a BigQuery staging table.
    WRITE_TRUNCATE = replace table on every run (idempotent).
    custom_gcs_temp_location required by FILE_LOADS method on DirectRunner.
    """
    return (
        pcollection
        | f'Write {label}' >> beam.io.WriteToBigQuery(
            table=f'{project}:{dataset}.{table}',
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=temp_location,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run(argv=None):
    parser = argparse.ArgumentParser()
    # Our custom args — everything else goes to PipelineOptions
    parser.add_argument('--input_bucket',   required=True,
                        help='GCS bucket name containing raw/ CSVs')
    parser.add_argument('--output_dataset', default='staging',
                        help='Target BigQuery dataset (default: staging)')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pass ALL remaining args (including --project, --runner, --region,
    # --temp_location, --job_name) directly to PipelineOptions so Beam
    # and Dataflow can read them natively. This fixes the
    # "Missing required option: project" error on DataflowRunner.
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    # Read project and temp_location from PipelineOptions (where Beam expects them)
    from apache_beam.options.pipeline_options import GoogleCloudOptions, WorkerOptions
    gcp_options  = options.view_as(GoogleCloudOptions)
    PROJECT      = gcp_options.project
    TEMP_LOC     = gcp_options.temp_location
    BUCKET       = known_args.input_bucket
    DATASET      = known_args.output_dataset

    def gcs(filename):
        return f'gs://{BUCKET}/raw/{filename}'

    logger.info(f"Starting pipeline | project={PROJECT} bucket={BUCKET} dataset={DATASET}")

    with beam.Pipeline(options=options) as p:

        # ── STEP 1: Read and parse all 8 CSVs ────────────────────────────────

        def read_csv(label, filename, parse_fn):
            return (
                p
                | f'Read {label}'  >> beam.io.ReadFromText(gcs(filename), skip_header_lines=1)
                | f'Parse {label}' >> beam.ParDo(parse_fn)
            )

        p_orders      = read_csv('Orders',      'olist_orders_dataset.csv',                  ParseOrdersFn())
        p_items       = read_csv('Items',        'olist_order_items_dataset.csv',             ParseItemsFn())
        p_customers   = read_csv('Customers',    'olist_customers_dataset.csv',               ParseCustomersFn())
        p_geo         = read_csv('Geolocation',  'olist_geolocation_dataset.csv',             ParseGeolocationFn())
        p_payments    = read_csv('Payments',     'olist_order_payments_dataset.csv',          ParsePaymentsFn())
        p_products    = read_csv('Products',     'olist_products_dataset.csv',                ParseProductsFn())
        p_sellers     = read_csv('Sellers',      'olist_sellers_dataset.csv',                 ParseSellersFn())
        p_translation = read_csv('Translation',  'product_category_name_translation.csv',    ParseTranslationFn())

        # ── STEP 2: Enrich orders — delivery delay + is_late ─────────────────

        p_orders_enriched = (
            p_orders
            | 'Enrich orders' >> beam.ParDo(EnrichOrdersFn())
        )

        # ── STEP 3: Join orders + customers on customer_id ───────────────────
        # Adds: customer_state, customer_city, customer_zip, customer_unique_id

        orders_by_cust   = p_orders_enriched | 'Key orders→customer_id'   >> beam.Map(lambda r: (r['customer_id'], r))
        customers_keyed  = p_customers       | 'Key customers→customer_id' >> beam.Map(lambda r: (r['customer_id'], r))

        p_orders_final = (
            {'orders': orders_by_cust, 'customers': customers_keyed}
            | 'CoGroup orders+customers'   >> beam.CoGroupByKey()
            | 'Merge order+customer rows'  >> beam.ParDo(MergeOrderCustomerFn())
        )

        # ── STEP 4: Join items + sellers on seller_id ────────────────────────
        # Adds: seller_state, seller_city

        items_by_seller  = p_items   | 'Key items→seller_id'   >> beam.Map(lambda r: (r['seller_id'], r))
        sellers_keyed    = p_sellers | 'Key sellers→seller_id'  >> beam.Map(lambda r: (r['seller_id'], r))

        p_items_with_seller = (
            {'items': items_by_seller, 'sellers': sellers_keyed}
            | 'CoGroup items+sellers'      >> beam.CoGroupByKey()
            | 'Merge item+seller rows'     >> beam.ParDo(MergeItemSellerFn())
        )

        # ── STEP 5: Join items + products on product_id ──────────────────────
        # Gets product_category_name onto each item row

        items_by_product   = p_items_with_seller | 'Key items→product_id'    >> beam.Map(lambda r: (r['product_id'], r))
        products_keyed     = p_products          | 'Key products→product_id'  >> beam.Map(lambda r: (r['product_id'], r))

        # Build category → english name lookup from translation table
        # Then join items to translations via product_category_name
        # First: get category_name onto item (from products join)

        def get_category(element):
            _, grouped = element
            items    = list(grouped['items'])
            products = list(grouped['products'])
            product  = products[0] if products else {}
            for item in items:
                item['product_category_name'] = product.get('product_category_name', '')
                yield item

        p_items_with_category = (
            {'items': items_by_product, 'products': products_keyed}
            | 'CoGroup items+products'     >> beam.CoGroupByKey()
            | 'Merge item+product'         >> beam.FlatMap(get_category)
        )

        # ── STEP 6: Join items + translation on product_category_name ────────
        # Adds: product_category_name_english (Portuguese → English)

        items_by_cat     = p_items_with_category | 'Key items→category'        >> beam.Map(lambda r: (r['product_category_name'], r))
        translation_keyed = p_translation        | 'Key translation→category'  >> beam.Map(lambda r: (r['product_category_name'], r))

        p_items_final = (
            {'items': items_by_cat, 'translations': translation_keyed}
            | 'CoGroup items+translation'  >> beam.CoGroupByKey()
            | 'Merge item+translation'     >> beam.ParDo(MergeItemProductTranslationFn())
        )

        # ── STEP 7: Deduplicate geolocation ──────────────────────────────────
        # 1,000,163 rows → ~19,015 unique ZIPs with averaged lat/lng

        p_geo_deduped = (
            p_geo
            | 'Key geo→zip'         >> beam.Map(lambda r: (r['geolocation_zip_code_prefix'], r))
            | 'Group geo by zip'    >> beam.GroupByKey()
            | 'Average geo per zip' >> beam.ParDo(AverageGeoFn())
        )

        # ── STEP 8: Write all 4 staging tables to BigQuery ───────────────────

        write_to_bq(p_orders_final, 'orders_enriched',
                    PROJECT, DATASET, 'orders_enriched',      ORDERS_ENRICHED_SCHEMA,      TEMP_LOC)

        write_to_bq(p_items_final,  'order_items_enriched',
                    PROJECT, DATASET, 'order_items_enriched', ORDER_ITEMS_ENRICHED_SCHEMA, TEMP_LOC)

        write_to_bq(p_payments,     'payments',
                    PROJECT, DATASET, 'payments',             PAYMENTS_SCHEMA,             TEMP_LOC)

        write_to_bq(p_geo_deduped,  'geolocation_deduped',
                    PROJECT, DATASET, 'geolocation_deduped',  GEOLOCATION_DEDUPED_SCHEMA,  TEMP_LOC)

    logger.info("Pipeline finished successfully.")


if __name__ == '__main__':
    run()