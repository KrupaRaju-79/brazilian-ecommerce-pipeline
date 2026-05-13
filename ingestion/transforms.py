# =============================================================================
# transforms.py
# Brazilian E-Commerce Pipeline — All DoFn classes and transformation helpers
# Compatible with: apache-beam==2.73.0, numpy==2.4.4
# =============================================================================

import apache_beam as beam
import csv
import io
import logging
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# Type-safe helpers
# NOTE: Do NOT use np.bool, np.int, np.float — removed in numpy 2.0
# Use plain Python bool, int, float instead
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(val):
    try:
        return float(val) if val and val.strip() else None
    except (ValueError, AttributeError):
        return None


def safe_int(val):
    try:
        return int(val) if val and val.strip() else None
    except (ValueError, AttributeError):
        return None


def safe_ts(val):
    """Return the timestamp string if it looks valid, else None."""
    try:
        v = val.strip() if val else None
        if not v:
            return None
        # Validate it's a real timestamp (not a text value shifted into wrong column)
        datetime.strptime(v[:19], '%Y-%m-%d %H:%M:%S')
        return v
    except (ValueError, AttributeError):
        return None


def parse_delay_days(delivered_str, estimated_str):
    """Return integer day difference between actual and estimated delivery.
    Negative = early, Positive = late, None = missing data."""
    try:
        fmt = '%Y-%m-%d %H:%M:%S'
        delivered = datetime.strptime(delivered_str[:19], fmt)
        estimated = datetime.strptime(estimated_str[:19], fmt)
        return (delivered - estimated).days
    except (ValueError, TypeError, AttributeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Parse DoFns — one per CSV file
# ─────────────────────────────────────────────────────────────────────────────

class ParseOrdersFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 8:
                return
            yield {
                'order_id':                      row[0].strip(),
                'customer_id':                   row[1].strip(),
                'order_status':                  row[2].strip(),
                'order_purchase_timestamp':      safe_ts(row[3]),
                'order_approved_at':             safe_ts(row[4]),
                'order_delivered_carrier_date':  safe_ts(row[5]),
                'order_delivered_customer_date': safe_ts(row[6]),
                'order_estimated_delivery_date': safe_ts(row[7]),
            }
        except Exception as e:
            logging.warning(f"ParseOrdersFn error: {e} | line: {line[:80]}")


class ParseItemsFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 7:
                return
            yield {
                'order_id':            row[0].strip(),
                'order_item_id':       safe_int(row[1]),
                'product_id':          row[2].strip(),
                'seller_id':           row[3].strip(),
                'shipping_limit_date': safe_ts(row[4]),
                'price':               safe_float(row[5]),
                'freight_value':       safe_float(row[6]),
            }
        except Exception as e:
            logging.warning(f"ParseItemsFn error: {e} | line: {line[:80]}")


class ParseCustomersFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 5:
                return
            yield {
                'customer_id':              row[0].strip(),
                'customer_unique_id':       row[1].strip(),
                'customer_zip_code_prefix': row[2].strip(),
                'customer_city':            row[3].strip(),
                'customer_state':           row[4].strip(),
            }
        except Exception as e:
            logging.warning(f"ParseCustomersFn error: {e} | line: {line[:80]}")


class ParseGeolocationFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 5:
                return
            yield {
                'geolocation_zip_code_prefix': row[0].strip(),
                'geolocation_lat':             safe_float(row[1]),
                'geolocation_lng':             safe_float(row[2]),
                'geolocation_city':            row[3].strip(),
                'geolocation_state':           row[4].strip(),
            }
        except Exception as e:
            logging.warning(f"ParseGeolocationFn error: {e} | line: {line[:80]}")


class ParsePaymentsFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 5:
                return
            yield {
                'order_id':              row[0].strip(),
                'payment_sequential':    safe_int(row[1]),
                'payment_type':          row[2].strip(),
                'payment_installments':  safe_int(row[3]),
                'payment_value':         safe_float(row[4]),
            }
        except Exception as e:
            logging.warning(f"ParsePaymentsFn error: {e} | line: {line[:80]}")


class ParseProductsFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 9:
                return
            yield {
                'product_id':                    row[0].strip(),
                'product_category_name':         row[1].strip(),
                'product_name_lenght':           safe_int(row[2]),
                'product_description_lenght':    safe_int(row[3]),
                'product_photos_qty':            safe_int(row[4]),
                'product_weight_g':              safe_float(row[5]),
                'product_length_cm':             safe_float(row[6]),
                'product_height_cm':             safe_float(row[7]),
                'product_width_cm':              safe_float(row[8]),
            }
        except Exception as e:
            logging.warning(f"ParseProductsFn error: {e} | line: {line[:80]}")


class ParseSellersFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 4:
                return
            yield {
                'seller_id':               row[0].strip(),
                'seller_zip_code_prefix':  row[1].strip(),
                'seller_city':             row[2].strip(),
                'seller_state':            row[3].strip(),
            }
        except Exception as e:
            logging.warning(f"ParseSellersFn error: {e} | line: {line[:80]}")


class ParseTranslationFn(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(io.StringIO(line)))
            if len(row) != 2:
                return
            yield {
                'product_category_name':         row[0].strip(),
                'product_category_name_english': row[1].strip(),
            }
        except Exception as e:
            logging.warning(f"ParseTranslationFn error: {e} | line: {line[:80]}")


# ─────────────────────────────────────────────────────────────────────────────
# Transformation DoFns — enrichment, joins, deduplication
# ─────────────────────────────────────────────────────────────────────────────

class EnrichOrdersFn(beam.DoFn):
    """
    Adds delivery_delay_days and is_late_delivery to each order row.
    Input:  raw order dict
    Output: enriched order dict with 2 new columns
    """
    def process(self, element):
        row = dict(element)
        delivered = row.get('order_delivered_customer_date')
        estimated = row.get('order_estimated_delivery_date')

        delay = parse_delay_days(delivered, estimated)
        row['delivery_delay_days'] = delay
        row['is_late_delivery']    = bool(delay > 0) if delay is not None else None
        yield row


class MergeOrderCustomerFn(beam.DoFn):
    """
    Merges CoGroupByKey result of orders + customers on customer_id.
    Adds customer_state, customer_city, customer_zip_code_prefix to each order.
    Drops orders with no matching customer (data quality).
    """
    def process(self, element):
        _, grouped = element
        orders    = list(grouped['orders'])
        customers = list(grouped['customers'])

        if not orders:
            return

        # Get customer details (there should be exactly 1 customer per customer_id)
        customer = customers[0] if customers else {}

        for order in orders:
            order['customer_state']            = customer.get('customer_state')
            order['customer_city']             = customer.get('customer_city')
            order['customer_zip_code_prefix']  = customer.get('customer_zip_code_prefix')
            order['customer_unique_id']        = customer.get('customer_unique_id')
            yield order


class MergeItemSellerFn(beam.DoFn):
    """
    Merges CoGroupByKey result of order_items + sellers on seller_id.
    Adds seller_state, seller_city to each order item.
    """
    def process(self, element):
        _, grouped = element
        items   = list(grouped['items'])
        sellers = list(grouped['sellers'])

        seller = sellers[0] if sellers else {}

        for item in items:
            item['seller_state'] = seller.get('seller_state')
            item['seller_city']  = seller.get('seller_city')
            yield item


class MergeItemProductTranslationFn(beam.DoFn):
    """
    Merges CoGroupByKey result of items + translations on product_category_name.
    Adds product_category_name_english to each order item.
    """
    def process(self, element):
        _, grouped = element
        items        = list(grouped['items'])
        translations = list(grouped['translations'])

        translation = translations[0] if translations else {}
        eng_name = translation.get('product_category_name_english', '')

        for item in items:
            item['product_category_name_english'] = eng_name
            yield item


class AverageGeoFn(beam.DoFn):
    """
    Deduplicates geolocation table.
    Input:  (zip_code_prefix, iterable_of_rows) from GroupByKey
    Output: one clean row per ZIP with avg lat/lng
    1,000,163 rows → ~19,015 unique ZIPs
    """
    def process(self, element):
        zip_code, rows = element
        rows = list(rows)

        lats = [r['geolocation_lat'] for r in rows if r.get('geolocation_lat') is not None]
        lngs = [r['geolocation_lng'] for r in rows if r.get('geolocation_lng') is not None]

        if not lats or not lngs:
            return

        yield {
            'zip_code_prefix': zip_code,
            'avg_lat':         round(sum(lats) / len(lats), 6),
            'avg_lng':         round(sum(lngs) / len(lngs), 6),
            'geolocation_state': rows[0].get('geolocation_state', ''),
            'geolocation_city':  rows[0].get('geolocation_city', ''),
        }