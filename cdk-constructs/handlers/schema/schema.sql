CREATE DATABASE IF NOT EXISTS ops;

USE ops;

CREATE TABLE IF NOT EXISTS orders_ops (
  order_id BINARY(16) PRIMARY KEY,
  partner_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  currency VARCHAR(8) NOT NULL DEFAULT 'USD',
  created_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  KEY idx_orders_partner (partner_id)
) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS shipments_ops (
  shipment_id BINARY(16) PRIMARY KEY,
  order_id BINARY(16) NOT NULL,
  carrier_code VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  tracking_no VARCHAR(64) NULL,
  created_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  KEY idx_shipments_order (order_id),
  KEY idx_shipments_carrier (carrier_code)
) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE shipment_events_ops (
  event_id VARCHAR(24) PRIMARY KEY,
  shipment_id BINARY(16) NOT NULL,
  event_type VARCHAR(40) NOT NULL,
  status VARCHAR(20) NULL,
  event_time DATETIME(3) NOT NULL,
  location_code VARCHAR(16) NULL,
  details_json JSON,
  updated_ts DATETIME(3) NOT NULL,
  is_deleted  TINYINT(1) NOT NULL DEFAULT 0,
  KEY ix_ship_time (shipment_id, event_time DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS invoices_ops (
  invoice_id BINARY(16) PRIMARY KEY,
  order_id BINARY(16) NOT NULL,
  partner_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  currency VARCHAR(8) NOT NULL DEFAULT 'USD',
  issued_ts DATETIME(3) NULL,
  due_ts DATETIME(3) NULL,
  paid_ts DATETIME(3) NULL,
  created_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  KEY idx_invoices_order (order_id),
  KEY idx_invoices_partner (partner_id)
) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS invoice_items_ops (
  invoice_id BINARY(16) NOT NULL,
  line_no INT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  description VARCHAR(255) NULL,
  quantity DECIMAL(12, 3) NOT NULL DEFAULT 0,
  unit_price DECIMAL(12, 2) NOT NULL DEFAULT 0,
  line_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  created_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (invoice_id, line_no),
  KEY idx_invoice_items_sku (sku)
) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_totals_history (
  snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  orders BIGINT UNSIGNED NOT NULL,
  shipments BIGINT UNSIGNED NOT NULL,
  shipment_events BIGINT UNSIGNED NOT NULL,
  invoices BIGINT UNSIGNED NOT NULL,
  invoice_items BIGINT UNSIGNED NOT NULL,
  total BIGINT UNSIGNED AS (
    orders + shipments + shipment_events + invoices + invoice_items
  ) STORED,
  captured_at DATETIME(3) NOT NULL DEFAULT NOW(3),
  KEY ix_captured_at (captured_at)
) ENGINE = InnoDB DEFAULT CHARSET=utf8mb4;
