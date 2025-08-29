CREATE DATABASE IF NOT EXISTS ops;

USE ops;

CREATE TABLE IF NOT EXISTS orders_ops (
  order_id VARCHAR(64) NOT NULL,
  partner_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  currency VARCHAR(8) NOT NULL DEFAULT 'USD',
  created_ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (order_id),
  KEY idx_orders_partner (partner_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS shipments_ops (
  shipment_id VARCHAR(64) NOT NULL,
  order_id VARCHAR(64) NOT NULL,
  carrier_code VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  tracking_no VARCHAR(64) NULL,
  created_ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (shipment_id),
  KEY idx_shipments_order (order_id),
  KEY idx_shipments_carrier (carrier_code)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS shipment_events_ops (
  event_id VARCHAR(128) NOT NULL,
  shipment_id VARCHAR(64) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  status VARCHAR(32) NULL,
  event_time TIMESTAMP(3) NOT NULL,
  location_code VARCHAR(64) NULL,
  details_json JSON NULL,
  created_ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (event_id),
  KEY idx_se_shipment (shipment_id),
  KEY idx_se_time (event_time)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS invoices_ops (
  invoice_id VARCHAR(64) NOT NULL,
  order_id VARCHAR(64) NOT NULL,
  partner_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  currency VARCHAR(8) NOT NULL DEFAULT 'USD',
  issued_ts TIMESTAMP(3) NULL,
  due_ts TIMESTAMP(3) NULL,
  paid_ts TIMESTAMP(3) NULL,
  created_ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (invoice_id),
  KEY idx_invoices_order (order_id),
  KEY idx_invoices_partner (partner_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS invoice_items_ops (
  invoice_id VARCHAR(64) NOT NULL,
  line_no INT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  description VARCHAR(255) NULL,
  quantity DECIMAL(12, 3) NOT NULL DEFAULT 0,
  unit_price DECIMAL(12, 2) NOT NULL DEFAULT 0,
  line_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
  created_ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  is_deleted BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (invoice_id, line_no),
  KEY idx_invoice_items_sku (sku)
) ENGINE = InnoDB;                                                                                 
