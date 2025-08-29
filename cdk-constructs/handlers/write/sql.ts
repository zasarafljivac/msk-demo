export const SQL = {
  orders_upsert: `
    INSERT INTO
      orders_ops (
        order_id,
        partner_id,
        status,
        total_amount,
        currency,
        updated_ts,
        is_deleted
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY
    UPDATE partner_id = new.partner_id,
    status = new.status,
    total_amount = new.total_amount,
    currency = new.currency,
    updated_ts = GREATEST (orders_ops.updated_ts, new.updated_ts),
    is_deleted = new.is_deleted;
  `,
  shipments_upsert: `
    INSERT INTO
      shipments_ops (
        shipment_id,
        order_id,
        carrier_code,
        status,
        tracking_no,
        updated_ts,
        is_deleted
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY
    UPDATE order_id = new.order_id,
    carrier_code = new.carrier_code,
    status = new.status,
    tracking_no = new.tracking_no,
    updated_ts = GREATEST (shipments_ops.updated_ts, new.updated_ts),
    is_deleted = new.is_deleted;
  `,
  shipment_events_upsert: `
    INSERT INTO
      shipment_events_ops (
        event_id,
        shipment_id,
        event_type,
        status,
        event_time,
        location_code,
        details_json,
        updated_ts,
        is_deleted
      )
    VALUES
      (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?) AS new ON DUPLICATE KEY
    UPDATE shipment_id = new.shipment_id,
    event_type = new.event_type,
    status = new.status,
    event_time = new.event_time,
    location_code = new.location_code,
    details_json = new.details_json,
    updated_ts = GREATEST (shipment_events_ops.updated_ts, new.updated_ts),
    is_deleted = new.is_deleted;
  `,
  invoices_upsert: `
    INSERT INTO
      invoices_ops (
        invoice_id,
        order_id,
        partner_id,
        status,
        total_amount,
        currency,
        issued_ts,
        due_ts,
        paid_ts,
        updated_ts,
        is_deleted
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY
    UPDATE order_id = new.order_id,
    partner_id = new.partner_id,
    status = new.status,
    total_amount = new.total_amount,
    currency = new.currency,
    issued_ts = new.issued_ts,
    due_ts = new.due_ts,
    paid_ts = new.paid_ts,
    updated_ts = GREATEST (invoices_ops.updated_ts, new.updated_ts),
    is_deleted = new.is_deleted;
  `,
  invoice_items_upsert: `
    INSERT INTO
      invoice_items_ops (
        invoice_id,
        line_no,
        sku,
        description,
        quantity,
        unit_price,
        line_amount,
        updated_ts,
        is_deleted
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY
    UPDATE sku = new.sku,
    description = new.description,
    quantity = new.quantity,
    unit_price = new.unit_price,
    line_amount = new.line_amount,
    updated_ts = GREATEST (invoice_items_ops.updated_ts, new.updated_ts),
    is_deleted = new.is_deleted;
  `,
};