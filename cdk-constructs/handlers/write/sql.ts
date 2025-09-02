export const SQL = {
  orders_upsert: `
    INSERT INTO orders_ops (
      order_id,
      partner_id,
      status,
      total_amount,
      currency,
      updated_ts,
      is_deleted
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    AS newdata
    ON DUPLICATE KEY UPDATE
      partner_id = newdata.partner_id,
      status = newdata.status,
      total_amount = newdata.total_amount,
      currency = newdata.currency,
      updated_ts = GREATEST(orders_ops.updated_ts, newdata.updated_ts),
      is_deleted = newdata.is_deleted;
  `,

  shipments_upsert: `
    INSERT INTO shipments_ops (
      shipment_id,
      order_id,
      carrier_code,
      status,
      tracking_no,
      updated_ts,
      is_deleted
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    AS newdata
    ON DUPLICATE KEY UPDATE
      order_id      = newdata.order_id,
      carrier_code  = newdata.carrier_code,
      status        = newdata.status,
      tracking_no   = newdata.tracking_no,
      updated_ts    = GREATEST(shipments_ops.updated_ts, newdata.updated_ts),
      is_deleted    = newdata.is_deleted;
  `,

  shipment_events_upsert: `
    INSERT INTO shipment_events_ops (
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
    VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?)
    AS newdata
    ON DUPLICATE KEY UPDATE
      shipment_id = newdata.shipment_id,
      event_type = newdata.event_type,
      status = newdata.status,
      event_time = newdata.event_time,
      location_code = newdata.location_code,
      details_json = newdata.details_json,
      updated_ts = GREATEST(shipment_events_ops.updated_ts, newdata.updated_ts),
      is_deleted = newdata.is_deleted;
  `,

  invoices_upsert: `
    INSERT INTO invoices_ops(
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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    AS newdata
    ON DUPLICATE KEY UPDATE
      order_id = newdata.order_id,
      partner_id = newdata.partner_id,
      status = newdata.status,
      total_amount = newdata.total_amount,
      currency = newdata.currency,
      issued_ts = newdata.issued_ts,
      due_ts = newdata.due_ts,
      paid_ts = newdata.paid_ts,
      updated_ts = GREATEST(invoices_ops.updated_ts, newdata.updated_ts),
      is_deleted = newdata.is_deleted;
  `,

  invoice_items_upsert: `
    INSERT INTO invoice_items_ops (
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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    AS newdata
    ON DUPLICATE KEY UPDATE
      sku = newdata.sku,
      description = newdata.description,
      quantity = newdata.quantity,
      unit_price = newdata.unit_price,
      line_amount = newdata.line_amount,
      updated_ts = GREATEST(invoice_items_ops.updated_ts, newdata.updated_ts),
      is_deleted = newdata.is_deleted;
  `,

  dashboard_totals_history: `
    INSERT INTO dashboard_totals_history
      (orders, shipments, shipment_events, invoices, invoice_items)
    VALUES (?, ?, ?, ?, ?);
  `,
};
