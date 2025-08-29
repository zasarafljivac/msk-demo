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
        is_deleted,
        latency_ts
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      partner_id = VALUES(partner_id),
      status = VALUES(status),
      total_amount = VALUES(total_amount),
      currency = VALUES(currency),
      updated_ts = GREATEST(orders_ops.updated_ts, VALUES(updated_ts)),
      is_deleted = VALUES(is_deleted),
      latency_ts = VALUES(latency_ts);
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
        is_deleted,
        latency_ts
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      order_id = VALUES(order_id),
      carrier_code = VALUES(carrier_code),
      status = VALUES(status),
      tracking_no = VALUES(tracking_no),
      updated_ts = GREATEST(shipments_ops.updated_ts, VALUES(updated_ts)),
      is_deleted = VALUES(is_deleted),
      latency_ts = VALUES(latency_ts);
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
        is_deleted,
        latency_ts
      )
    VALUES
      (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      shipment_id = VALUES(shipment_id),
      event_type = VALUES(event_type),
      status = VALUES(status),
      event_time = VALUES(event_time),
      location_code = VALUES(location_code),
      details_json = VALUES(details_json),
      updated_ts = GREATEST(shipment_events_ops.updated_ts, VALUES(updated_ts)),
      is_deleted = VALUES(is_deleted),
      latency_ts = VALUES(latency_ts);
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
        is_deleted,
        latency_ts
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      order_id = VALUES(order_id),
      partner_id = VALUES(partner_id),
      status = VALUES(status),
      total_amount = VALUES(total_amount),
      currency = VALUES(currency),
      issued_ts = VALUES(issued_ts),
      due_ts = VALUES(due_ts),
      paid_ts = VALUES(paid_ts),
      updated_ts = GREATEST(invoices_ops.updated_ts, VALUES(updated_ts)),
      is_deleted = VALUES(is_deleted),
      latency_ts = VALUES(latency_ts);
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
        is_deleted,
        latency_ts
      )
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      sku = VALUES(sku),
      description = VALUES(description),
      quantity = VALUES(quantity),
      unit_price = VALUES(unit_price),
      line_amount = VALUES(line_amount),
      updated_ts = GREATEST(invoice_items_ops.updated_ts, VALUES(updated_ts)),
      is_deleted = VALUES(is_deleted),
      latency_ts = VALUES(latency_ts);
  `,
};
