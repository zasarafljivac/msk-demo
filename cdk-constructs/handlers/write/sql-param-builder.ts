const toISO = (t: string | Date | null | undefined) =>
  t ? (t instanceof Date ? t.toISOString() : t) : null;

export const params = {
  orders: (data: {
    order_id: Buffer;
    partner_id: string;
    status: string;
    total_amount: number;
    currency?: string;
    updated_ts: string | Date;
    is_deleted?: boolean | number;
  }) => [
    data.order_id,
    data.partner_id,
    data.status,
    data.total_amount,
    data.currency ?? "USD",
    toISO(data.updated_ts)!,
    Number(!!data.is_deleted),
  ],

  shipments: (data: {
    shipment_id: Buffer;
    order_id: Buffer;
    carrier_code: string;
    status: string;
    tracking_no?: string | null;
    updated_ts: string | Date;
    is_deleted?: boolean | number;
  }) => [
    data.shipment_id,
    data.order_id,
    data.carrier_code,
    data.status,
    data.tracking_no ?? null,
    toISO(data.updated_ts)!,
    Number(!!data.is_deleted),
  ],

  shipmentEvents: (data: {
    event_id: string;
    shipment_id: Buffer;
    event_type: string;
    status?: string | null;
    event_time: string | Date;
    location_code?: string | null;
    details_json?: any;
    updated_ts: string | Date;
    is_deleted?: boolean | number;
  }) => [
    data.event_id,
    data.shipment_id,
    data.event_type,
    data.status ?? null,
    toISO(data.event_time)!,
    data.location_code ?? null,
    data.details_json == null ? null : JSON.stringify(data.details_json),
    toISO(data.updated_ts)!,
    Number(!!data.is_deleted),
  ],

  invoices: (data: {
    invoice_id: Buffer;
    order_id: Buffer;
    partner_id: string;
    status: string;
    total_amount: number;
    currency?: string;
    issued_ts?: string | Date | null;
    due_ts?: string | Date | null;
    paid_ts?: string | Date | null;
    updated_ts: string | Date;
    is_deleted?: boolean | number;
  }) => [
    data.invoice_id,
    data.order_id,
    data.partner_id,
    data.status,
    data.total_amount,
    data.currency ?? "USD",
    toISO(data.issued_ts),
    toISO(data.due_ts),
    toISO(data.paid_ts),
    toISO(data.updated_ts)!,
    Number(!!data.is_deleted),
  ],

  invoiceItems: (data: {
    invoice_id: Buffer;
    line_no: number;
    sku: string;
    description?: string | null;
    quantity: number;
    unit_price: number;
    line_amount: number;
    updated_ts: string | Date;
    is_deleted?: boolean | number;
  }) => [
    data.invoice_id,
    data.line_no,
    data.sku,
    data.description ?? null,
    data.quantity,
    data.unit_price,
    data.line_amount,
    toISO(data.updated_ts)!,
    Number(!!data.is_deleted),
  ],
};
