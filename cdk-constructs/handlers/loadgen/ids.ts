import { createHash } from "crypto";
import {
  v7 as uuidv7,
  parse as uuidParse,
  stringify as uuidStringify,
} from "uuid";

type UuidVariants = string | Buffer | Uint8Array;

const newBinaryId = (): Buffer => Buffer.from(uuidParse(uuidv7()));
const binaryIdToString = (idBin: Buffer): string =>
  uuidStringify(new Uint8Array(idBin));
const stringToBinaryId = (uuidStr: string) => Buffer.from(uuidParse(uuidStr));

const normalizeUuid = (id: UuidVariants): string => {
  if (typeof id === "string") return id.trim().toLowerCase();
  if (id instanceof Uint8Array || Buffer.isBuffer(id)) {
    if (id.length === 16) return uuidStringify(id as Uint8Array).toLowerCase();
    return Buffer.from(id).toString("hex").toLowerCase();
  }
  throw new Error("Unsupported uuid type");
};

const makeEventId = (shipmentData: {
  shipment_id: UuidVariants;
  event_type: string;
  event_time: string;
  location_code?: string | null;
}): string => {
  const composite = JSON.stringify({
    id: normalizeUuid(shipmentData.shipment_id),
    etype: shipmentData.event_type.trim().toUpperCase(),
    etime: new Date(shipmentData.event_time).toISOString(),
    l: (shipmentData.location_code ?? "").trim().toUpperCase(),
  });
  return createHash("sha256").update(composite).digest("hex").slice(0, 24);
};

export const ids = {
  newBinaryId,
  binaryIdToString,
  stringToBinaryId,
  makeEventId,
};
