import { tableFromIPC, type Schema } from "apache-arrow";

const CONTINUATION = 0xffffffff;

/**
 * Decodes the IPC schema message a Flight SQL FlightInfo carries.
 *
 * apache-arrow's reader wants an IPC stream, not a lone message, so an
 * end-of-stream marker is always appended. arrow-rs already writes the
 * encapsulated form - continuation marker, little-endian length, then the
 * flatbuffer message - and passes straight through; a bare message (some
 * other producers) is wrapped into that form first.
 */
export function decodeIpcSchema(bytes: Uint8Array): Schema {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const encapsulated =
    bytes.byteLength >= 8 && view.getUint32(0, true) === CONTINUATION ? bytes : encapsulate(bytes);
  return tableFromIPC(withEndOfStream(encapsulated)).schema;
}

function encapsulate(message: Uint8Array): Uint8Array {
  const padded = Math.ceil(message.byteLength / 8) * 8;
  const out = new Uint8Array(8 + padded);
  const view = new DataView(out.buffer);
  view.setUint32(0, CONTINUATION, true);
  view.setInt32(4, padded, true);
  out.set(message, 8);
  return out;
}

function withEndOfStream(stream: Uint8Array): Uint8Array {
  const out = new Uint8Array(stream.byteLength + 8);
  out.set(stream, 0);
  const view = new DataView(out.buffer, stream.byteLength, 8);
  view.setUint32(0, CONTINUATION, true);
  view.setInt32(4, 0, true);
  return out;
}
