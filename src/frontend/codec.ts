export type CodecPayload = {
  /** Normalized absolute path ("/foo/bar.txt"). */
  path: string;
  /** Raw file bytes before transport/storage encoding. */
  content: Uint8Array;
  /** Encoding label describing how callers should treat the bytes. */
  encoding: "utf8" | "base64";
};

/**
 * Optional hook that can transform payloads before they hit the network or
 * persistence (encryption/compression) and reverse the transformation on read.
 */
export interface Codec {
  encode(payload: CodecPayload): Promise<CodecPayload> | CodecPayload;
  decode(payload: CodecPayload): Promise<CodecPayload> | CodecPayload;
}

/** Default passthrough implementation used when callers do not supply a codec. */
export const identityCodec: Codec = {
  encode: (payload) => payload,
  decode: (payload) => payload,
};
