// SHA-256 of the whole file, lowercase hex. Files are capped at 32 MiB, so
// reading one into an ArrayBuffer for Web Crypto is acceptable for now.
export async function sha256Hex(file: File): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', await file.arrayBuffer())
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}
