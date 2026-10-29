import { sha256 } from '@noble/hashes/sha2.js'

self.onmessage = async ({ data: file }: MessageEvent<File>) => {
  try {
    const hash = sha256.create()
    for (let offset = 0; offset < file.size; offset += 4 * 1024 * 1024)
      hash.update(new Uint8Array(await file.slice(offset, offset + 4 * 1024 * 1024).arrayBuffer()))
    self.postMessage({
      digest: Array.from(hash.digest(), (b) => b.toString(16).padStart(2, '0')).join(''),
    })
  } catch {
    self.postMessage({ error: '无法计算文件摘要，请重新选择文件。' })
  }
}
