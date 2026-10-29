// Stream large files in a worker; never allocate a whole-file ArrayBuffer.
export async function sha256Hex(file: File): Promise<string> {
  if (file.size <= 32 * 1024 * 1024) {
    const digest = await crypto.subtle.digest('SHA-256', await file.arrayBuffer())
    return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('')
  }
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./hash.worker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = ({ data }: MessageEvent<{ digest?: string; error?: string }>) => {
      worker.terminate()
      if (data.digest) resolve(data.digest)
      else reject(new Error(data.error ?? '摘要计算失败'))
    }
    worker.onerror = () => {
      worker.terminate()
      reject(new Error('摘要计算失败'))
    }
    worker.postMessage(file)
  })
}
