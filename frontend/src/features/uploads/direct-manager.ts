import { api } from '../../shared/http/client'

export type DirectSession = {
  id: string
  status:
    | 'initializing'
    | 'uploading'
    | 'completing'
    | 'verifying'
    | 'completed'
    | 'canceled'
    | 'expired'
    | 'failed'
  part_size: number
  expected_size: number
  uploaded_parts: { number: number; size: number; etag: string }[]
  result_file_id?: string
  last_error?: string
}
type Signed = { url: string; headers: Record<string, string> }
const path = (id: string) => `/direct-upload-sessions/${encodeURIComponent(id)}`
export const getDirect = (id: string) => api.request<DirectSession>(path(id))
export const cancelDirect = (id: string) =>
  api.request<DirectSession>(path(id), { method: 'DELETE' })
const wait = (signal: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    signal.throwIfAborted()
    const abort = () => {
      clearTimeout(timer)
      reject(signal.reason)
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort)
      resolve()
    }, 1000)
    signal.addEventListener('abort', abort, { once: true })
  })

// Only identity and IDs are persisted, not File objects, credentials or signed URLs.
// Reselecting the same file recomputes its full digest before resuming.
export async function runDirect(
  file: File,
  parent: string,
  digest: string,
  signal: AbortSignal,
  onSession: (session: DirectSession) => void,
  onProgress: (n: number) => void,
) {
  const key = `lcf-direct:${JSON.stringify([parent, file.name, file.size, digest])}`
  const saved = localStorage.getItem(key)
  let resume: { requestId: string; id?: string } = saved
    ? JSON.parse(saved)
    : { requestId: crypto.randomUUID() }
  let session: DirectSession
  if (resume.id) {
    session = await getDirect(resume.id)
    if (['failed', 'canceled', 'expired'].includes(session.status)) {
      resume = { requestId: crypto.randomUUID() }
    } else if (session.status === 'completed') {
      localStorage.removeItem(key)
      onSession(session)
      return session
    }
  }
  localStorage.setItem(key, JSON.stringify(resume))
  session = resume.id
    ? await getDirect(resume.id)
    : await api.request<DirectSession>('/direct-upload-sessions', {
        method: 'POST',
        headers: { 'Idempotency-Key': resume.requestId },
        body: { parent_id: parent, name: file.name, size: file.size, sha256: digest },
      })
  localStorage.setItem(key, JSON.stringify({ ...resume, id: session.id }))
  onSession(session)
  signal.throwIfAborted()
  // A lost initialization response must not start another multipart implicitly.
  if (session.status === 'initializing')
    throw new Error('初始化结果待服务端核对，请稍后重新选择同一文件。')
  if (session.status === 'uploading') {
    if (session.part_size <= 0 || session.expected_size !== file.size)
      throw new Error('直传会话参数不匹配')
    const partSize = session.part_size
    const uploaded = new Map(session.uploaded_parts.map((p) => [p.number, p]))
    const pending: number[] = []
    let loaded = 0
    for (let n = 1; n <= Math.ceil(file.size / partSize); n++) {
      const size = Math.min(partSize, file.size - (n - 1) * partSize)
      const p = uploaded.get(n)
      if (p && p.size === size && p.etag) loaded += size
      else pending.push(n)
    }
    onProgress(loaded)
    let next = 0
    const workers = Array.from({ length: Math.min(3, pending.length) }, async () => {
      while (next < pending.length) {
        const number = pending[next++]!
        const blob = file.slice((number - 1) * partSize, Math.min(number * partSize, file.size))
        for (let attempt = 0; ; attempt++) {
          signal.throwIfAborted()
          try {
            // Fresh signature on every retry; never recreate a session for a failed part.
            const urls = await api.request<Record<string, Signed>>(
              `${path(session.id)}/part-urls`,
              { method: 'POST', body: { part_numbers: [number] } },
            )
            const signed = urls[String(number)]!
            const response = await fetch(signed.url, {
              method: 'PUT',
              headers: signed.headers,
              body: blob,
              signal,
            })
            if (!response.ok) throw new Error(`COS 分块失败：${response.status}`)
            if (!response.headers.get('ETag')) throw new Error('COS CORS 必须暴露 ETag 响应头')
            loaded += blob.size
            onProgress(loaded)
            break
          } catch (error) {
            if (signal.aborted || attempt >= 2) throw error
            await wait(signal)
          }
        }
      }
    })
    // Wait for all in-flight parts before surfacing failure or cancellation.
    const results = await Promise.allSettled(workers)
    const failure = results.find((result) => result.status === 'rejected')
    if (failure?.status === 'rejected') throw failure.reason
    signal.throwIfAborted()
    try {
      session = await api.request<DirectSession>(`${path(session.id)}/complete`, { method: 'POST' })
    } catch {
      // Request/response loss is ambiguous: query, do not create a second upload.
      session = await getDirect(session.id)
      if (session.status === 'uploading') throw new Error('合并尚未开始，请重新选择同一文件继续。')
    }
  }
  while (['completing', 'verifying'].includes(session.status)) {
    onSession(session)
    await wait(signal)
    session = await getDirect(session.id)
  }
  onSession(session)
  if (session.status !== 'completed')
    throw new Error(session.last_error || `上传未发布：${session.status}`)
  localStorage.removeItem(key)
  return session
}
