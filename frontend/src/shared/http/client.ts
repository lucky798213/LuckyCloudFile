import { ApiError, responseError } from './errors'

type Auth = { getToken: () => string | null; onUnauthorized: () => void }
type RequestOptions = Omit<RequestInit, 'body'> & { body?: unknown }

export class HttpClient {
  private auth: Auth = { getToken: () => null, onUnauthorized: () => undefined }

  constructor(private readonly transport: typeof fetch = (...args) => fetch(...args)) {}

  configureAuth(auth: Auth) {
    this.auth = auth
  }

  private rejectUnauthorized(status: number, token: string | null) {
    // A late response from account A must never log out a newly logged-in account B.
    if (status === 401 && token && token === this.auth.getToken()) this.auth.onUnauthorized()
  }

  async request<T>(
    path: string,
    options: RequestOptions = {},
    kind: 'json' | 'blob' = 'json',
  ): Promise<T> {
    const token = this.auth.getToken()
    const headers = new Headers(options.headers)
    if (token) headers.set('Authorization', `Bearer ${token}`)
    if (options.body !== undefined) headers.set('Content-Type', 'application/json')
    const response = await this.transport(`/api${path}`, {
      ...options,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    })
    if (!response.ok) {
      this.rejectUnauthorized(response.status, token)
      const body: unknown = await response.json().catch(() => null)
      throw responseError(body, response.status)
    }
    if (response.status === 204) return undefined as T
    if (kind === 'blob') return (await response.blob()) as T
    try {
      return (await response.json()) as T
    } catch {
      throw new ApiError(
        'invalid_response',
        '后端响应不是有效 JSON，请检查 API_PROXY_TARGET 和反向代理配置。',
        response.status,
      )
    }
  }

  // XHR is intentionally limited to byte transport: it provides real upload progress.
  upload<T>(
    path: string,
    blob: Blob,
    signal: AbortSignal,
    progress: (bytes: number) => void,
  ): Promise<T> {
    const token = this.auth.getToken()
    return new Promise((resolve, reject) => {
      if (signal.aborted) {
        reject(new DOMException('上传已停止', 'AbortError'))
        return
      }
      const xhr = new XMLHttpRequest()
      const abort = () => xhr.abort()
      const cleanup = () => signal.removeEventListener('abort', abort)
      xhr.open('PUT', `/api${path}`)
      xhr.responseType = 'json'
      xhr.timeout = 5 * 60_000
      xhr.setRequestHeader('Content-Type', 'application/octet-stream')
      if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`)
      xhr.upload.onprogress = (event) => progress(event.loaded)
      xhr.onload = () => {
        cleanup()
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(xhr.response as T)
          return
        }
        this.rejectUnauthorized(xhr.status, token)
        reject(responseError(xhr.response, xhr.status))
      }
      xhr.onerror = () => {
        cleanup()
        reject(new ApiError('network_error', '网络连接失败', 0))
      }
      xhr.ontimeout = () => {
        cleanup()
        reject(new ApiError('network_error', '上传超时，请查询状态', 0))
      }
      xhr.onabort = () => {
        cleanup()
        reject(new DOMException('上传已停止', 'AbortError'))
      }
      signal.addEventListener('abort', abort, { once: true })
      xhr.send(blob)
    })
  }
}

export const api = new HttpClient()
