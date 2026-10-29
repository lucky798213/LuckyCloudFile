import { ApiError, errorMessage } from '../../shared/http/errors'
import { sha256Hex } from './hash'
import { runDirect, getDirect, cancelDirect } from './direct-manager'
import type { DirectSession } from './direct-manager'
import { MAX_UPLOAD_BYTES, resolveMode } from './types'
import type { ModeChoice, Transfer, UploadGateway, UploadSession } from './types'

// The instant endpoint is idempotent, so an unknown result is retried with the
// same key. A hit is a metadata-only operation; a miss is the only outcome that
// is allowed to fall back to the normal byte upload.
const INSTANT_CONFIRM_ATTEMPTS = 3
const INSTANT_RETRY_DELAYS = [200, 600]

class InstantUploadRejectedError extends Error {
  constructor(cause: ApiError) {
    super(errorMessage(cause))
    this.name = 'InstantUploadRejectedError'
  }
}

class InstantUploadUnconfirmedError extends Error {
  constructor() {
    super(
      '秒传结果未确认：服务端可能已经创建文件。为避免重复上传，已停止普通上传，请点击“重试确认”。',
    )
    this.name = 'InstantUploadUnconfirmedError'
  }
}

function isUnknownInstantError(error: unknown) {
  if (error instanceof ApiError) {
    // 507 是明确的容量不足，服务端没有创建文件；不能当成“结果未知”去重试。
    if (error.code === 'insufficient_storage' || error.status === 507) return false
    return (
      error.status === 0 ||
      error.status === 408 ||
      error.status === 425 ||
      error.status === 429 ||
      error.status >= 500 ||
      error.code === 'network_error' ||
      error.code === 'invalid_response'
    )
  }
  // fetch 在网络中断/CORS 失败时抛 TypeError；其他 DOMException 同样无法
  // 证明服务端没有执行，按结果未知处理。
  return error instanceof TypeError || error instanceof DOMException
}

// A small, framework-independent queue. React subscribes; it never schedules byte writes.
export class UploadManager {
  private items: Transfer[] = []
  private listeners = new Set<() => void>()
  private controller: AbortController | null = null
  private activeId: string | null = null
  private canceled = new Set<string>()
  private alive = true

  constructor(
    private readonly gateway: UploadGateway,
    private readonly published: (parentId: string) => void,
    private readonly makeId = () => crypto.randomUUID(),
  ) {}

  getSnapshot = () => this.items
  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }
  start() {
    this.alive = true
    void this.pump()
  }
  stop() {
    this.alive = false
    this.controller?.abort()
  }

  enqueue(files: File[], parentId: string, mode: ModeChoice) {
    if (
      files.some(
        (file) =>
          file.size >
          ((mode === 'auto' || mode === 'direct') && this.gateway.directCapabilities
            ? 80 * 1024 * 1024 * 1024
            : MAX_UPLOAD_BYTES),
      )
    )
      throw new Error('超过当前上传方式上限：代理 32 MiB，COS 直传 128 MiB。')
    this.items = [
      ...this.items,
      ...files.map((file) => ({
        id: this.makeId(),
        file,
        parentId,
        mode: resolveMode(file.size, mode),
        choice: mode,
        status: 'queued' as const,
        loaded: 0,
      })),
    ]
    this.emit()
    void this.pump()
  }

  cancel(id: string) {
    const item = this.find(id)
    if (!item || ['completed', 'canceled', 'expired', 'canceling'].includes(item.status)) return
    if (item.status === 'failed' && !item.sessionId) return
    this.canceled.add(id)
    if (item.status === 'queued') {
      this.update(id, { status: 'canceled' })
      return
    }
    this.update(id, { status: 'canceling', message: undefined })
    if (this.activeId === id) {
      this.controller?.abort()
      return
    }
    if (item.sessionId) void this.cancelRemote(id, item.sessionId)
    else this.update(id, { status: 'canceled' })
  }

  async check(id: string) {
    const item = this.find(id)
    if (!item?.sessionId || item.status !== 'failed') return
    this.update(id, { status: 'checking', message: undefined })
    try {
      if (item.direct) {
        const session = await getDirect(item.sessionId)
        this.applyDirect(id, session)
        if (['initializing', 'uploading', 'completing', 'verifying'].includes(session.status))
          this.update(id, { status: 'failed', message: '会话仍在进行，可重试续传或继续查询状态。' })
      } else this.applySession(id, await this.gateway.get(item.sessionId))
    } catch (error) {
      this.update(id, { status: 'failed', message: errorMessage(error) })
    }
  }

  // 秒传结果未知时，用同一个幂等键重新走一次预检。命中就完成，明确未命中
  // 才继续普通上传；再次未知会重新进入待确认状态。
  retry(id: string) {
    const item = this.find(id)
    if (!item || item.status !== 'failed' || (!item.instantUnknown && !item.direct)) return
    this.canceled.delete(id)
    this.update(id, {
      status: 'queued',
      instantUnknown: false,
      message: '正在重新确认秒传结果…',
    })
    void this.pump()
  }

  async confirm(id: string) {
    const item = this.find(id)
    if (!item?.sessionId || item.status !== 'failed' || item.serverStatus !== 'ready') return
    this.update(id, { status: 'confirming', message: undefined })
    try {
      this.applySession(id, await this.gateway.complete(item.sessionId))
    } catch (error) {
      this.update(id, {
        status: 'failed',
        message: `完成结果未确认。${errorMessage(error)} 请先查询状态，不要重复创建上传。`,
      })
    }
  }

  clearFinished() {
    this.items = this.items.filter(
      (item) => !['completed', 'canceled', 'expired'].includes(item.status),
    )
    this.emit()
  }

  private async pump() {
    if (!this.alive || this.activeId) return
    const item = this.items.find((entry) => entry.status === 'queued')
    if (!item) return
    this.activeId = item.id
    this.controller = new AbortController()
    try {
      await this.run(item, this.controller.signal)
    } catch (error) {
      if (this.alive) {
        const latest = this.find(item.id)
        if (this.canceled.has(item.id) && latest?.sessionId)
          await this.cancelRemote(item.id, latest.sessionId)
        else if (error instanceof InstantUploadUnconfirmedError)
          this.update(item.id, {
            status: 'failed',
            instantUnknown: true,
            message: error.message,
          })
        else if (error instanceof InstantUploadRejectedError)
          this.update(item.id, {
            status: 'failed',
            message: `秒传校验失败：${error.message} 未开始普通上传。`,
          })
        else
          this.update(item.id, {
            status: 'failed',
            message: `${errorMessage(error)}${latest?.sessionId ? ' 可查询服务端状态或取消该会话。' : ' 创建结果未确认；请检查目录，未完成占位会话将由后端过期清理。'}`,
          })
      }
    } finally {
      this.activeId = null
      this.controller = null
      if (this.alive) void this.pump()
    }
  }

  private async run(item: Transfer, signal: AbortSignal) {
    this.update(item.id, {
      status: 'creating',
      message: item.instantDigest ? '正在确认秒传结果…' : '正在计算内容摘要…',
    })
    const capability = this.gateway.directCapabilities
      ? await this.gateway.directCapabilities()
      : { enabled: false, max_bytes: MAX_UPLOAD_BYTES }
    const direct =
      item.file.size > 0 &&
      capability.enabled &&
      (item.choice === 'auto' || item.choice === 'direct')
    if (item.choice === 'direct' && item.file.size > 0 && !direct)
      throw new Error('当前存储策略尚未启用 COS 直传。')
    if (item.file.size > (direct ? capability.max_bytes : MAX_UPLOAD_BYTES))
      throw new Error('文件超过当前存储策略的上传上限。')
    if (direct) this.update(item.id, { direct: true })
    if ((await this.tryInstantUpload(item)) === 'hit') return
    if (direct) {
      const digest = this.find(item.id)?.instantDigest ?? (await sha256Hex(item.file))
      await runDirect(
        item.file,
        item.parentId,
        digest,
        signal,
        (session) => this.applyDirect(item.id, session),
        (loaded) => this.update(item.id, { loaded }),
      )
      return
    }
    if (!this.alive) return
    if (this.canceled.has(item.id)) {
      this.update(item.id, { status: 'canceled', message: undefined })
      return
    }
    this.update(item.id, { status: 'creating', message: undefined })
    // Do not abort creation on user cancel: wait for its ID, then cancel that exact session.
    let session = await this.gateway.create(item.file, item.parentId, item.mode)
    if (!this.alive) return
    this.update(item.id, { sessionId: session.id, serverStatus: session.status })
    if (this.canceled.has(item.id)) {
      await this.cancelRemote(item.id, session.id)
      return
    }
    this.update(item.id, { status: 'uploading' })
    if (session.status !== 'ready') {
      if (session.mode === 'full') {
        session = await this.gateway.content(session.id, item.file, signal, (bytes) =>
          this.update(item.id, { loaded: Math.min(bytes, item.file.size) }),
        )
      } else {
        if (
          session.chunk_size <= 0 ||
          session.chunk_count !== Math.ceil(item.file.size / session.chunk_size)
        )
          throw new Error('服务端返回的分片参数不合法。')
        for (let index = 0; index < session.chunk_count; index++) {
          if (!this.alive || signal.aborted) throw new DOMException('上传已停止', 'AbortError')
          const offset = index * session.chunk_size
          const blob = item.file.slice(
            offset,
            Math.min(offset + session.chunk_size, item.file.size),
          )
          session = await this.gateway.chunk(session.id, index, blob, signal, (bytes) =>
            this.update(item.id, { loaded: Math.min(offset + bytes, item.file.size) }),
          )
        }
      }
    }
    if (!this.alive) return
    if (this.canceled.has(item.id)) {
      await this.cancelRemote(item.id, session.id)
      return
    }
    if (session.status !== 'ready' && session.status !== 'completed')
      throw new Error('服务端尚未确认内容完整，请查询会话状态。')
    this.update(item.id, {
      status: 'confirming',
      loaded: item.file.size,
      serverStatus: session.status,
    })
    session = await this.gateway.complete(session.id)
    if (!this.alive) return
    if (this.canceled.has(item.id) && session.status !== 'completed') {
      await this.cancelRemote(item.id, session.id)
      return
    }
    this.applySession(item.id, session)
  }

  // 秒传预检的三种结果必须分开处理：
  //   hit   → 服务端已经创建 File，结束；
  //   miss  → 明确没有命中，可以安全地走普通上传；
  //   throw → 只有明确失败才拒绝；网络/5xx 等结果未知会先用同一个幂等键重试。
  private async tryInstantUpload(item: Transfer): Promise<'hit' | 'miss'> {
    let digest = item.instantDigest
    if (!digest) {
      try {
        digest = await sha256Hex(item.file)
      } catch {
        // 本地摘要失败说明请求根本没发出去，普通上传是安全的。
        return 'miss'
      }
      if (!this.alive) return 'miss'
      this.update(item.id, { instantDigest: digest })
    }
    if (this.canceled.has(item.id)) return 'miss'

    for (let attempt = 0; attempt < INSTANT_CONFIRM_ATTEMPTS; attempt++) {
      try {
        const result = await this.gateway.instant(
          item.parentId,
          item.file.name,
          item.file.size,
          digest,
          item.id,
        )
        if (!this.alive) return 'miss'
        // 只有明确的 {"instant":false} 才是“未命中”。缺字段、空响应或
        // 命中却没有 file_id 都不能证明服务端没有创建文件，按结果未知重试。
        if (typeof result?.instant !== 'boolean') {
          throw new TypeError('instant upload returned an invalid response')
        }
        if (!result.instant) return 'miss'
        if (!result.file_id) {
          throw new TypeError('instant upload hit without a file id')
        }
        // 命中说明服务端已经创建了 File，取消已经无法撤销这次元数据写入。
        // 仍然展示完成结果，避免目录里出现一个界面上看不见的文件。
        this.update(item.id, {
          status: 'completed',
          loaded: item.file.size,
          serverStatus: 'completed',
          resultFileId: result.file_id,
          instant: true,
          instantUnknown: false,
          message: '秒传完成：服务端已有相同内容。',
        })
        this.published(item.parentId)
        return 'hit'
      } catch (error) {
        if (!this.alive) return 'miss'
        if (!isUnknownInstantError(error)) {
          if (error instanceof ApiError) throw new InstantUploadRejectedError(error)
          throw error
        }
        if (attempt < INSTANT_CONFIRM_ATTEMPTS - 1) {
          await new Promise((resolve) => setTimeout(resolve, INSTANT_RETRY_DELAYS[attempt] ?? 0))
          if (!this.alive) return 'miss'
        }
      }
    }

    throw new InstantUploadUnconfirmedError()
  }

  private async cancelRemote(id: string, sessionId: string) {
    if (!this.alive) return
    try {
      if (this.find(id)?.direct) this.applyDirect(id, await cancelDirect(sessionId))
      else this.applySession(id, await this.gateway.cancel(sessionId))
    } catch (error) {
      this.update(id, {
        status: 'failed',
        message: `取消结果未确认。${errorMessage(error)} 请查询状态。`,
      })
    }
  }

  private applyDirect(id: string, session: DirectSession) {
    const completed = session.status === 'completed'
    this.update(id, {
      direct: true,
      sessionId: session.id,
      resultFileId: session.result_file_id,
      status: completed
        ? 'completed'
        : session.status === 'canceled' ||
            session.status === 'expired' ||
            session.status === 'failed'
          ? session.status
          : session.status === 'uploading'
            ? 'uploading'
            : 'confirming',
      message:
        session.status === 'verifying'
          ? 'COS 已合并，等待 worker 完整校验后发布…'
          : session.last_error,
    })
    if (completed) {
      const item = this.find(id)
      if (item) this.published(item.parentId)
    }
  }

  private applySession(id: string, session: UploadSession) {
    if (!this.alive) return
    const terminal = ['completed', 'canceled', 'expired'].includes(session.status)
    this.update(id, {
      serverStatus: session.status,
      loaded: session.actual_size,
      resultFileId: session.result_file_id,
      status: terminal ? (session.status as 'completed' | 'canceled' | 'expired') : 'failed',
      message: terminal
        ? undefined
        : session.status === 'ready'
          ? '内容已接收，可确认完成或取消上传。'
          : '会话尚未完成。可取消后重新选择文件；不会自动恢复字节传输。',
    })
    if (session.status === 'completed') this.published(session.parent_id)
  }

  private find(id: string) {
    return this.items.find((item) => item.id === id)
  }
  private emit() {
    this.listeners.forEach((listener) => listener())
  }
  private update(id: string, patch: Partial<Transfer>) {
    if (!this.alive) return
    this.items = this.items.map((item) => (item.id === id ? { ...item, ...patch } : item))
    this.emit()
  }
}
