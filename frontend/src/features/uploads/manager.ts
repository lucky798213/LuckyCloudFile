import { errorMessage } from '../../shared/http/errors'
import { sha256Hex } from './hash'
import { MAX_UPLOAD_BYTES, resolveMode } from './types'
import type { ModeChoice, Transfer, UploadGateway, UploadSession } from './types'

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
    if (files.some((file) => file.size > MAX_UPLOAD_BYTES))
      throw new Error('单个文件不能超过 32 MiB。')
    this.items = [
      ...this.items,
      ...files.map((file) => ({
        id: this.makeId(),
        file,
        parentId,
        mode: resolveMode(file.size, mode),
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
      this.applySession(id, await this.gateway.get(item.sessionId))
    } catch (error) {
      this.update(id, { status: 'failed', message: errorMessage(error) })
    }
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
    this.update(item.id, { status: 'creating', message: '正在计算内容摘要…' })
    if (await this.tryInstantUpload(item)) return
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

  // Best-effort preflight. Hashing or the instant request failing must never
  // block the normal upload path, so every failure is treated as a miss.
  private async tryInstantUpload(item: Transfer) {
    try {
      const digest = await sha256Hex(item.file)
      if (!this.alive || this.canceled.has(item.id)) return false
      const result = await this.gateway.instant(
        item.parentId,
        item.file.name,
        item.file.size,
        digest,
        item.id,
      )
      if (!this.alive) return false
      if (!result.instant || !result.file_id) return false
      // 命中说明服务端已经创建了 File，取消已经无法撤销这次元数据写入。
      // 仍然展示完成结果，避免目录里出现一个界面上看不见的文件。
      this.update(item.id, {
        status: 'completed',
        loaded: item.file.size,
        serverStatus: 'completed',
        resultFileId: result.file_id,
        instant: true,
        message: '秒传完成：服务端已有相同内容。',
      })
      this.published(item.parentId)
      return true
    } catch {
      return false
    }
  }

  private async cancelRemote(id: string, sessionId: string) {
    if (!this.alive) return
    try {
      this.applySession(id, await this.gateway.cancel(sessionId))
    } catch (error) {
      this.update(id, {
        status: 'failed',
        message: `取消结果未确认。${errorMessage(error)} 请查询状态。`,
      })
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
