import { describe, expect, it, vi } from 'vitest'
import { ApiError } from '../../shared/http/errors'
import { UploadManager } from './manager'
import { MAX_UPLOAD_BYTES, resolveMode } from './types'
import type { UploadGateway, UploadSession } from './types'

const base: UploadSession = {
  id: 'session',
  parent_id: 'root',
  name: 'sample.txt',
  expected_size: 5,
  actual_size: 0,
  mode: 'full',
  chunk_size: 0,
  chunk_count: 0,
  status: 'pending',
  expires_at: new Date(Date.now() + 60_000).toISOString(),
  result_file_id: null,
}
function gateway(overrides: Partial<UploadGateway> = {}): UploadGateway {
  return {
    create: vi.fn<UploadGateway['create']>(async () => ({ ...base })),
    instant: vi.fn<UploadGateway['instant']>(async () => ({ instant: false })),
    content: vi.fn<UploadGateway['content']>(async () => ({
      ...base,
      status: 'ready',
      actual_size: 5,
    })),
    chunk: vi.fn<UploadGateway['chunk']>(async () => ({
      ...base,
      status: 'ready',
      actual_size: 5,
    })),
    complete: vi.fn<UploadGateway['complete']>(async () => ({
      ...base,
      status: 'completed',
      actual_size: 5,
      result_file_id: 'file',
    })),
    cancel: vi.fn<UploadGateway['cancel']>(async () => ({ ...base, status: 'canceled' })),
    get: vi.fn<UploadGateway['get']>(async () => ({ ...base })),
    ...overrides,
  }
}
const file = () => new File(['hello'], 'sample.txt')
async function waitFor(check: () => boolean, timeout = 1000) {
  await vi.waitFor(() => expect(check()).toBe(true), { interval: 5, timeout })
}

describe('upload scheduler', () => {
  it('does not claim an unknown created session was remotely canceled without its ID', async () => {
    const api = gateway({
      create: vi.fn<UploadGateway['create']>(async () => {
        throw new TypeError('connection lost')
      }),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'failed')
    manager.cancel(manager.getSnapshot()[0]!.id)
    expect(manager.getSnapshot()[0]?.status).toBe('failed')
    expect(api.cancel).not.toHaveBeenCalled()
  })

  it('treats server expiry as authoritative even when expires_at is still in the future', async () => {
    const api = gateway({
      content: vi.fn<UploadGateway['content']>(async () => {
        throw new TypeError('server restarted')
      }),
      get: vi.fn<UploadGateway['get']>(async () => ({ ...base, status: 'expired' })),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'failed')
    await manager.check(manager.getSnapshot()[0]!.id)
    expect(manager.getSnapshot()[0]?.status).toBe('expired')
  })

  it('completes from the instant endpoint without creating an upload session', async () => {
    const api = gateway({
      instant: vi.fn<UploadGateway['instant']>(async () => ({
        instant: true,
        file_id: 'instant-file',
      })),
    })
    const published = vi.fn()
    const manager = new UploadManager(api, published)
    manager.enqueue([file()], 'root', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')

    const item = manager.getSnapshot()[0]!
    expect(item.instant).toBe(true)
    expect(item.resultFileId).toBe('instant-file')
    expect(item.loaded).toBe(5)
    expect(api.instant).toHaveBeenCalledTimes(1)
    expect(api.create).not.toHaveBeenCalled()
    expect(api.content).not.toHaveBeenCalled()
    expect(api.complete).not.toHaveBeenCalled()
    expect(published).toHaveBeenCalledWith('root')
  })

  it('falls back to a normal upload when the instant probe misses', async () => {
    const api = gateway()
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')

    expect(api.instant).toHaveBeenCalledTimes(1)
    expect(api.create).toHaveBeenCalledTimes(1)
    expect(api.content).toHaveBeenCalledTimes(1)
    expect(api.complete).toHaveBeenCalledTimes(1)
    expect(manager.getSnapshot()[0]?.instant).toBeUndefined()
  })

  it('shows an instant hit that lands while the user is canceling', async () => {
    let finish: (result: { instant: boolean; file_id?: string }) => void = () => undefined
    const api = gateway({
      instant: vi.fn<UploadGateway['instant']>(
        () =>
          new Promise((resolve) => {
            finish = resolve
          }),
      ),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => vi.mocked(api.instant).mock.calls.length === 1)
    manager.cancel(manager.getSnapshot()[0]!.id)
    finish({ instant: true, file_id: 'raced-file' })
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')
    expect(manager.getSnapshot()[0]?.instant).toBe(true)
    expect(manager.getSnapshot()[0]?.resultFileId).toBe('raced-file')
  })

  it('retries an uncertain instant response with the same idempotency key', async () => {
    const instant = vi
      .fn<UploadGateway['instant']>()
      .mockRejectedValueOnce(new TypeError('response lost'))
      .mockResolvedValueOnce({ instant: true, file_id: 'confirmed-file' })
    const api = gateway({ instant })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')

    const item = manager.getSnapshot()[0]!
    expect(item.instant).toBe(true)
    expect(item.resultFileId).toBe('confirmed-file')
    expect(api.create).not.toHaveBeenCalled()
    expect(api.instant).toHaveBeenCalledTimes(2)
    const keys = vi.mocked(api.instant).mock.calls.map((call) => call[4])
    expect(new Set(keys).size).toBe(1)
  })

  it('stops before a normal upload while the instant result is unknown, then confirms with the same key', async () => {
    const instant = vi.fn<UploadGateway['instant']>(async () => {
      throw new TypeError('offline')
    })
    const api = gateway({ instant })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'failed', 3000)

    const item = manager.getSnapshot()[0]!
    expect(item.instantUnknown).toBe(true)
    expect(item.message).toContain('重试确认')
    expect(api.create).not.toHaveBeenCalled()
    expect(api.instant).toHaveBeenCalledTimes(3)
    const firstKey = vi.mocked(api.instant).mock.calls[0]![4]

    instant.mockResolvedValue({ instant: true, file_id: 'confirmed-file' })
    manager.retry(item.id)
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')
    expect(manager.getSnapshot()[0]?.instant).toBe(true)
    expect(api.create).not.toHaveBeenCalled()
    expect(vi.mocked(api.instant).mock.calls.at(-1)?.[4]).toBe(firstKey)
  })

  it('does not fall back to a normal upload when the instant request is rejected', async () => {
    const api = gateway({
      instant: vi.fn<UploadGateway['instant']>(async () => {
        throw new ApiError('insufficient_storage', 'quota', 507)
      }),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'failed')

    expect(api.instant).toHaveBeenCalledTimes(1)
    expect(api.create).not.toHaveBeenCalled()
    expect(manager.getSnapshot()[0]?.instantUnknown).toBeUndefined()
    expect(manager.getSnapshot()[0]?.message).toContain('未开始普通上传')
  })

  it('chooses modes at the boundary and rejects >32 MiB before creating a session', () => {
    expect(resolveMode(0, 'auto')).toBe('full')
    expect(resolveMode(1024 * 1024, 'auto')).toBe('full')
    expect(resolveMode(1024 * 1024 + 1, 'auto')).toBe('chunked')
    expect(resolveMode(2, 'chunked')).toBe('chunked')
    const api = gateway()
    const manager = new UploadManager(api, vi.fn())
    expect(() =>
      manager.enqueue([new File([new Uint8Array(MAX_UPLOAD_BYTES + 1)], 'large')], '', 'auto'),
    ).toThrow('32 MiB')
    expect(api.create).not.toHaveBeenCalled()
  })

  it('waits for publishing acknowledgement instead of declaring 100% bytes as success', async () => {
    let finish: (session: UploadSession) => void = () => undefined
    const api = gateway({
      complete: vi.fn<UploadGateway['complete']>(
        () =>
          new Promise((resolve) => {
            finish = resolve
          }),
      ),
    })
    const published = vi.fn()
    const manager = new UploadManager(api, published)
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'confirming')
    expect(published).not.toHaveBeenCalled()
    finish({ ...base, status: 'completed', actual_size: 5, result_file_id: 'file' })
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')
    expect(published).toHaveBeenCalledTimes(1)
  })

  it('uses server chunk geometry, serial chunks, and serial files', async () => {
    const events: string[] = []
    const api = gateway({
      create: vi.fn<UploadGateway['create']>(async (selected) => {
        events.push(`create:${selected.name}`)
        return { ...base, mode: 'chunked', chunk_size: 2, chunk_count: 3 }
      }),
      chunk: vi.fn<UploadGateway['chunk']>(async (_id, index, blob) => {
        events.push(`chunk:${index}:${blob.size}`)
        return {
          ...base,
          mode: 'chunked',
          chunk_size: 2,
          chunk_count: 3,
          status: index === 2 ? 'ready' : 'receiving',
        }
      }),
      complete: vi.fn<UploadGateway['complete']>(async () => {
        events.push('complete')
        return { ...base, status: 'completed' }
      }),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file(), new File(['hello'], 'second.txt')], '', 'chunked')
    await waitFor(() => manager.getSnapshot().every((item) => item.status === 'completed'))
    expect(events).toEqual([
      'create:sample.txt',
      'chunk:0:2',
      'chunk:1:2',
      'chunk:2:1',
      'complete',
      'create:second.txt',
      'chunk:0:2',
      'chunk:1:2',
      'chunk:2:1',
      'complete',
    ])
  })

  it('completes a zero-byte chunked file without sending a chunk', async () => {
    const api = gateway({
      create: vi.fn<UploadGateway['create']>(async () => ({
        ...base,
        mode: 'chunked',
        expected_size: 0,
        status: 'ready',
        chunk_size: 1024,
        chunk_count: 0,
      })),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([new File([], 'empty.txt')], '', 'chunked')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')
    expect(api.chunk).not.toHaveBeenCalled()
    expect(api.complete).toHaveBeenCalledTimes(1)
  })

  it('cancels during session creation only after the exact session ID is known', async () => {
    let created: (session: UploadSession) => void = () => undefined
    const api = gateway({
      create: vi.fn<UploadGateway['create']>(
        () =>
          new Promise((resolve) => {
            created = resolve
          }),
      ),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => vi.mocked(api.instant).mock.calls.length === 1)
    manager.cancel(manager.getSnapshot()[0]!.id)
    expect(manager.getSnapshot()[0]?.status).toBe('canceling')
    created(base)
    await waitFor(() => manager.getSnapshot()[0]?.status === 'canceled')
    expect(api.content).not.toHaveBeenCalled()
    expect(api.cancel).toHaveBeenCalledWith('session')
  })

  it('cancels in-flight bytes, but accepts completed as the authoritative cancellation result', async () => {
    const api = gateway({
      content: vi.fn<UploadGateway['content']>(
        (_id, _blob, signal) =>
          new Promise((_resolve, reject) =>
            signal.addEventListener('abort', () =>
              reject(new DOMException('stopped', 'AbortError')),
            ),
          ),
      ),
      cancel: vi.fn<UploadGateway['cancel']>(async () => ({
        ...base,
        status: 'completed',
        result_file_id: 'already-published',
      })),
    })
    const published = vi.fn()
    const manager = new UploadManager(api, published)
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'uploading')
    manager.cancel(manager.getSnapshot()[0]!.id)
    await waitFor(() => manager.getSnapshot()[0]?.status === 'completed')
    expect(published).toHaveBeenCalledTimes(1)
  })

  it('does not retry failed writes and reconciles an uncertain completion by querying', async () => {
    const api = gateway({
      complete: vi.fn<UploadGateway['complete']>(async () => {
        throw new TypeError('lost response')
      }),
      get: vi.fn<UploadGateway['get']>(async () => ({
        ...base,
        status: 'completed',
        result_file_id: 'result',
      })),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    await waitFor(() => manager.getSnapshot()[0]?.status === 'failed')
    expect(api.complete).toHaveBeenCalledTimes(1)
    await manager.check(manager.getSnapshot()[0]!.id)
    expect(manager.getSnapshot()[0]?.status).toBe('completed')
    expect(api.create).toHaveBeenCalledTimes(1)
  })

  it('does not send follow-up writes after authentication teardown', async () => {
    let created: (session: UploadSession) => void = () => undefined
    const api = gateway({
      create: vi.fn<UploadGateway['create']>(
        () =>
          new Promise((resolve) => {
            created = resolve
          }),
      ),
    })
    const manager = new UploadManager(api, vi.fn())
    manager.enqueue([file()], '', 'full')
    manager.stop()
    created(base)
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(api.content).not.toHaveBeenCalled()
    expect(api.complete).not.toHaveBeenCalled()
  })
})
