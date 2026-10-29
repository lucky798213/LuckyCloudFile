import { expect, it, vi } from 'vitest'
import { api } from '../../shared/http/client'
import { runDirect } from './direct-manager'

it('reselects a persisted session and sends only missing parts directly to COS', async () => {
  localStorage.clear()
  const file = new File(['0123456789'], 'resume.bin')
  const key = `lcf-direct:${JSON.stringify(['root', file.name, file.size, 'digest'])}`
  localStorage.setItem(key, JSON.stringify({ requestId: 'request', id: 'session' }))
  const session = {
    id: 'session',
    status: 'uploading',
    expected_size: 10,
    part_size: 8,
    uploaded_parts: [{ number: 1, size: 8, etag: 'first' }],
  }
  const request = vi.spyOn(api, 'request').mockImplementation(async (path, options) => {
    if (path.endsWith('/part-urls')) {
      expect(options?.body).toEqual({ part_numbers: [2] })
      return { '2': { url: 'https://bucket.cos.example/part?signature=test', headers: {} } }
    }
    if (path.endsWith('/complete'))
      return { ...session, status: 'completed', result_file_id: 'file' }
    return session
  })
  const fetchPart = vi.fn().mockResolvedValue(new Response('', { headers: { ETag: 'second' } }))
  vi.stubGlobal('fetch', fetchPart)
  try {
    const result = await runDirect(
      file,
      'root',
      'digest',
      new AbortController().signal,
      vi.fn(),
      vi.fn(),
    )
    expect(result.status).toBe('completed')
    expect(fetchPart).toHaveBeenCalledTimes(1)
    expect(fetchPart.mock.calls[0]?.[1].body.size).toBe(2)
    expect(localStorage.getItem(key)).toBeNull()
    expect(request.mock.calls.some(([path]) => path === '/direct-upload-sessions')).toBe(false)
  } finally {
    request.mockRestore()
    vi.unstubAllGlobals()
  }
})
