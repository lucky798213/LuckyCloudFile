import { describe, expect, it, vi } from 'vitest'
import { HttpClient } from './client'
import { ApiError } from './errors'

describe('HTTP boundary', () => {
  it('sends a Bearer header and handles 204 without parsing JSON', async () => {
    const transport = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    const client = new HttpClient(transport)
    client.configureAuth({ getToken: () => 'sample-token', onUnauthorized: vi.fn() })
    expect(await client.request('/files/item', { method: 'DELETE' })).toBeUndefined()
    const [url, options] = transport.mock.calls[0]!
    expect(url).toBe('/api/files/item')
    expect(new Headers(options?.headers).get('Authorization')).toBe('Bearer sample-token')
    expect(String(url)).not.toContain('sample-token')
    expect(transport).toHaveBeenCalledTimes(1)
  })

  it('invalidates only the authenticated session that received 401', async () => {
    let token = 'account-a'
    let respond: (response: Response) => void = () => undefined
    const transport = vi.fn<typeof fetch>().mockImplementation(
      () =>
        new Promise((resolve) => {
          respond = resolve
        }),
    )
    const client = new HttpClient(transport)
    const unauthorized = vi.fn()
    client.configureAuth({ getToken: () => token, onUnauthorized: unauthorized })
    const oldRequest = client.request('/files')
    token = 'account-b'
    respond(Response.json({ code: 'unauthorized', message: 'expired' }, { status: 401 }))
    await expect(oldRequest).rejects.toMatchObject({ status: 401 })
    expect(unauthorized).not.toHaveBeenCalled()
    const currentRequest = client.request('/files')
    respond(Response.json({ code: 'unauthorized' }, { status: 401 }))
    await expect(currentRequest).rejects.toBeInstanceOf(ApiError)
    expect(unauthorized).toHaveBeenCalledTimes(1)
  })

  it('does not clear authentication on 500 or retry a mutation', async () => {
    const transport = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json({ code: 'internal_error' }, { status: 500 }))
    const unauthorized = vi.fn()
    const client = new HttpClient(transport)
    client.configureAuth({ getToken: () => 'valid-token', onUnauthorized: unauthorized })
    await expect(
      client.request('/archive-tasks', { method: 'POST', body: { name: 'test.zip' } }),
    ).rejects.toMatchObject({ status: 500 })
    expect(transport).toHaveBeenCalledTimes(1)
    expect(unauthorized).not.toHaveBeenCalled()
  })

  it('returns binary downloads and normalizes non-JSON errors', async () => {
    const transport = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response('file-content'))
      .mockResolvedValueOnce(new Response('gateway failed', { status: 502 }))
    const client = new HttpClient(transport)
    const blob = await client.request<Blob>('/files/id', {}, 'blob')
    expect(await blob.text()).toBe('file-content')
    await expect(client.request('/files')).rejects.toMatchObject({
      code: 'request_failed',
      status: 502,
    })
  })
})
