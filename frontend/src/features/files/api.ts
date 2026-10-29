import { api } from '../../shared/http/client'
import { saveBlob } from '../../shared/lib/format'
import type { FileEntry, FilePage } from './types'

const filePath = (id: string) => `/files/${encodeURIComponent(id)}`

export const filesApi = {
  async list(parentId: string, page: number, signal?: AbortSignal): Promise<FilePage> {
    const query = new URLSearchParams({ parent_id: parentId, page: String(page), page_size: '20' })
    const result = await api.request<FilePage>(`/files?${query}`, { signal })
    return { ...result, items: result.items ?? [] }
  },
  createDirectory: (parentId: string, name: string) =>
    api.request<FileEntry>('/directories', { method: 'POST', body: { parent_id: parentId, name } }),
  rename: (id: string, name: string) =>
    api.request<FileEntry>(filePath(id), { method: 'PATCH', body: { name } }),
  move: (id: string, parentId: string) =>
    api.request<FileEntry>(`${filePath(id)}/move`, {
      method: 'POST',
      body: { parent_id: parentId },
    }),
  copy: (id: string, parentId: string, name: string) =>
    api.request<FileEntry>(`${filePath(id)}/copy`, {
      method: 'POST',
      body: { parent_id: parentId, name },
    }),
  delete: (id: string) => api.request<void>(filePath(id), { method: 'DELETE' }),
  async download(id: string, name: string) {
    saveBlob(await api.request<Blob>(filePath(id), {}, 'blob'), name)
  },
}
