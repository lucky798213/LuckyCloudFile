import { api } from '../../shared/http/client'
import type { InstantUploadResult, UploadGateway, UploadSession } from './types'

const path = (id: string) => `/upload-sessions/${encodeURIComponent(id)}`

export const uploadApi: UploadGateway = {
  create: (file, parentId, mode) =>
    api.request<UploadSession>('/upload-sessions', {
      method: 'POST',
      body: { parent_id: parentId, name: file.name, size: file.size, mode },
    }),
  instant: (parentId, name, size, sha256, idempotencyKey) =>
    api.request<InstantUploadResult>('/files/instant-upload', {
      method: 'POST',
      headers: { 'Idempotency-Key': idempotencyKey },
      body: { parent_id: parentId, name, size, sha256 },
    }),
  content: (id, blob, signal, progress) =>
    api.upload<UploadSession>(`${path(id)}/content`, blob, signal, progress),
  chunk: (id, index, blob, signal, progress) =>
    api.upload<UploadSession>(`${path(id)}/chunks/${index}`, blob, signal, progress),
  complete: (id) => api.request<UploadSession>(`${path(id)}/complete`, { method: 'POST' }),
  cancel: (id) => api.request<UploadSession>(path(id), { method: 'DELETE' }),
  get: (id) => api.request<UploadSession>(path(id)),
}
