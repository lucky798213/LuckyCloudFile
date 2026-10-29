import { api } from '../../shared/http/client'
import type { Task } from './types'

export const tasksApi = {
  create: (fileIds: string[], parentId: string, name: string) =>
    api.request<{ task_id: string }>('/archive-tasks', {
      method: 'POST',
      body: { file_ids: fileIds, parent_id: parentId, name },
    }),
  verify: (fileIds: string[]) =>
    api.request<{ task_ids: string[] }>('/content-verify-tasks', {
      method: 'POST',
      body: { file_ids: fileIds },
    }),
  get: (id: string, signal?: AbortSignal) =>
    api.request<Task>(`/tasks/${encodeURIComponent(id)}`, { signal }),
}
