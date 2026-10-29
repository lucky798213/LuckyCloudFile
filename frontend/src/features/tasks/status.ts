import { ApiError } from '../../shared/http/errors'
import type { TaskStatus } from './types'

export const taskLabels: Record<TaskStatus, string> = {
  queued: '排队中',
  running: '执行中',
  retry_wait: '等待后端重试',
  completed: '已完成',
  failed: '执行失败',
}

export function taskPollInterval(status: string | undefined, error: unknown): number | false {
  if (status === 'completed' || status === 'failed') return false
  if (error instanceof ApiError && error.status === 404) return false
  return 2000
}
