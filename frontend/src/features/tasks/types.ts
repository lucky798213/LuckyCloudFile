export type TaskStatus = 'queued' | 'running' | 'retry_wait' | 'completed' | 'failed'
export type TaskKind = 'archive' | 'verify'
export type VerifyTaskResult = {
  file_id: string
  entity_id: number
  size: number
  sha256: string
}
export type Task = {
  task_id: string
  status: TaskStatus
  stage: string
  processed_files: number
  total_files: number
  error?: string
  result_file_id: string
  worker_id?: string
  attempt: number
  lease_until?: string
  result?: VerifyTaskResult | null
}
export type TaskRecord = {
  id: string
  name: string
  parentId: string
  createdAt: string
  kind?: TaskKind
}
