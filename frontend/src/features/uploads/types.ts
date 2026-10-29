export const MAX_UPLOAD_BYTES = 32 * 1024 * 1024
export type UploadMode = 'full' | 'chunked'
export type ModeChoice = 'auto' | 'direct' | UploadMode
export type SessionStatus = 'pending' | 'receiving' | 'ready' | 'completed' | 'canceled' | 'expired'
export type UploadSession = {
  id: string
  parent_id: string
  name: string
  expected_size: number
  actual_size: number
  mode: UploadMode
  chunk_size: number
  chunk_count: number
  status: SessionStatus
  expires_at: string
  result_file_id: string | null
}
export type InstantUploadResult = {
  instant: boolean
  file_id?: string
}
export type TransferStatus =
  | 'queued'
  | 'creating'
  | 'uploading'
  | 'confirming'
  | 'canceling'
  | 'checking'
  | 'completed'
  | 'canceled'
  | 'expired'
  | 'failed'
export type Transfer = {
  id: string
  file: File
  parentId: string
  mode: UploadMode
  status: TransferStatus
  loaded: number
  direct?: boolean
  choice?: ModeChoice
  sessionId?: string
  serverStatus?: SessionStatus
  resultFileId?: string | null
  instant?: boolean
  // 秒传请求的摘要和“结果未知”标记：结果未知时不能直接开始普通上传，
  // 必须用同一个幂等键重新确认。
  instantDigest?: string
  instantUnknown?: boolean
  message?: string
}
export type UploadGateway = {
  directCapabilities?: () => Promise<{ enabled: boolean; max_bytes: number }>
  create: (file: File, parentId: string, mode: UploadMode) => Promise<UploadSession>
  instant: (
    parentId: string,
    name: string,
    size: number,
    sha256: string,
    idempotencyKey: string,
  ) => Promise<InstantUploadResult>
  content: (
    id: string,
    blob: Blob,
    signal: AbortSignal,
    progress: (bytes: number) => void,
  ) => Promise<UploadSession>
  chunk: (
    id: string,
    index: number,
    blob: Blob,
    signal: AbortSignal,
    progress: (bytes: number) => void,
  ) => Promise<UploadSession>
  complete: (id: string) => Promise<UploadSession>
  cancel: (id: string) => Promise<UploadSession>
  get: (id: string) => Promise<UploadSession>
}

export function resolveMode(size: number, choice: ModeChoice): UploadMode {
  return choice === 'auto' || choice === 'direct'
    ? size <= 1024 * 1024
      ? 'full'
      : 'chunked'
    : choice
}

export function isActive(status: TransferStatus) {
  return ['queued', 'creating', 'uploading', 'confirming', 'canceling', 'checking'].includes(status)
}
