import { isRecord } from '../lib/storage'

export class ApiError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export function responseError(body: unknown, status: number) {
  return new ApiError(
    isRecord(body) && typeof body.code === 'string' ? body.code : 'request_failed',
    isRecord(body) && typeof body.message === 'string' ? body.message : `请求失败 (${status})`,
    status,
  )
}

const messages: Record<string, string> = {
  invalid_credentials: '用户名或密码不正确，请重试。',
  unauthorized: '登录已失效，请重新登录。',
  already_exists: '此目录已有同名文件或文件夹，请换一个名字。',
  directory_not_empty: '文件夹不是空的，请先删除其中的内容。',
  invalid_name: '名称不能为空、不能为 . 或 ..，也不能包含路径分隔符。',
  invalid_move: '不能将文件夹移入自身或它的子文件夹。',
  file_not_found: '文件不存在或你无权访问，请刷新列表。',
  parent_not_found: '目录不存在或你无权访问，请返回全部文件。',
  file_or_parent_not_found: '文件或目标目录不存在，或你无权访问。',
  task_not_found: '任务不存在或你无权访问。',
  not_a_directory: '目标不是文件夹。',
  is_directory: '此操作仅支持普通文件。',
  root_directory: '不能修改根目录。',
  file_too_large: '单个文件不能超过 32 MiB。',
  insufficient_storage: '账户可用容量不足，请清理文件后重试。',
  resource_locked: '目标正被其他操作或上传占用，请稍后重试。',
  resource_changed: '目标路径已发生变化，请刷新后重试。',
  task_limit: '后台任务已达到上限，请稍后再提交。',
  upload_session_expired: '上传会话已失效，请重新选择文件上传。',
  upload_session_not_found: '上传会话不存在或你无权访问。',
  upload_session_conflict: '上传状态冲突，请查询状态后重试。',
  upload_session_not_completable: '文件尚未完整接收，暂时不能确认完成。',
  size_mismatch: '实际接收大小不匹配，请重新上传。',
  network_error: '网络连接失败，请检查网络或后端代理配置。',
  internal_error: '服务暂时不可用，请稍后重试。',
}

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return messages[error.code] ?? error.message
  if (error instanceof TypeError) return messages.network_error!
  if (error instanceof Error) return error.message
  return '操作失败，请稍后重试。'
}
