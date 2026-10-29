import { expect, it } from 'vitest'
import { ApiError, errorMessage } from './errors'

it.each([
  [409, 'already_exists', '同名'],
  [409, 'directory_not_empty', '不是空的'],
  [409, 'resource_locked', '占用'],
  [507, 'insufficient_storage', '容量不足'],
  [413, 'file_too_large', '32 MiB'],
  [401, 'unauthorized', '登录已失效'],
  [503, 'task_limit', '上限'],
] as const)('translates HTTP %s / %s without masking its meaning', (status, code, phrase) => {
  expect(errorMessage(new ApiError(code, 'server message', status))).toContain(phrase)
})
