import { describe, expect, it } from 'vitest'
import { taskLabels, taskPollInterval } from './status'
import { ApiError } from '../../shared/http/errors'

describe('server-authoritative task polling', () => {
  it.each(['queued', 'running', 'retry_wait', undefined])('polls %s every two seconds', (status) =>
    expect(taskPollInterval(status, null)).toBe(2000),
  )
  it.each(['completed', 'failed'])('stops polling terminal state %s', (status) =>
    expect(taskPollInterval(status, null)).toBe(false),
  )
  it('stops on 404, not on transient failures, and labels backend retry correctly', () => {
    expect(taskPollInterval(undefined, new ApiError('task_not_found', 'not found', 404))).toBe(
      false,
    )
    expect(taskPollInterval(undefined, new ApiError('internal_error', 'unavailable', 500))).toBe(
      2000,
    )
    expect(taskLabels.retry_wait).toBe('等待后端重试')
  })
})
