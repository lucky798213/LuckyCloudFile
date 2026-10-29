import { createContext, useContext } from 'react'
import type { UseQueryResult } from '@tanstack/react-query'
import type { Task, TaskRecord } from './types'

type TaskRegistry = {
  records: TaskRecord[]
  queries: UseQueryResult<Task>[]
  add: (record: TaskRecord) => void
  clearFinished: () => void
}
export const TaskContext = createContext<TaskRegistry | null>(null)

export function useTasks() {
  const registry = useContext(TaskContext)
  if (!registry) throw new Error('任务模块必须位于 TaskProvider 内。')
  return registry
}
