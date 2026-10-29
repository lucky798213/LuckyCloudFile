import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { useQueries, useQueryClient } from '@tanstack/react-query'
import { TaskContext } from './context'
import { tasksApi } from './api'
import type { TaskRecord } from './types'
import { isRecord, readStored, writeStored } from '../../shared/lib/storage'
import { taskPollInterval } from './status'
import { fileKeys } from '../files/queries'

const key = 'lucky.tasks'
export function TaskProvider({ children }: { children: ReactNode }) {
  const [records, setRecords] = useState(
    () =>
      readStored(
        key,
        (value): value is TaskRecord[] =>
          Array.isArray(value) &&
          value.every(
            (item) =>
              isRecord(item) &&
              typeof item.id === 'string' &&
              typeof item.name === 'string' &&
              typeof item.parentId === 'string' &&
              typeof item.createdAt === 'string' &&
              (item.kind === undefined || item.kind === 'archive' || item.kind === 'verify'),
          ),
      ) ?? [],
  )
  const client = useQueryClient()
  const queries = useQueries({
    queries: records.map((record) => ({
      queryKey: ['tasks', record.id],
      queryFn: ({ signal }: { signal: AbortSignal }) => tasksApi.get(record.id, signal),
      refetchInterval: (query: { state: { data?: { status: string }; error: Error | null } }) =>
        taskPollInterval(query.state.data?.status, query.state.error),
    })),
  })
  const completed = queries
    .flatMap((query) => (query.data?.status === 'completed' ? [query.data.task_id] : []))
    .join(',')
  useEffect(() => {
    if (completed) void client.invalidateQueries({ queryKey: fileKeys.all })
  }, [client, completed])
  function save(next: TaskRecord[]) {
    setRecords(next)
    writeStored(key, next)
  }
  return (
    <TaskContext
      value={{
        records,
        queries,
        add: (record) =>
          setRecords((current) => {
            const next = [record, ...current.filter((item) => item.id !== record.id)]
            writeStored(key, next)
            return next
          }),
        clearFinished: () =>
          save(
            records.filter(
              (_, index) => !['completed', 'failed'].includes(queries[index]?.data?.status ?? ''),
            ),
          ),
      }}
    >
      {children}
    </TaskContext>
  )
}
