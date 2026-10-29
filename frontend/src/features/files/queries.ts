import { useQuery } from '@tanstack/react-query'
import { filesApi } from './api'

export const fileKeys = {
  all: ['files'] as const,
  list: (parentId: string, page: number) => ['files', parentId, page] as const,
}

export function useFiles(parentId: string, page: number) {
  return useQuery({
    queryKey: fileKeys.list(parentId, page),
    queryFn: ({ signal }) => filesApi.list(parentId, page, signal),
  })
}
