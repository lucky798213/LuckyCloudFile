import { useSearchParams } from 'react-router'
import { isRecord, readStored, writeStored } from '../../shared/lib/storage'
import type { Crumb, FileEntry } from './types'

const key = 'lucky.paths'
type Paths = Record<string, Crumb[]>

function isPaths(value: unknown): value is Paths {
  return (
    isRecord(value) &&
    Object.values(value).every(
      (path) =>
        Array.isArray(path) &&
        path.every(
          (item) => isRecord(item) && typeof item.id === 'string' && typeof item.name === 'string',
        ),
    )
  )
}

export function clearPaths() {
  writeStored(key, null)
}

export function useFileNavigation() {
  const [params, setParams] = useSearchParams()
  const parentId = params.get('parent_id') ?? ''
  const rawPage = Number(params.get('page') ?? 1)
  const page = Number.isSafeInteger(rawPage) && rawPage > 0 ? rawPage : 1
  const paths = readStored(key, isPaths) ?? {}
  const crumbs: Crumb[] = parentId
    ? Object.hasOwn(paths, parentId)
      ? paths[parentId]!
      : [{ id: parentId, name: '当前目录' }]
    : []

  function go(id: string, trail: Crumb[]) {
    if (id) {
      paths[id] = trail
      writeStored(key, paths)
    }
    setParams(id ? { parent_id: id } : {})
  }

  return {
    parentId,
    page,
    crumbs,
    enter: (folder: FileEntry) => go(folder.id, [...crumbs, { id: folder.id, name: folder.name }]),
    root: () => go('', []),
    ancestor: (index: number) => {
      const crumb = crumbs[index]
      if (crumb) go(crumb.id, crumbs.slice(0, index + 1))
    },
    setPage: (next: number) =>
      setParams(parentId ? { parent_id: parentId, page: String(next) } : { page: String(next) }),
  }
}
