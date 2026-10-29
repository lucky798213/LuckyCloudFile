import { createContext, useContext, useSyncExternalStore } from 'react'
import type { UploadManager } from './manager'

export const UploadContext = createContext<UploadManager | null>(null)

export function useUploads() {
  const manager = useContext(UploadContext)
  if (!manager) throw new Error('上传模块必须位于 UploadProvider 内。')
  const transfers = useSyncExternalStore(manager.subscribe, manager.getSnapshot)
  return { manager, transfers }
}
