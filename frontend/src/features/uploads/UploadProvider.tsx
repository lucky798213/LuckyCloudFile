import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { UploadContext } from './context'
import { UploadManager } from './manager'
import { uploadApi } from './api'
import { fileKeys } from '../files/queries'

export function UploadProvider({ children }: { children: ReactNode }) {
  const client = useQueryClient()
  const [manager] = useState(
    () =>
      new UploadManager(uploadApi, () => {
        void client.invalidateQueries({ queryKey: fileKeys.all })
      }),
  )
  useEffect(() => {
    manager.start()
    return () => manager.stop()
  }, [manager])
  return <UploadContext value={manager}>{children}</UploadContext>
}
