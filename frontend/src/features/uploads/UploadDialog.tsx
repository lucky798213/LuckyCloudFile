import { useEffect, useState } from 'react'
import { uploadApi } from './api'
import { Link } from 'react-router'
import { useUploads } from './context'
import { MAX_UPLOAD_BYTES, resolveMode } from './types'
import type { ModeChoice } from './types'
import { formatBytes } from '../../shared/lib/format'
import { Modal } from '../../shared/ui/Modal'
import { Button } from '../../shared/ui/Button'
import { ErrorNotice } from '../../shared/ui/Feedback'
import { FileGlyph } from '../files/FileGlyph'
import ui from '../../shared/ui/ui.module.css'
import styles from './uploads.module.css'

export function UploadDialog({
  files,
  parentId,
  onClose,
}: {
  files: File[]
  parentId: string
  onClose: () => void
}) {
  const { manager } = useUploads()
  const [mode, setMode] = useState<ModeChoice>('auto')
  const [error, setError] = useState<unknown>(null)
  const [capabilities, setCapabilities] = useState<{ enabled: boolean; max_bytes: number }>()
  useEffect(() => {
    let alive = true
    void uploadApi
      .directCapabilities?.()
      .then((value) => {
        if (alive) setCapabilities(value)
      })
      .catch(setError)
    return () => {
      alive = false
    }
  }, [])
  const limit =
    (mode === 'auto' || mode === 'direct') && capabilities?.enabled
      ? capabilities.max_bytes
      : MAX_UPLOAD_BYTES
  const oversized = files.some((file) => file.size > limit)
  return (
    <Modal
      title="上传文件"
      description={`准备将 ${files.length} 个文件上传到当前目录。上传队列在切换页面时继续运行。`}
      onClose={onClose}
    >
      <form
        className={ui.form}
        onSubmit={(event) => {
          event.preventDefault()
          try {
            manager.enqueue(files, parentId, mode)
            onClose()
          } catch (failure) {
            setError(failure)
          }
        }}
      >
        <label className={ui.field}>
          上传方式
          <select value={mode} onChange={(event) => setMode(event.target.value as ModeChoice)}>
            <option value="auto">自动选择（COS 策略直传，其他策略代理上传）</option>
            <option value="direct">COS 直传（支持重新选文件续传）</option>
            <option value="full">整文件上传</option>
            <option value="chunked">分片上传</option>
          </select>
        </label>
        <ul className={styles.uploadList}>
          {files.map((file, index) => (
            <li key={index}>
              <FileGlyph entry={{ name: file.name, kind: 'file' }} />
              <div>
                <strong>{file.name}</strong>
                <span>
                  {formatBytes(file.size)} ·{' '}
                  {resolveMode(file.size, mode) === 'full' ? '整文件' : '分片'}
                </span>
              </div>
              {file.size > limit && <span className={styles.tooLarge}>超出上限</span>}
            </li>
          ))}
        </ul>
        {oversized && (
          <ErrorNotice
            error={new Error('包含超过当前方式上限的文件，代理 32 MiB / COS 按服务端配置。')}
          />
        )}
        {error !== null && <ErrorNotice error={error} />}
        <p className={ui.description}>
          COS 直传刷新后重新选择同名、同内容文件即可补传缺片；代理上传不支持跨重启续传。可在
          <Link to="/transfers" onClick={onClose} style={{ color: 'var(--accent)' }}>
            传输列表
          </Link>
          查看进度与取消。
        </p>
        <div className={ui.footer}>
          <Button onClick={onClose}>取消</Button>
          <Button type="submit" variant="primary" disabled={oversized || files.length === 0}>
            开始上传
          </Button>
        </div>
      </form>
    </Modal>
  )
}
