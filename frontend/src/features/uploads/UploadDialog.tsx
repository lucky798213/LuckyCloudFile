import { useState } from 'react'
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
  const oversized = files.some((file) => file.size > MAX_UPLOAD_BYTES)
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
            <option value="auto">自动选择（≤ 1 MiB 整文件，其余分片）</option>
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
              {file.size > MAX_UPLOAD_BYTES && <span className={styles.tooLarge}>超出上限</span>}
            </li>
          ))}
        </ul>
        {oversized && <ErrorNotice error={new Error('包含超过 32 MiB 的文件，请重新选择。')} />}
        {error !== null && <ErrorNotice error={error} />}
        <p className={ui.description}>
          刷新或关闭页面会中断上传，不支持自动续传。可在
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
