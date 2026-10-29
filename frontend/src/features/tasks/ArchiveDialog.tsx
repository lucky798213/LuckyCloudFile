import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { tasksApi } from './api'
import { useTasks } from './context'
import type { FileEntry } from '../files/types'
import { Modal } from '../../shared/ui/Modal'
import { Button } from '../../shared/ui/Button'
import { ErrorNotice } from '../../shared/ui/Feedback'
import { formatBytes, validName } from '../../shared/lib/format'
import { MAX_UPLOAD_BYTES } from '../uploads/types'
import ui from '../../shared/ui/ui.module.css'

export function ArchiveDialog({
  files,
  parentId,
  onClose,
}: {
  files: FileEntry[]
  parentId: string
  onClose: () => void
}) {
  const [name, setName] = useState('归档.zip')
  const registry = useTasks()
  const total = files.reduce((sum, file) => sum + file.size, 0)
  const withinLimits = files.length > 0 && files.length <= 100 && total <= MAX_UPLOAD_BYTES
  const mutation = useMutation({
    mutationFn: () =>
      tasksApi.create(
        files.map((file) => file.id),
        parentId,
        name,
      ),
    onSuccess: (result) => {
      registry.add({
        id: result.task_id,
        name,
        parentId,
        createdAt: new Date().toISOString(),
        kind: 'archive',
      })
      onClose()
    },
  })
  return (
    <Modal
      title="打包为 ZIP"
      description={`已选择 ${files.length} 个普通文件，共 ${formatBytes(total)}。结果保存在当前目录，可在任务中心查看进度。`}
      onClose={onClose}
      busy={mutation.isPending}
    >
      <form
        className={ui.form}
        onSubmit={(event) => {
          event.preventDefault()
          if (!mutation.isPending) mutation.mutate()
        }}
      >
        <label className={ui.field}>
          压缩文件名称
          <input
            autoFocus
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={mutation.isPending}
            required
          />
        </label>
        <p className={ui.description}>
          最多 100 个普通文件，源文件总大小及 ZIP 产物均不能超过 32 MiB。不支持递归打包文件夹。
        </p>
        {!withinLimits && <ErrorNotice error={new Error('所选文件数量或总大小超出打包限制。')} />}
        {mutation.isError && (
          <>
            <ErrorNotice error={mutation.error} />
            <p className={ui.description}>
              若网络中断导致提交结果未确认，请先检查当前目录；重复提交可能创建另一项任务。
            </p>
          </>
        )}
        <div className={ui.footer}>
          <Button onClick={onClose} disabled={mutation.isPending}>
            取消
          </Button>
          <Button
            variant="primary"
            type="submit"
            busy={mutation.isPending}
            disabled={!withinLimits || !validName(name)}
          >
            创建打包任务
          </Button>
        </div>
      </form>
    </Modal>
  )
}
