import { useId, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { filesApi } from './api'
import { fileKeys } from './queries'
import { clearPaths } from './navigation'
import type { FileEntry } from './types'
import { DirectoryPicker } from './DirectoryPicker'
import { Modal } from '../../shared/ui/Modal'
import { Button } from '../../shared/ui/Button'
import { ErrorNotice } from '../../shared/ui/Feedback'
import { validName } from '../../shared/lib/format'
import ui from '../../shared/ui/ui.module.css'

export type FileOperation =
  { kind: 'create' } | { kind: 'rename' | 'move' | 'copy' | 'delete'; entry: FileEntry }
const titles = {
  create: '新建文件夹',
  rename: '重命名',
  move: '移动到',
  copy: '复制文件',
  delete: '确认删除',
}

export function FileOperationDialog({
  operation,
  parentId,
  onClose,
}: {
  operation: FileOperation
  parentId: string
  onClose: () => void
}) {
  const entry = operation.kind === 'create' ? null : operation.entry
  const [name, setName] = useState(
    operation.kind === 'copy' ? `副本-${entry!.name}` : (entry?.name ?? ''),
  )
  const [destination, setDestination] = useState('')
  const nameId = useId()
  const client = useQueryClient()
  const mutation = useMutation({
    mutationFn: async () => {
      switch (operation.kind) {
        case 'create':
          return filesApi.createDirectory(parentId, name)
        case 'rename':
          return filesApi.rename(operation.entry.id, name)
        case 'move':
          return filesApi.move(operation.entry.id, destination)
        case 'copy':
          return filesApi.copy(operation.entry.id, destination, name)
        case 'delete':
          return filesApi.delete(operation.entry.id)
      }
    },
    onSuccess: () => {
      if (operation.kind === 'rename' || operation.kind === 'move' || operation.kind === 'delete')
        clearPaths()
      void client.invalidateQueries({ queryKey: fileKeys.all })
      onClose()
    },
  })
  const needsName = ['create', 'rename', 'copy'].includes(operation.kind)
  const needsDestination = operation.kind === 'move' || operation.kind === 'copy'
  const description =
    operation.kind === 'delete'
      ? `即将删除“${entry!.name}”。此操作不能撤销。${entry!.kind === 'directory' ? '只能删除空文件夹，请先清理其中内容。' : '文件删除成功后，磁盘空间可能稍后释放。'}`
      : operation.kind === 'move'
        ? `将“${entry!.name}”移入下方选定的文件夹。`
        : undefined
  return (
    <Modal
      title={titles[operation.kind]}
      description={description}
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
        {needsName && (
          <div className={ui.field}>
            <label htmlFor={nameId}>{operation.kind === 'create' ? '文件夹名称' : '名称'}</label>
            <input
              id={nameId}
              aria-describedby={`${nameId}-help`}
              autoFocus
              required
              value={name}
              onChange={(event) => setName(event.target.value)}
              disabled={mutation.isPending}
              placeholder="输入名称"
            />
            <small id={`${nameId}-help`}>最多 255 个字符，不支持 / 和 \ 路径分隔符。</small>
          </div>
        )}
        {needsDestination && (
          <fieldset
            disabled={mutation.isPending}
            style={{ border: 0, padding: 0, margin: 0, minWidth: 0 }}
          >
            <legend className="sr-only">选择目标目录</legend>
            <DirectoryPicker
              onChange={setDestination}
              excludedId={operation.kind === 'move' ? entry!.id : undefined}
            />
          </fieldset>
        )}
        {mutation.isError && <ErrorNotice error={mutation.error} />}
        <div className={ui.footer}>
          <Button onClick={onClose} disabled={mutation.isPending}>
            取消
          </Button>
          <Button
            type="submit"
            variant={operation.kind === 'delete' ? 'danger' : 'primary'}
            busy={mutation.isPending}
            disabled={needsName && !validName(name)}
          >
            {operation.kind === 'delete' ? '确认删除' : '确认'}
          </Button>
        </div>
      </form>
    </Modal>
  )
}
