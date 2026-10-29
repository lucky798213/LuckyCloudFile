import { useMutation } from '@tanstack/react-query'
import { tasksApi } from './api'
import { useTasks } from './context'
import type { FileEntry } from '../files/types'
import { Modal } from '../../shared/ui/Modal'
import { Button } from '../../shared/ui/Button'
import { ErrorNotice } from '../../shared/ui/Feedback'
import { formatBytes } from '../../shared/lib/format'
import ui from '../../shared/ui/ui.module.css'

export function VerifyDialog({
  files,
  parentId,
  onClose,
}: {
  files: FileEntry[]
  parentId: string
  onClose: () => void
}) {
  const registry = useTasks()
  const total = files.reduce((sum, file) => sum + file.size, 0)
  const mutation = useMutation({
    mutationFn: () => tasksApi.verify(files.map((file) => file.id)),
    onSuccess: (result) => {
      // 一个文件一条任务，接口按提交顺序返回 task_id。
      result.task_ids.forEach((id, index) => {
        const file = files[index]
        if (!file) return
        registry.add({
          id,
          name: file.name,
          parentId,
          createdAt: new Date().toISOString(),
          kind: 'verify',
        })
      })
      onClose()
    },
  })
  return (
    <Modal
      title="校验文件内容"
      description={`已选择 ${files.length} 个普通文件，共 ${formatBytes(total)}。每个文件会创建一条后台校验任务。`}
      onClose={onClose}
      busy={mutation.isPending}
    >
      <div className={ui.form}>
        <p className={ui.description}>
          worker 会重新读取文件内容并计算完整
          SHA-256，成功后回填可信摘要，让这份内容能被后续秒传命中。校验不会修改文件本身。
        </p>
        <p className={ui.description}>
          提交后可在任务中心查看由哪个 worker 执行、第几次尝试以及最终校验结果。
        </p>
        {mutation.isError && <ErrorNotice error={mutation.error} />}
        <div className={ui.footer}>
          <Button onClick={onClose} disabled={mutation.isPending}>
            取消
          </Button>
          <Button
            variant="primary"
            busy={mutation.isPending}
            disabled={files.length === 0}
            onClick={() => mutation.mutate()}
          >
            提交校验任务
          </Button>
        </div>
      </div>
    </Modal>
  )
}
