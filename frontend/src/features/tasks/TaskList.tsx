import { useMutation } from '@tanstack/react-query'
import { Link } from 'react-router'
import { useTasks } from './context'
import { taskLabels } from './status'
import { filesApi } from '../files/api'
import { FileGlyph } from '../files/FileGlyph'
import { Button } from '../../shared/ui/Button'
import { EmptyState, ErrorNotice } from '../../shared/ui/Feedback'
import { Icon } from '../../shared/ui/Icon'
import { formatBytes } from '../../shared/lib/format'
import styles from './tasks.module.css'

const stages: Record<string, string> = {
  prepare: '准备文件',
  compress: '压缩内容',
  publish: '发布文件',
  finalize: '完成收尾',
  verify: '校验内容',
  completed: '完成',
  failed: '失败',
}

export function TaskList() {
  const { records, queries, clearFinished } = useTasks()
  const download = useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) => filesApi.download(id, name),
  })
  return (
    <>
      <section className={styles.panel} aria-label="后台任务列表">
        <div className={styles.header}>
          <div>
            <h2>本次浏览器会话记录</h2>
            <p>刷新可恢复，退出登录后清除 · 非服务端完整历史</p>
          </div>
          <Button
            variant="ghost"
            onClick={clearFinished}
            disabled={
              !queries.some(
                (query) => query.data && ['completed', 'failed'].includes(query.data.status),
              )
            }
          >
            清除已结束
          </Button>
        </div>
        {download.isError && <ErrorNotice error={download.error} />}
        {records.length === 0 ? (
          <EmptyState
            icon="tasks"
            title="暂时没有后台任务"
            description="在文件列表勾选普通文件，点击“校验文件”或“打包 ZIP”，就可以在这里跟踪进度。"
          />
        ) : (
          <ul className={styles.list}>
            {records.map((record, index) => {
              const query = queries[index]!
              const task = query.data
              const kind = record.kind ?? 'archive'
              return (
                <li key={record.id}>
                  <FileGlyph entry={{ name: record.name, kind: 'file' }} />
                  <div className={styles.body}>
                    <div className={styles.title}>
                      <h3>{record.name}</h3>
                      {task && (
                        <span
                          className={`${styles.badge} ${task.status === 'completed' ? styles.success : task.status === 'failed' ? styles.failed : ''}`}
                        >
                          {taskLabels[task.status]}
                        </span>
                      )}
                    </div>
                    <p>
                      {new Date(record.createdAt).toLocaleString('zh-CN')} 提交 ·{' '}
                      {kind === 'verify' ? '内容校验' : 'ZIP 打包'}
                      {task ? ` · ${stages[task.stage] ?? task.stage}` : ' · 正在查询状态'}
                    </p>
                    {task && (
                      <div className={styles.workerInfo}>
                        <span>worker {task.worker_id || '等待领取'}</span>
                        <span>attempt {task.attempt}</span>
                      </div>
                    )}
                    {task && (
                      <div className={styles.progressInfo}>
                        {kind === 'verify' ? (
                          task.result?.sha256 ? (
                            <>
                              <span className={styles.hashLabel}>
                                <Icon name="shield" size={14} />
                                SHA-256
                              </span>
                              <code className={styles.hash}>{task.result.sha256}</code>
                              <span>{formatBytes(task.result.size)}</span>
                            </>
                          ) : (
                            <span>
                              {task.status === 'completed'
                                ? '校验完成，结果已写入服务端。'
                                : task.status === 'failed'
                                  ? '校验失败，详情见下方错误。'
                                  : 'worker 会读取内容并计算完整 SHA-256。'}
                            </span>
                          )
                        ) : task.total_files > 0 ? (
                          <>
                            <progress
                              aria-label={`${record.name} 打包进度`}
                              max={task.total_files}
                              value={task.processed_files}
                            />
                            <span>
                              {task.processed_files} / {task.total_files} 个文件
                            </span>
                          </>
                        ) : (
                          <span>
                            {task.status === 'completed'
                              ? '结果文件已发布'
                              : '进度快照暂不可用，任务状态仍以服务端为准。'}
                          </span>
                        )}
                      </div>
                    )}
                    {task?.error && (
                      <p className={styles.taskError}>
                        {task.status === 'retry_wait' ? '上次尝试：' : ''}
                        {task.error}
                      </p>
                    )}
                    {query.isError && (
                      <ErrorNotice
                        error={query.error}
                        retry={() => {
                          void query.refetch()
                        }}
                      />
                    )}
                  </div>
                  <div className={styles.actions}>
                    {kind === 'archive' && task?.status === 'completed' && task.result_file_id && (
                      <Button
                        onClick={() =>
                          download.mutate({ id: task.result_file_id, name: record.name })
                        }
                        busy={download.isPending}
                      >
                        <Icon name="download" size={16} />
                        下载 ZIP
                      </Button>
                    )}
                    <Link
                      to={
                        record.parentId
                          ? `/files?parent_id=${encodeURIComponent(record.parentId)}`
                          : '/files'
                      }
                    >
                      {kind === 'verify' ? '打开所在目录' : '打开输出目录'}
                      <Icon name="chevron" size={13} />
                    </Link>
                  </div>
                </li>
              )
            })}
          </ul>
        )}
      </section>
      <p className={styles.note}>
        <Icon name="clock" size={16} />
        未结束任务每两秒查询一次。服务重启后，任务恢复由后端负责。
      </p>
    </>
  )
}
