import { useUploads } from './context'
import { isActive } from './types'
import type { TransferStatus } from './types'
import { FileGlyph } from '../files/FileGlyph'
import { formatBytes } from '../../shared/lib/format'
import { Button } from '../../shared/ui/Button'
import { Icon } from '../../shared/ui/Icon'
import { EmptyState } from '../../shared/ui/Feedback'
import styles from './uploads.module.css'

const labels: Record<TransferStatus, string> = {
  queued: '等待上传',
  creating: '创建上传会话',
  uploading: '正在上传',
  confirming: '正在确认完成',
  canceling: '正在取消',
  checking: '正在查询状态',
  completed: '上传完成',
  canceled: '已取消',
  expired: '会话已失效',
  failed: '需要处理',
}

export function TransferList() {
  const { manager, transfers } = useUploads()
  const active = transfers.filter((item) => isActive(item.status)).length
  return (
    <section className={styles.panel} aria-label="上传传输列表">
      <div className={styles.panelHeader}>
        <div>
          <h2>
            上传记录 <span>{transfers.length}</span>
          </h2>
          <p>{active ? `${active} 个文件正在队列中` : '当前没有正在进行的上传'}</p>
        </div>
        <Button
          variant="ghost"
          onClick={() => manager.clearFinished()}
          disabled={
            !transfers.some((item) => ['completed', 'canceled', 'expired'].includes(item.status))
          }
        >
          清除已结束
        </Button>
      </div>
      {transfers.length === 0 ? (
        <EmptyState
          icon="upload"
          title="还没有上传记录"
          description="在“我的文件”中选择或拖入文件，传输进度会显示在这里。此列表不保存跨刷新传输。"
        />
      ) : (
        <ul className={styles.transfers}>
          {transfers.map((item) => {
            const percent =
              item.file.size === 0
                ? item.status === 'completed'
                  ? 100
                  : 0
                : Math.min(100, Math.round((item.loaded / item.file.size) * 100))
            return (
              <li key={item.id}>
                <FileGlyph entry={{ name: item.file.name, kind: 'file' }} />
                <div className={styles.transferContent}>
                  <div className={styles.transferTitle}>
                    <strong>{item.file.name}</strong>
                    <span
                      className={`${styles.status} ${item.status === 'completed' ? styles.success : item.status === 'failed' || item.status === 'expired' ? styles.warning : ''}`}
                    >
                      {item.status === 'completed' && <Icon name="check" size={13} />}
                      {item.status === 'completed' && item.instant
                        ? '秒传完成'
                        : labels[item.status]}
                    </span>
                  </div>
                  <p>
                    {formatBytes(item.loaded)} / {formatBytes(item.file.size)} ·{' '}
                    {item.instant ? '秒传' : item.mode === 'full' ? '整文件' : '分片上传'}
                    {isActive(item.status) ? ` · ${percent}%` : ''}
                  </p>
                  <div
                    className={styles.progress}
                    role="progressbar"
                    aria-label={`${item.file.name} 上传进度`}
                    aria-valuemin={0}
                    aria-valuemax={100}
                    aria-valuenow={percent}
                  >
                    <span
                      style={{
                        width: `${percent}%`,
                        background: item.status === 'completed' ? 'var(--success)' : undefined,
                      }}
                    />
                  </div>
                  {item.message && (
                    <p className={styles.transferMessage} role="status">
                      {item.message}
                    </p>
                  )}
                </div>
                <div className={styles.transferActions}>
                  {item.status === 'failed' && item.instantUnknown && (
                    <Button onClick={() => manager.retry(item.id)}>重试确认</Button>
                  )}
                  {item.status === 'failed' && item.sessionId && (
                    <>
                      <Button
                        variant="ghost"
                        onClick={() => {
                          void manager.check(item.id)
                        }}
                      >
                        查询状态
                      </Button>
                      {item.serverStatus === 'ready' && (
                        <Button
                          onClick={() => {
                            void manager.confirm(item.id)
                          }}
                        >
                          确认完成
                        </Button>
                      )}
                    </>
                  )}
                  {!['completed', 'canceled', 'expired', 'canceling', 'checking'].includes(
                    item.status,
                  ) &&
                    !(item.status === 'failed' && !item.sessionId) && (
                      <Button variant="ghost" onClick={() => manager.cancel(item.id)}>
                        取消
                      </Button>
                    )}
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
