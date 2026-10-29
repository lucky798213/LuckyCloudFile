import { useState } from 'react'
import { useFiles } from './queries'
import type { Crumb } from './types'
import { ErrorNotice, LoadingState, Pagination } from '../../shared/ui/Feedback'
import { Icon } from '../../shared/ui/Icon'
import styles from './files.module.css'
import ui from '../../shared/ui/ui.module.css'

export function DirectoryPicker({
  onChange,
  excludedId,
}: {
  onChange: (id: string) => void
  excludedId?: string
}) {
  const [trail, setTrail] = useState<Crumb[]>([])
  const [page, setPage] = useState(1)
  const parentId = trail.at(-1)?.id ?? ''
  const query = useFiles(parentId, page)
  const directories = query.data?.items.filter((item) => item.kind === 'directory') ?? []
  function go(next: Crumb[]) {
    setTrail(next)
    setPage(1)
    onChange(next.at(-1)?.id ?? '')
  }
  return (
    <div className={styles.picker}>
      <nav className={styles.pickerHeader} aria-label="目标文件夹路径">
        <button type="button" onClick={() => go([])}>
          全部文件
        </button>
        {trail.map((crumb, index) => (
          <span key={crumb.id}>
            <Icon name="chevron" size={12} />
            <button type="button" onClick={() => go(trail.slice(0, index + 1))}>
              {crumb.name}
            </button>
          </span>
        ))}
      </nav>
      {query.isPending && <LoadingState />}
      {query.isError && (
        <ErrorNotice
          error={query.error}
          retry={() => {
            void query.refetch()
          }}
        />
      )}
      {query.data && (
        <>
          <div className={styles.pickerItems}>
            {directories.map((directory) => (
              <button
                key={directory.id}
                type="button"
                disabled={directory.id === excludedId}
                onClick={() => go([...trail, { id: directory.id, name: directory.name }])}
              >
                <Icon name="folder" size={18} />
                {directory.name}
                <Icon name="chevron" size={14} />
              </button>
            ))}
            {directories.length === 0 && (
              <p className={ui.description} style={{ padding: 12 }}>
                本页没有子文件夹{query.data.total > 20 ? '，可切换分页查找。' : '。'}
              </p>
            )}
          </div>
          <Pagination page={page} total={query.data.total} onChange={setPage} />
        </>
      )}
      <p className={ui.description} style={{ padding: '0 12px 12px' }}>
        将使用：{trail.at(-1)?.name ?? '全部文件（根目录）'}
      </p>
    </div>
  )
}
