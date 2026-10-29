import type { ReactNode } from 'react'
import { errorMessage } from '../http/errors'
import { Button } from './Button'
import { Icon } from './Icon'
import type { IconName } from './Icon'
import styles from './ui.module.css'

export function ErrorNotice({ error, retry }: { error: unknown; retry?: () => void }) {
  return (
    <div className={styles.error} role="alert">
      <Icon name="alert" size={18} />
      <span>{errorMessage(error)}</span>
      {retry && (
        <Button variant="ghost" onClick={retry}>
          重试
        </Button>
      )}
    </div>
  )
}

export function EmptyState({
  icon = 'folder',
  title,
  description,
  children,
}: {
  icon?: IconName
  title: string
  description: string
  children?: ReactNode
}) {
  return (
    <div className={styles.empty}>
      <span className={styles.emptyIcon}>
        <Icon name={icon} size={36} />
      </span>
      <h2>{title}</h2>
      <p>{description}</p>
      {children}
    </div>
  )
}

export function LoadingState() {
  return (
    <div className={styles.loading} role="status">
      <span className={styles.spinner} />
      正在加载…
    </div>
  )
}

export function PageHeading({
  eyebrow,
  title,
  description,
  children,
}: {
  eyebrow: string
  title: string
  description: string
  children?: ReactNode
}) {
  return (
    <header className={styles.pageHeading}>
      <div>
        <p className={styles.eyebrow}>{eyebrow}</p>
        <h1>{title}</h1>
        <p className={styles.description}>{description}</p>
      </div>
      {children && <div className={styles.headingActions}>{children}</div>}
    </header>
  )
}

export function Pagination({
  page,
  total,
  pageSize = 20,
  onChange,
}: {
  page: number
  total: number
  pageSize?: number
  onChange: (page: number) => void
}) {
  const pages = Math.max(1, Math.ceil(total / pageSize))
  return (
    <nav className={styles.pagination} aria-label="分页">
      <span>
        共 {total} 项 · 第 {page} / {pages} 页
      </span>
      <div>
        <Button disabled={page <= 1} onClick={() => onChange(page - 1)}>
          上一页
        </Button>
        <Button disabled={page >= pages} onClick={() => onChange(page + 1)}>
          下一页
        </Button>
      </div>
    </nav>
  )
}
