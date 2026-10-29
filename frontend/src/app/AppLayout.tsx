import { useState } from 'react'
import { NavLink, Outlet } from 'react-router'
import { authSession, useSession } from '../features/auth/session'
import { authApi } from '../features/auth/api'
import { useFiles } from '../features/files/queries'
import { useMutation } from '@tanstack/react-query'
import { Icon } from '../shared/ui/Icon'
import { ErrorNotice } from '../shared/ui/Feedback'
import { Button } from '../shared/ui/Button'
import { useUploads } from '../features/uploads/context'
import { isActive } from '../features/uploads/types'
import styles from './layout.module.css'

export function AppLayout() {
  const session = useSession()
  const [open, setOpen] = useState(false)
  const { transfers } = useUploads()
  const activeCount = transfers.filter((item) => isActive(item.status)).length
  const validation = useFiles('', 1)
  const logout = useMutation({ mutationFn: authApi.logout, onSuccess: () => authSession.set(null) })
  return (
    <div className={styles.app}>
      <a href="#main-content" className={styles.skipLink}>
        跳到主要内容
      </a>
      <button
        className={`${styles.scrim} ${open ? styles.scrimOpen : ''}`}
        tabIndex={open ? 0 : -1}
        aria-label="关闭导航"
        onClick={() => setOpen(false)}
      />
      <aside className={`${styles.sidebar} ${open ? styles.sidebarOpen : ''}`}>
        <NavLink to="/files" className={styles.brand} onClick={() => setOpen(false)}>
          <span className={styles.brandIcon}>
            <Icon name="cloud" size={26} />
          </span>
          <span>
            lucky<span className={styles.brandLight}>CloudFile</span>
            <small>你的私人文件空间</small>
          </span>
        </NavLink>
        <p className={styles.sectionLabel}>工作空间</p>
        <nav aria-label="主导航" className={styles.navigation}>
          <NavLink
            to="/files"
            className={({ isActive }) => (isActive ? styles.active : '')}
            onClick={() => setOpen(false)}
          >
            <Icon name="files" />
            我的文件
          </NavLink>
          <NavLink
            to="/transfers"
            className={({ isActive }) => (isActive ? styles.active : '')}
            onClick={() => setOpen(false)}
          >
            <Icon name="upload" />
            传输列表{activeCount > 0 && <span className={styles.navCount}>{activeCount}</span>}
          </NavLink>
          <NavLink
            to="/tasks"
            className={({ isActive }) => (isActive ? styles.active : '')}
            onClick={() => setOpen(false)}
          >
            <Icon name="tasks" />
            任务中心
          </NavLink>
        </nav>
        <div className={styles.sideNote}>
          <span className={styles.noteGlyph}>
            <Icon name="shield" size={22} />
          </span>
          <h3>简单，也安心。</h3>
          <p>
            文件按账号独立隔离。
            <br />
            把注意力留给重要的事。
          </p>
          <span className={styles.noteLine} />
        </div>
        <div className={styles.account}>
          <span className={styles.avatar}>{session?.username.slice(0, 1).toUpperCase()}</span>
          <div>
            <strong>{session?.username}</strong>
            <span>个人账户</span>
          </div>
          <button
            onClick={() => logout.mutate()}
            disabled={logout.isPending}
            aria-label="退出登录"
            title="退出登录"
          >
            <Icon name="logout" size={18} />
          </button>
        </div>
      </aside>
      <div className={styles.mainColumn}>
        <header className={styles.topbar}>
          <div>
            <button
              className={styles.mobileMenu}
              aria-label="打开导航"
              aria-expanded={open}
              onClick={() => setOpen(true)}
            >
              <Icon name="menu" />
            </button>
            <span className={styles.spaceLabel}>个人工作台</span>
            <span className={styles.topSeparator}>/</span>
            <span className={styles.topMuted}>PRIVATE SPACE</span>
          </div>
          <span className={styles.privateBadge}>
            <Icon name="shield" size={14} />
            仅你可见
          </span>
        </header>
        <main id="main-content" className={styles.content} tabIndex={-1}>
          {validation.isError && (
            <ErrorNotice
              error={validation.error}
              retry={() => {
                void validation.refetch()
              }}
            />
          )}
          {logout.isError && (
            <div>
              <ErrorNotice error={logout.error} />
              <Button variant="ghost" onClick={() => authSession.set(null)}>
                仅清除本地登录（服务端会话未确认撤销）
              </Button>
            </div>
          )}
          <Outlet />
        </main>
        <footer className={styles.footer}>
          <span>luckyCloudFile</span>
          <span>留一份从容，给每一个文件。</span>
        </footer>
      </div>
    </div>
  )
}
