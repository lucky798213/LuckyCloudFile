import { useId, useState } from 'react'
import type { FileEntry } from './types'
import type { FileOperation } from './FileOperationDialog'
import { Icon } from '../../shared/ui/Icon'
import styles from './files.module.css'
import ui from '../../shared/ui/ui.module.css'

export function FileActionMenu({
  entry,
  onAction,
}: {
  entry: FileEntry
  onAction: (operation: FileOperation) => void
}) {
  const id = useId()
  const [position, setPosition] = useState({ top: 0, left: 0 })
  function choose(kind: 'rename' | 'move' | 'copy' | 'delete', button: HTMLButtonElement) {
    button.closest<HTMLElement>('[popover]')?.hidePopover()
    onAction({ kind, entry })
  }
  return (
    <>
      <button
        type="button"
        className={ui.iconButton}
        aria-label={`${entry.name} 的更多操作`}
        popoverTarget={id}
        onClick={(event) => {
          const rect = event.currentTarget.getBoundingClientRect()
          setPosition({
            top: Math.min(rect.bottom + 5, window.innerHeight - 210),
            left: Math.max(8, rect.right - 150),
          })
        }}
      >
        <Icon name="more" size={19} />
      </button>
      <div
        id={id}
        popover="auto"
        className={styles.actionPopover}
        style={position}
        role="group"
        aria-label={`${entry.name} 操作`}
      >
        <button onClick={(event) => choose('rename', event.currentTarget)}>
          <Icon name="edit" size={16} />
          重命名
        </button>
        <button onClick={(event) => choose('move', event.currentTarget)}>
          <Icon name="move" size={16} />
          移动到
        </button>
        {entry.kind === 'file' && (
          <button onClick={(event) => choose('copy', event.currentTarget)}>
            <Icon name="copy" size={16} />
            复制文件
          </button>
        )}
        <button
          className={styles.deleteAction}
          onClick={(event) => choose('delete', event.currentTarget)}
        >
          <Icon name="trash" size={16} />
          删除
        </button>
      </div>
    </>
  )
}
