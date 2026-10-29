import { useLayoutEffect, useId, useRef } from 'react'
import type { ReactNode } from 'react'
import { Icon } from './Icon'
import styles from './ui.module.css'

export function Modal({
  title,
  description,
  onClose,
  children,
  busy = false,
}: {
  title: string
  description?: string
  onClose: () => void
  children: ReactNode
  busy?: boolean
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const id = useId()
  useLayoutEffect(() => {
    const dialog = ref.current!
    const previous = document.activeElement
    dialog.showModal()
    dialog.querySelector<HTMLElement>('input:not([type="hidden"]), select, textarea')?.focus()
    return () => {
      dialog.close()
      if (previous instanceof HTMLElement && previous.isConnected) previous.focus()
    }
  }, [])
  return (
    <dialog
      ref={ref}
      className={styles.modal}
      aria-labelledby={`${id}-title`}
      aria-describedby={description ? `${id}-description` : undefined}
      onCancel={(event) => {
        event.preventDefault()
        if (!busy) onClose()
      }}
    >
      <header className={styles.modalHeader}>
        <h2 id={`${id}-title`}>{title}</h2>
        <button
          type="button"
          className={styles.iconButton}
          aria-label="关闭对话框"
          disabled={busy}
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </header>
      {description && (
        <p id={`${id}-description`} className={styles.description}>
          {description}
        </p>
      )}
      {children}
    </dialog>
  )
}
