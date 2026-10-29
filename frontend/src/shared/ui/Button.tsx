import type { ButtonHTMLAttributes } from 'react'
import styles from './ui.module.css'

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: 'primary' | 'secondary' | 'ghost' | 'danger'
  busy?: boolean
}

export function Button({
  variant = 'secondary',
  busy,
  children,
  className = '',
  disabled,
  ...props
}: Props) {
  return (
    <button
      type="button"
      {...props}
      className={`${styles.button} ${styles[variant]} ${className}`}
      disabled={disabled || busy}
      aria-busy={busy || undefined}
    >
      {busy && <span className={styles.spinner} aria-hidden="true" />}
      {children}
    </button>
  )
}
