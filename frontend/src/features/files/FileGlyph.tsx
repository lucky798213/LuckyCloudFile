import { Icon } from '../../shared/ui/Icon'
import type { FileEntry } from './types'
import { filePresentation } from './presentation'
import styles from './files.module.css'

export function FileGlyph({ entry }: { entry: Pick<FileEntry, 'kind' | 'name'> }) {
  const view = filePresentation(entry)
  return (
    <span className={`${styles.glyph} ${styles[view.tone]}`}>
      <Icon name={view.icon} size={22} />
    </span>
  )
}
