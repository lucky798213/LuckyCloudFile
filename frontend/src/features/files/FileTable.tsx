import { useMutation } from '@tanstack/react-query'
import { filesApi } from './api'
import { FileGlyph } from './FileGlyph'
import { FileActionMenu } from './FileActionMenu'
import { filePresentation } from './presentation'
import type { FileEntry } from './types'
import type { FileOperation } from './FileOperationDialog'
import { formatBytes } from '../../shared/lib/format'
import { ErrorNotice } from '../../shared/ui/Feedback'
import { Icon } from '../../shared/ui/Icon'
import styles from './files.module.css'
import ui from '../../shared/ui/ui.module.css'

type Props = {
  entries: FileEntry[]
  selected: Set<string>
  onSelect: (ids: Set<string>) => void
  onEnter: (entry: FileEntry) => void
  onAction: (operation: FileOperation) => void
}

export function FileTable({ entries, selected, onSelect, onEnter, onAction }: Props) {
  const download = useMutation({
    mutationFn: (entry: FileEntry) => filesApi.download(entry.id, entry.name),
  })
  const files = entries.filter((entry) => entry.kind === 'file')
  const allSelected = files.length > 0 && files.every((file) => selected.has(file.id))
  function select(id: string) {
    const next = new Set(selected)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    onSelect(next)
  }
  return (
    <>
      {download.isError && <ErrorNotice error={download.error} />}
      <div className={styles.tableScroll}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>
                <input
                  type="checkbox"
                  aria-label="选择本页全部普通文件"
                  checked={allSelected}
                  disabled={files.length === 0}
                  onChange={() =>
                    onSelect(allSelected ? new Set() : new Set(files.map((file) => file.id)))
                  }
                />
              </th>
              <th scope="col">名称</th>
              <th scope="col" className={styles.typeColumn}>
                文件类型
              </th>
              <th scope="col">大小</th>
              <th scope="col">
                <span className="sr-only">操作</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {entries.map((entry) => (
              <tr key={entry.id} className={selected.has(entry.id) ? styles.selected : undefined}>
                <td>
                  {entry.kind === 'file' && (
                    <input
                      type="checkbox"
                      aria-label={`选择 ${entry.name}`}
                      checked={selected.has(entry.id)}
                      onChange={() => select(entry.id)}
                    />
                  )}
                </td>
                <td>
                  <div className={styles.nameCell}>
                    <FileGlyph entry={entry} />
                    <button
                      className={styles.filename}
                      onClick={() => {
                        if (entry.kind === 'directory') onEnter(entry)
                        else download.mutate(entry)
                      }}
                      title={entry.name}
                      disabled={entry.kind === 'file' && download.isPending}
                    >
                      {entry.name}
                    </button>
                  </div>
                </td>
                <td className={`${styles.typeColumn} ${ui.muted}`}>
                  {filePresentation(entry).label}
                </td>
                <td className={ui.muted}>
                  {entry.kind === 'directory' ? '—' : formatBytes(entry.size)}
                </td>
                <td>
                  <div className={styles.rowActions}>
                    {entry.kind === 'file' && (
                      <button
                        className={ui.iconButton}
                        title="下载"
                        aria-label={`下载 ${entry.name}`}
                        onClick={() => download.mutate(entry)}
                        disabled={download.isPending}
                      >
                        <Icon name="download" size={17} />
                      </button>
                    )}
                    <FileActionMenu entry={entry} onAction={onAction} />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}
