import type { IconName } from '../../shared/ui/Icon'
import type { FileEntry } from './types'

export function filePresentation(entry: Pick<FileEntry, 'kind' | 'name'>): {
  icon: IconName
  tone: string
  label: string
} {
  if (entry.kind === 'directory') return { icon: 'folder', tone: 'folder', label: '文件夹' }
  const ext = entry.name.includes('.') ? (entry.name.split('.').pop()?.toLowerCase() ?? '') : ''
  if (['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'heic'].includes(ext))
    return { icon: 'image', tone: 'image', label: '图片' }
  if (['zip', 'tar', 'gz', 'rar', '7z'].includes(ext))
    return { icon: 'archive', tone: 'archive', label: '压缩文件' }
  if (['go', 'ts', 'tsx', 'js', 'json', 'html', 'css', 'py', 'yaml', 'yml', 'sql'].includes(ext))
    return { icon: 'code', tone: 'code', label: '代码文件' }
  return { icon: 'file', tone: 'document', label: ext ? `${ext.toUpperCase()} 文件` : '文件' }
}
