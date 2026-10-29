export type FileEntry = {
  id: string
  name: string
  size: number
  kind: 'file' | 'directory'
  parent_id: string | null
}
export type FilePage = { items: FileEntry[]; total: number; page: number; page_size: number }
export type Crumb = { id: string; name: string }
