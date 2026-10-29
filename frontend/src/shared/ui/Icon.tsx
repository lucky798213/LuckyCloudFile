import type { CSSProperties } from 'react'

export type IconName =
  | 'cloud'
  | 'files'
  | 'upload'
  | 'download'
  | 'tasks'
  | 'folder'
  | 'file'
  | 'image'
  | 'code'
  | 'archive'
  | 'plus'
  | 'chevron'
  | 'check'
  | 'close'
  | 'menu'
  | 'logout'
  | 'refresh'
  | 'more'
  | 'arrow'
  | 'copy'
  | 'move'
  | 'edit'
  | 'trash'
  | 'shield'
  | 'clock'
  | 'alert'

const paths: Record<IconName, string> = {
  cloud: 'M6 18a5 5 0 0 1-.5-10 7 7 0 0 1 13.5 1.5A4.5 4.5 0 0 1 18 18H6Z',
  files: 'M4 5h6l2 3h8v12H4V5Zm4-2h6l2 3h4',
  upload: 'M12 16V3m-5 5 5-5 5 5M4 15v6h16v-6',
  download: 'M12 3v13m-5-5 5 5 5-5M4 15v6h16v-6',
  tasks: 'M8 4h12v17H4V4h4m0-2h8v5H8V2Zm0 9h8m-8 5h5',
  folder: 'M3 6h7l2 3h9v11H3V6Z',
  file: 'M6 3h8l5 5v13H6V3Zm8 0v6h5M9 13h7m-7 4h5',
  image: 'M3 3h18v18H3V3Zm0 14 6-6 5 5 3-3 4 4M15 7h.01',
  code: 'm8 7-5 5 5 5m8-10 5 5-5 5m-3-14-2 18',
  archive: 'M5 3h14v18H5V3Zm6 0v3h2v3h-2v3h2v3h-2v3h2',
  plus: 'M12 5v14M5 12h14',
  chevron: 'm9 5 7 7-7 7',
  check: 'm5 12 4 4L19 6',
  close: 'm6 6 12 12M6 18 18 6',
  menu: 'M4 6h16M4 12h16M4 18h16',
  logout: 'M10 4H4v16h6m4-13 5 5-5 5M9 12h12',
  refresh: 'M20 7V3m0 4h-4M4 17v4m0-4h4M20 7a9 9 0 0 0-15 0m-1 10a9 9 0 0 0 15 0',
  more: 'M5 12h.01M12 12h.01M19 12h.01',
  arrow: 'M4 12h16m-6-6 6 6-6 6',
  copy: 'M9 9h12v12H9V9ZM5 15H3V3h12v2',
  move: 'M12 3v18M3 12h18m-5-5 5 5-5 5M7 7l-5 5 5 5',
  edit: 'm14 4 6 6M4 20l5-1L21 7l-5-5L4 14v6Z',
  trash: 'M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7',
  shield: 'M12 3 3 6v6c0 5 9 9 9 9s9-4 9-9V6l-9-3Zm-4 9 3 3 5-6',
  clock: 'M12 7v5l3 2M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z',
  alert: 'm12 3 10 18H2L12 3Zm0 5v6m0 3v.01',
}

export function Icon({
  name,
  size = 20,
  className,
  style,
}: {
  name: IconName
  size?: number
  className?: string
  style?: CSSProperties
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className={className}
      style={style}
    >
      <path d={paths[name]} />
    </svg>
  )
}
