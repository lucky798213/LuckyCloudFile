export function formatBytes(bytes: number) {
  if (bytes === 0) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  const units = ['KiB', 'MiB', 'GiB']
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)) - 1, 2)
  return `${(bytes / 1024 ** (index + 1)).toLocaleString('zh-CN', { maximumFractionDigits: 1 })} ${units[index]}`
}

export function validName(name: string) {
  return (
    name.trim().length > 0 &&
    !['.', '..'].includes(name) &&
    !/[\\/]/.test(name) &&
    !name.includes('\0') &&
    [...name].length <= 255
  )
}

export function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = name
  document.body.append(anchor)
  anchor.click()
  anchor.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
