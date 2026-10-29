import { act, fireEvent, render, screen } from '@testing-library/react'
import type { PropsWithChildren } from 'react'
import { MemoryRouter } from 'react-router'
import { expect, it, vi } from 'vitest'
import { uploadApi } from './api'
import { UploadContext } from './context'
import { UploadDialog } from './UploadDialog'
import { UploadManager } from './manager'

// Dialog presentation is unrelated to upload-mode capability selection.
vi.mock('../../shared/ui/Modal', () => ({
  Modal: ({ children }: PropsWithChildren) => <div>{children}</div>,
}))

it.each([true, false])(
  'offers proxy chunks only for non-COS policies (COS=%s)',
  async (enabled) => {
    let resolve!: (value: { enabled: boolean; max_bytes: number }) => void
    vi.spyOn(uploadApi, 'directCapabilities').mockReturnValue(
      new Promise((done) => {
        resolve = done
      }),
    )
    const manager = new UploadManager(uploadApi, vi.fn())
    const enqueue = vi.spyOn(manager, 'enqueue').mockImplementation(() => {})
    const files = [new File(['hello'], 'hello.txt')]
    render(
      <MemoryRouter>
        <UploadContext value={manager}>
          <UploadDialog files={files} parentId="root" onClose={vi.fn()} />
        </UploadContext>
      </MemoryRouter>,
    )
    const chunkOption = screen.getByRole('option', { name: /代理分片上传/ })
    expect(chunkOption).toBeDisabled() // No unsupported choice while loading.
    await act(async () => {
      resolve({ enabled, max_bytes: 128 * 1024 * 1024 })
    })
    if (enabled) expect(chunkOption).toBeDisabled()
    else expect(chunkOption).toBeEnabled()
    expect(screen.getByRole('combobox')).toHaveValue('auto')
    expect(screen.getByRole('option', { name: /COS 直传/ })).toBeEnabled()
    expect(screen.getByRole('option', { name: '整文件上传' })).toBeEnabled()
    fireEvent.change(screen.getByRole('combobox'), {
      target: { value: enabled ? 'full' : 'chunked' },
    })
    fireEvent.click(screen.getByRole('button', { name: '开始上传' }))
    expect(enqueue).toHaveBeenCalledWith(files, 'root', enabled ? 'full' : 'chunked')
  },
)
