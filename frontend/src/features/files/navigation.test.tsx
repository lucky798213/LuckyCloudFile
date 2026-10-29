import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { useFileNavigation } from './navigation'

function BrowserFixture() {
  const navigation = useFileNavigation()
  return (
    <>
      <span>{navigation.parentId || 'root'}</span>
      <span>{navigation.crumbs.map((item) => item.name).join('/') || '全部文件'}</span>
      <button
        onClick={() =>
          navigation.enter({
            id: 'folder',
            name: '资料',
            kind: 'directory',
            size: 0,
            parent_id: null,
          })
        }
      >
        进入资料
      </button>
      <button onClick={navigation.root}>返回根目录</button>
    </>
  )
}

describe('navigation without a metadata API', () => {
  it('stores only a visited breadcrumb path and supports returning to root', async () => {
    sessionStorage.clear()
    const user = userEvent.setup()
    render(
      <MemoryRouter>
        <BrowserFixture />
      </MemoryRouter>,
    )
    await user.click(screen.getByRole('button', { name: '进入资料' }))
    expect(screen.getByText('资料')).toBeInTheDocument()
    expect(sessionStorage.getItem('lucky.paths')).toContain('folder')
    await user.click(screen.getByRole('button', { name: '返回根目录' }))
    expect(screen.getByText('root')).toBeInTheDocument()
  })
  it('does not invent ancestors for an unknown deep link', () => {
    sessionStorage.clear()
    render(
      <MemoryRouter initialEntries={['/files?parent_id=unseen']}>
        <BrowserFixture />
      </MemoryRouter>,
    )
    expect(screen.getByText('当前目录')).toBeInTheDocument()
    expect(screen.getByText('unseen')).toBeInTheDocument()
  })
})
