import { test, expect } from '@playwright/test'
import type { Page } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'

const username = process.env.E2E_USERNAME
const password = process.env.E2E_PASSWORD
test.skip(
  !process.env.E2E_BASE_URL || !username || !password,
  '需要真实前后端及 E2E_BASE_URL / E2E_USERNAME / E2E_PASSWORD。',
)

async function login(page: Page, name = username!, secret = password!) {
  await page.goto('/login')
  await page.getByRole('textbox', { name: '用户名' }).fill(name)
  await page.getByLabel('密码', { exact: true }).fill(secret)
  await page.getByRole('button', { name: '进入我的空间' }).click()
  await expect(page.getByRole('heading', { name: '我的文件', exact: true })).toBeVisible()
}

async function createFolder(page: Page, name: string) {
  await page.getByRole('button', { name: '新建文件夹', exact: true }).click()
  await page.getByRole('textbox', { name: '文件夹名称' }).fill(name)
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByRole('button', { name, exact: true })).toBeVisible()
}

async function action(page: Page, name: string, operation: string) {
  await page.getByRole('button', { name: `${name} 的更多操作` }).click()
  await page
    .getByRole('group', { name: `${name} 操作`, exact: true })
    .getByRole('button', { name: operation, exact: true })
    .click()
}

async function enterFolder(page: Page, name: string) {
  await page.getByRole('button', { name, exact: true }).click()
  await expect(
    page.getByRole('navigation', { name: '文件夹路径' }).getByRole('button', { name, exact: true }),
  ).toHaveAttribute('aria-current', 'location')
  await expect(page.getByRole('region', { name: '文件列表' })).toHaveAttribute('aria-busy', 'false')
}

test('真实闭环：目录、两种上传、改名复制移动、下载、ZIP、刷新恢复与删除', async ({ page }) => {
  const suffix = Date.now().toString(36)
  const source = `前端验收-${suffix}`
  const target = `副本目录-${suffix}`
  const text = Buffer.from('你好，Ethan。\nA little space, all yours.\n')
  await login(page)
  await createFolder(page, source)
  await createFolder(page, target)
  await enterFolder(page, source)
  await page.getByLabel('选择上传文件').setInputFiles([
    { name: '说明.md', mimeType: 'text/markdown', buffer: text },
    {
      name: '大文件.bin',
      mimeType: 'application/octet-stream',
      buffer: Buffer.alloc(2 * 1024 * 1024 + 17, 83),
    },
    { name: '空文件.txt', mimeType: 'text/plain', buffer: Buffer.alloc(0) },
  ])
  await page.getByRole('button', { name: '开始上传' }).click()
  await page.getByRole('link', { name: '传输列表' }).click()
  await expect(page.getByText('上传完成', { exact: true })).toHaveCount(3)
  await page.getByRole('link', { name: '我的文件' }).click()
  await enterFolder(page, source)
  await page.screenshot({ path: 'test-results/files-populated-desktop.png', fullPage: true })
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({ path: 'test-results/files-populated-mobile.png', fullPage: true })
  await expect(page.getByRole('button', { name: '说明.md 的更多操作' })).toBeInViewport({
    ratio: 1,
  })
  await expect(page.getByRole('button', { name: '下载 说明.md', exact: true })).toBeInViewport({
    ratio: 1,
  })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  )
  await page.setViewportSize({ width: 1440, height: 1000 })
  await action(page, '说明.md', '重命名')
  await page.getByRole('textbox', { name: '名称', exact: true }).fill('阅读说明.md')
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByRole('button', { name: '阅读说明.md', exact: true })).toBeVisible()
  const downloadPromise = page.waitForEvent('download')
  await page.getByRole('button', { name: '下载 阅读说明.md', exact: true }).click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('阅读说明.md')
  expect(await readFile((await download.path())!)).toEqual(text)

  await action(page, '阅读说明.md', '复制文件')
  await page.getByRole('textbox', { name: '名称', exact: true }).fill('说明副本.md')
  await page.getByRole('dialog').getByRole('button', { name: target, exact: true }).click()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByRole('dialog')).toHaveCount(0)
  await action(page, '大文件.bin', '移动到')
  await page.getByRole('dialog').getByRole('button', { name: target, exact: true }).click()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByRole('button', { name: '大文件.bin', exact: true })).toHaveCount(0)

  await page.getByRole('checkbox', { name: '选择 阅读说明.md', exact: true }).check()
  await page.getByRole('button', { name: '打包 ZIP', exact: true }).click()
  await page.getByRole('textbox', { name: '压缩文件名称' }).fill(`验收-${suffix}.zip`)
  await page.getByRole('button', { name: '创建打包任务' }).click()
  await page.getByRole('link', { name: '任务中心' }).click()
  await expect(page.getByText('已完成', { exact: true })).toBeVisible()
  await page.reload()
  await expect(page.getByText(`验收-${suffix}.zip`, { exact: true })).toBeVisible()
  await expect(page.getByText('已完成', { exact: true })).toBeVisible()
  const zipPromise = page.waitForEvent('download')
  await page.getByRole('button', { name: '下载 ZIP' }).click()
  const zip = await zipPromise
  const zipPath = (await zip.path())!
  expect(
    execFileSync('python3', [
      '-c',
      'import sys,zipfile; z=zipfile.ZipFile(sys.argv[1]); assert z.namelist()==["阅读说明.md"]; sys.stdout.buffer.write(z.read("阅读说明.md"))',
      zipPath,
    ]),
  ).toEqual(text)

  await page.getByRole('link', { name: '我的文件' }).click()
  await action(page, source, '删除')
  await page.getByRole('button', { name: '确认删除' }).click()
  await expect(page.getByRole('alert').filter({ hasText: '文件夹不是空的' })).toBeVisible()
  await page.getByRole('button', { name: '取消', exact: true }).click()
  await enterFolder(page, source)
  for (const name of ['阅读说明.md', '空文件.txt', `验收-${suffix}.zip`]) {
    await action(page, name, '删除')
    await page.getByRole('button', { name: '确认删除' }).click()
    await expect(page.getByRole('button', { name, exact: true })).toHaveCount(0)
  }
  await page.getByRole('button', { name: '全部文件', exact: true }).click()
  await action(page, source, '删除')
  await page.getByRole('button', { name: '确认删除' }).click()
  await expect(page.getByRole('button', { name: source, exact: true })).toHaveCount(0)
  await enterFolder(page, target)
  for (const name of ['大文件.bin', '说明副本.md']) {
    await action(page, name, '删除')
    await page.getByRole('button', { name: '确认删除' }).click()
    await expect(page.getByRole('button', { name, exact: true })).toHaveCount(0)
  }
  await page.getByRole('button', { name: '全部文件', exact: true }).click()
  await action(page, target, '删除')
  await page.getByRole('button', { name: '确认删除' }).click()
  await page.getByRole('button', { name: '退出登录' }).click()
  await expect(page.getByRole('heading', { name: '登录你的空间' })).toBeVisible()
})

test('实际取消分片上传，不把中止连接伪装成服务端已取消', async ({ page }) => {
  await login(page)
  await page.route('**/api/upload-sessions/*/chunks/*', async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 500))
    await route.continue().catch(() => undefined)
  })
  await page.getByLabel('选择上传文件').setInputFiles({
    name: `取消验收-${Date.now()}.bin`,
    mimeType: 'application/octet-stream',
    buffer: Buffer.alloc(4 * 1024 * 1024, 1),
  })
  await page.getByRole('button', { name: '开始上传' }).click()
  await page.getByRole('link', { name: /传输列表/ }).click()
  await expect(page.getByText('正在上传', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: '取消', exact: true }).click()
  await expect(page.getByText('已取消', { exact: true })).toBeVisible()
  await expect(page.getByText('上传完成', { exact: true })).toHaveCount(0)
})

test('401、错误密码和可选的零配额账号', async ({ page, request }) => {
  await page.goto('/login')
  await page.getByRole('textbox', { name: '用户名' }).fill(username!)
  await page.getByLabel('密码', { exact: true }).fill('incorrect-password-for-test')
  await page.getByRole('button', { name: '进入我的空间' }).click()
  await expect(page.getByRole('alert')).toContainText('用户名或密码不正确')
  await login(page)
  const raw = await page.evaluate(() => sessionStorage.getItem('lucky.session'))
  const token = (JSON.parse(raw!) as { token: string }).token
  const response = await request.post('/api/auth/logout', {
    headers: { Authorization: `Bearer ${token}` },
  })
  expect(response.status()).toBe(204)
  await page.getByRole('button', { name: '刷新文件列表' }).click()
  await expect(page.getByRole('heading', { name: '登录你的空间' })).toBeVisible()
  expect(await page.evaluate(() => sessionStorage.getItem('lucky.tasks'))).toBeNull()
  if (process.env.E2E_EMPTY_USERNAME && process.env.E2E_EMPTY_PASSWORD) {
    await login(page, process.env.E2E_EMPTY_USERNAME, process.env.E2E_EMPTY_PASSWORD)
    await expect(page.getByText('这里还是空的')).toBeVisible()
    await page
      .getByLabel('选择上传文件')
      .setInputFiles({ name: '容量验收.txt', mimeType: 'text/plain', buffer: Buffer.from('quota') })
    await page.getByRole('button', { name: '开始上传' }).click()
    await page.getByRole('link', { name: /传输列表/ }).click()
    await expect(page.getByText(/账户可用容量不足/)).toBeVisible()
  }
})

test('桌面与手机：页面布局、键盘焦点和导航', async ({ page }) => {
  await page.goto('/login')
  await page.screenshot({ path: 'test-results/login-desktop.png', fullPage: true })
  await login(page)
  await page.screenshot({ path: 'test-results/files-desktop.png', fullPage: true })
  await page.getByRole('button', { name: '新建文件夹', exact: true }).click()
  await expect(page.getByRole('textbox', { name: '文件夹名称' })).toBeFocused()
  await page.keyboard.press('Escape')
  await expect(page.getByRole('dialog')).toHaveCount(0)
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({ path: 'test-results/files-mobile.png', fullPage: true })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  )
  await page.getByRole('button', { name: '打开导航' }).click()
  await page.getByRole('link', { name: /传输列表/ }).click()
  await expect(page.getByRole('heading', { name: '传输列表', exact: true })).toBeVisible()
  await page.getByRole('button', { name: '打开导航' }).click()
  await page.getByRole('link', { name: '任务中心' }).click()
  await expect(page.getByText('暂时没有打包任务')).toBeVisible()
  await page.screenshot({ path: 'test-results/tasks-mobile.png', fullPage: true })
  await page.getByRole('button', { name: '打开导航' }).click()
  await page.getByRole('button', { name: '退出登录' }).click()
  await expect(page.getByRole('heading', { name: '登录你的空间' })).toBeVisible()
  await page.screenshot({ path: 'test-results/login-mobile.png', fullPage: true })
})
