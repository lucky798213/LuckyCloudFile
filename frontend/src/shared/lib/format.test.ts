import { describe, expect, it } from 'vitest'
import { formatBytes, validName } from './format'

describe('file display and validation', () => {
  it('formats bytes without claiming a missing size', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(1024)).toBe('1 KiB')
    expect(formatBytes(32 * 1024 * 1024)).toBe('32 MiB')
  })
  it.each(['', ' ', '.', '..', 'a/b', 'a\\b', '\0', '文'.repeat(256)])(
    'rejects invalid name %j',
    (name) => expect(validName(name)).toBe(false),
  )
  it.each(['学习资料', 'hello.md', '😀'.repeat(255)])('accepts valid name %j', (name) =>
    expect(validName(name)).toBe(true),
  )
})
