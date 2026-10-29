import { afterEach, describe, expect, it } from 'vitest'
import { authSession, isSession } from './session'

afterEach(() => {
  authSession.set(null)
  sessionStorage.clear()
})
const session = {
  token: 'sample-token',
  username: 'Ethan',
  expires_at: new Date(Date.now() + 60_000).toISOString(),
}

describe('tab-scoped authentication', () => {
  it('rejects expired or malformed restored sessions', () => {
    expect(isSession(session)).toBe(true)
    expect(isSession({ ...session, expires_at: 'invalid' })).toBe(false)
    expect(isSession({ ...session, expires_at: '2000-01-01' })).toBe(false)
    expect(isSession({ token: 'token' })).toBe(false)
  })
  it('persists no password and clears account-specific records on account switch', () => {
    authSession.set(session)
    expect(JSON.parse(sessionStorage.getItem('lucky.session')!)).toEqual(session)
    expect(localStorage.getItem('lucky.session')).toBeNull()
    sessionStorage.setItem('lucky.tasks', '[{"id":"a"}]')
    sessionStorage.setItem('lucky.paths', '{}')
    authSession.set({ ...session, token: 'another-token', username: 'B' })
    expect(sessionStorage.getItem('lucky.tasks')).toBeNull()
    expect(sessionStorage.getItem('lucky.paths')).toBeNull()
    authSession.set(null)
    expect(authSession.getSnapshot()).toBeNull()
    expect(sessionStorage.getItem('lucky.session')).toBeNull()
  })
})
