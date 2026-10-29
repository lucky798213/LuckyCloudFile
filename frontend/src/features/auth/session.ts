import { useSyncExternalStore } from 'react'
import { isRecord, readStored, writeStored } from '../../shared/lib/storage'

export type Session = { token: string; expires_at: string; username: string }
const key = 'lucky.session'
const listeners = new Set<() => void>()

export function isSession(value: unknown): value is Session {
  return (
    isRecord(value) &&
    typeof value.token === 'string' &&
    value.token.length > 0 &&
    typeof value.username === 'string' &&
    typeof value.expires_at === 'string' &&
    Date.parse(value.expires_at) > Date.now()
  )
}

let current = readStored(key, isSession)

export const authSession = {
  getSnapshot: () => current,
  subscribe(listener: () => void) {
    listeners.add(listener)
    return () => {
      listeners.delete(listener)
    }
  },
  set(session: Session | null) {
    const changed = current?.token !== session?.token
    current = session
    writeStored(key, session)
    if (!session || changed) {
      writeStored('lucky.paths', null)
      writeStored('lucky.tasks', null)
    }
    listeners.forEach((listener) => listener())
  },
}

export function useSession() {
  return useSyncExternalStore(authSession.subscribe, authSession.getSnapshot)
}
