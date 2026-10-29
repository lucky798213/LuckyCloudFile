// Browser storage is an optional persistence boundary, not a source of authorization.
export function readStored<T>(key: string, validate: (value: unknown) => value is T): T | null {
  try {
    const raw = sessionStorage.getItem(key)
    if (!raw) return null
    const value: unknown = JSON.parse(raw)
    return validate(value) ? value : null
  } catch {
    return null
  }
}

export function writeStored(key: string, value: unknown) {
  try {
    if (value === null) sessionStorage.removeItem(key)
    else sessionStorage.setItem(key, JSON.stringify(value))
  } catch {
    // Private browsing/storage limits must not break an otherwise usable session.
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
