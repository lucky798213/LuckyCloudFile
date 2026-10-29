import { api } from '../../shared/http/client'

type LoginResponse = { token: string; expires_at: string }

export const authApi = {
  login: (username: string, password: string) =>
    api.request<LoginResponse>('/auth/login', { method: 'POST', body: { username, password } }),
  logout: () => api.request<void>('/auth/logout', { method: 'POST' }),
}
