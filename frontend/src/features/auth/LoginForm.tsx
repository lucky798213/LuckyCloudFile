import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { authApi } from './api'
import { authSession } from './session'
import { Button } from '../../shared/ui/Button'
import { ErrorNotice } from '../../shared/ui/Feedback'
import ui from '../../shared/ui/ui.module.css'

export function LoginForm() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const login = useMutation({
    mutationFn: () => authApi.login(username.trim(), password),
    onSuccess: (result) => {
      authSession.set({ ...result, username: username.trim() })
    },
  })
  return (
    <form
      className={ui.form}
      onSubmit={(event) => {
        event.preventDefault()
        if (!login.isPending) login.mutate()
      }}
    >
      <label className={ui.field}>
        用户名
        <input
          autoComplete="username"
          name="username"
          required
          autoFocus
          value={username}
          onChange={(event) => setUsername(event.target.value)}
          placeholder="输入你的用户名"
          maxLength={255}
          disabled={login.isPending}
        />
      </label>
      <label className={ui.field}>
        密码
        <input
          type="password"
          autoComplete="current-password"
          name="password"
          required
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          placeholder="输入密码"
          disabled={login.isPending}
        />
      </label>
      {login.isError && <ErrorNotice error={login.error} />}
      <Button
        variant="primary"
        type="submit"
        busy={login.isPending}
        disabled={!username.trim() || !password}
      >
        进入我的空间 <span aria-hidden="true">→</span>
      </Button>
      <p className={ui.description}>登录仅在当前标签页保留。账号由项目维护者通过命令行创建。</p>
    </form>
  )
}
