import { useEffect, useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Navigate, Outlet, Route, Routes } from 'react-router'
import { api } from '../shared/http/client'
import { ApiError } from '../shared/http/errors'
import { authSession, useSession } from '../features/auth/session'
import { AppLayout } from './AppLayout'
import { LoginPage } from '../pages/LoginPage'
import { FilesPage } from '../pages/FilesPage'
import { TransfersPage } from '../pages/TransfersPage'
import { TasksPage } from '../pages/TasksPage'
import { UploadProvider } from '../features/uploads/UploadProvider'
import { TaskProvider } from '../features/tasks/TaskProvider'

api.configureAuth({
  getToken: () => authSession.getSnapshot()?.token ?? null,
  onUnauthorized: () => authSession.set(null),
})

function RequireAuth() {
  const session = useSession()
  return session ? <Outlet /> : <Navigate to="/login" replace />
}

function SessionApplication() {
  const session = useSession()
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 15_000,
            retry: (count, error) =>
              count < 1 && (!(error instanceof ApiError) || error.status >= 500),
          },
          mutations: { retry: false },
        },
      }),
  )
  useEffect(() => {
    if (!session) return
    const timeout = setTimeout(
      () => authSession.set(null),
      Math.max(0, Date.parse(session.expires_at) - Date.now()),
    )
    return () => clearTimeout(timeout)
  }, [session])
  useEffect(
    () => () => {
      void queryClient.cancelQueries()
      queryClient.clear()
    },
    [queryClient],
  )

  return (
    <QueryClientProvider client={queryClient}>
      <Routes>
        <Route path="/login" element={session ? <Navigate to="/files" replace /> : <LoginPage />} />
        <Route element={<RequireAuth />}>
          <Route
            element={
              <UploadProvider>
                <TaskProvider>
                  <AppLayout />
                </TaskProvider>
              </UploadProvider>
            }
          >
            <Route path="/files" element={<FilesPage />} />
            <Route path="/transfers" element={<TransfersPage />} />
            <Route path="/tasks" element={<TasksPage />} />
          </Route>
        </Route>
        <Route path="*" element={<Navigate to={session ? '/files' : '/login'} replace />} />
      </Routes>
    </QueryClientProvider>
  )
}

export function App() {
  const session = useSession()
  return <SessionApplication key={session?.token ?? 'guest'} />
}
