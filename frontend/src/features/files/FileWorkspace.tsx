import { useEffect, useRef, useState } from 'react'
import type { DragEvent } from 'react'
import { useFileNavigation } from './navigation'
import { useFiles } from './queries'
import { FileTable } from './FileTable'
import { FileOperationDialog } from './FileOperationDialog'
import type { FileOperation } from './FileOperationDialog'
import { UploadDialog } from '../uploads/UploadDialog'
import { ArchiveDialog } from '../tasks/ArchiveDialog'
import { VerifyDialog } from '../tasks/VerifyDialog'
import { Button } from '../../shared/ui/Button'
import {
  EmptyState,
  ErrorNotice,
  LoadingState,
  PageHeading,
  Pagination,
} from '../../shared/ui/Feedback'
import { Icon } from '../../shared/ui/Icon'
import styles from './files.module.css'
import ui from '../../shared/ui/ui.module.css'

type Navigation = ReturnType<typeof useFileNavigation>

function DirectoryView({ navigation }: { navigation: Navigation }) {
  const query = useFiles(navigation.parentId, navigation.page)
  const [operation, setOperation] = useState<FileOperation | null>(null)
  const [uploadFiles, setUploadFiles] = useState<File[] | null>(null)
  const [archive, setArchive] = useState(false)
  const [verify, setVerify] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [dragging, setDragging] = useState(false)
  const input = useRef<HTMLInputElement>(null)
  const dragDepth = useRef(0)
  const selectedFiles =
    query.data?.items.filter((entry) => entry.kind === 'file' && selected.has(entry.id)) ?? []
  const lastPage = Math.max(1, Math.ceil((query.data?.total ?? 0) / 20))
  useEffect(() => {
    if (query.data && navigation.page > lastPage) navigation.setPage(lastPage)
  }, [query.data, navigation, lastPage])

  function onDrop(event: DragEvent) {
    event.preventDefault()
    dragDepth.current = 0
    setDragging(false)
    const files = Array.from(event.dataTransfer.files)
    if (files.length) setUploadFiles(files)
  }

  return (
    <div
      onDragOver={(event) => {
        if (event.dataTransfer.types.includes('Files')) event.preventDefault()
      }}
      onDragEnter={(event) => {
        if (event.dataTransfer.types.includes('Files')) {
          event.preventDefault()
          dragDepth.current++
          setDragging(true)
        }
      }}
      onDragLeave={() => {
        dragDepth.current = Math.max(0, dragDepth.current - 1)
        if (dragDepth.current === 0) setDragging(false)
      }}
      onDrop={onDrop}
    >
      <PageHeading
        eyebrow="YOUR PERSONAL LIBRARY"
        title="我的文件"
        description="每一份灵感，都有自己的位置。"
      >
        <Button onClick={() => setOperation({ kind: 'create' })}>
          <Icon name="plus" size={17} />
          新建文件夹
        </Button>
        <Button variant="primary" onClick={() => input.current?.click()}>
          <Icon name="upload" size={17} />
          上传文件
        </Button>
      </PageHeading>
      <input
        ref={input}
        type="file"
        multiple
        className="sr-only"
        tabIndex={-1}
        aria-label="选择上传文件"
        onChange={(event) => {
          const files = Array.from(event.target.files ?? [])
          if (files.length) setUploadFiles(files)
          event.target.value = ''
        }}
      />
      <section
        className={`${styles.workbench} ${dragging ? styles.dragging : ''}`}
        aria-label="文件列表"
        aria-busy={query.isFetching}
      >
        <div className={styles.toolbar}>
          <nav className={styles.breadcrumbs} aria-label="文件夹路径">
            <button onClick={navigation.root}>
              <Icon name="files" size={17} /> 全部文件
            </button>
            {navigation.crumbs.map((crumb, index) => (
              <span key={crumb.id}>
                <Icon name="chevron" size={12} />
                <button
                  title={crumb.name}
                  onClick={() => navigation.ancestor(index)}
                  aria-current={index === navigation.crumbs.length - 1 ? 'location' : undefined}
                >
                  {crumb.name}
                </button>
              </span>
            ))}
          </nav>
          <div className={styles.tools}>
            <span className={ui.badge}>{query.data?.total ?? '—'} 项</span>
            <button
              className={ui.iconButton}
              aria-label="刷新文件列表"
              disabled={query.isFetching}
              onClick={() => {
                void query.refetch()
              }}
            >
              <Icon name="refresh" size={17} />
            </button>
          </div>
        </div>
        {selectedFiles.length > 0 && (
          <div className={styles.selection}>
            <span>已选择 {selectedFiles.length} 个文件</span>
            <div>
              <Button variant="ghost" onClick={() => setSelected(new Set())}>
                取消选择
              </Button>
              <Button variant="secondary" onClick={() => setVerify(true)}>
                <Icon name="shield" size={16} />
                校验文件
              </Button>
              <Button variant="secondary" onClick={() => setArchive(true)}>
                <Icon name="archive" size={16} />
                打包 ZIP
              </Button>
            </div>
          </div>
        )}
        {query.isPending && <LoadingState />}
        {query.isError && (
          <ErrorNotice
            error={query.error}
            retry={() => {
              void query.refetch()
            }}
          />
        )}
        {query.data && (
          <>
            {query.data.items.length === 0 ? (
              <EmptyState
                title="这里还是空的"
                description="新建一个文件夹，或上传你的第一份文件。所有重要的，都可以从这里开始。"
              >
                <Button variant="primary" onClick={() => input.current?.click()}>
                  <Icon name="upload" size={16} />
                  上传第一份文件
                </Button>
              </EmptyState>
            ) : (
              <FileTable
                entries={query.data.items}
                selected={selected}
                onSelect={setSelected}
                onEnter={navigation.enter}
                onAction={setOperation}
              />
            )}
            <Pagination
              page={navigation.page}
              total={query.data.total}
              onChange={navigation.setPage}
            />
          </>
        )}
      </section>
      <div className={styles.dropHint}>
        <Icon name="upload" size={17} />
        <span>
          {dragging ? '松开鼠标，将文件添加到上传队列' : '也可以将文件拖到这里上传'}
          <span style={{ margin: '0 10px', color: '#c6cbd6' }}>·</span>单文件最大 32 MiB
        </span>
      </div>
      {operation && (
        <FileOperationDialog
          operation={operation}
          parentId={navigation.parentId}
          onClose={() => setOperation(null)}
        />
      )}
      {uploadFiles && (
        <UploadDialog
          files={uploadFiles}
          parentId={navigation.parentId}
          onClose={() => setUploadFiles(null)}
        />
      )}
      {archive && (
        <ArchiveDialog
          files={selectedFiles}
          parentId={navigation.parentId}
          onClose={() => {
            setArchive(false)
            setSelected(new Set())
          }}
        />
      )}
      {verify && (
        <VerifyDialog
          files={selectedFiles}
          parentId={navigation.parentId}
          onClose={() => {
            setVerify(false)
            setSelected(new Set())
          }}
        />
      )}
    </div>
  )
}

export function FileWorkspace() {
  const navigation = useFileNavigation()
  return <DirectoryView key={`${navigation.parentId}:${navigation.page}`} navigation={navigation} />
}
