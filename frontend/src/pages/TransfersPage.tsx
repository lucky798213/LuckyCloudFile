import { TransferList } from '../features/uploads/TransferList'
import { PageHeading } from '../shared/ui/Feedback'
import { Icon } from '../shared/ui/Icon'
import styles from '../features/uploads/uploads.module.css'

export function TransfersPage() {
  return (
    <>
      <PageHeading
        eyebrow="A LITTLE PROGRESS, EVERY MOMENT"
        title="传输列表"
        description="每一份文件，都在稳稳抵达。"
      />
      <TransferList />
      <p className={styles.note}>
        <Icon name="alert" size={16} />
        <span>
          切换页面不影响上传；刷新或关闭标签页会中断传输，需重新选择文件。未完成会话由后端过期清理。
        </span>
      </p>
    </>
  )
}
