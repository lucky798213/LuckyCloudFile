import { TaskList } from '../features/tasks/TaskList'
import { PageHeading } from '../shared/ui/Feedback'

export function TasksPage() {
  return (
    <>
      <PageHeading
        eyebrow="GOOD THINGS TAKE A LITTLE TIME"
        title="任务中心"
        description="把打包和内容校验交给后台，继续手边的事。"
      />
      <TaskList />
    </>
  )
}
