import { LoginForm } from '../features/auth/LoginForm'
import { Icon } from '../shared/ui/Icon'
import styles from './login.module.css'

export function LoginPage() {
  return (
    <main className={styles.page}>
      <section className={styles.story} aria-label="luckyCloudFile 私人文件空间">
        <a href="/login" className={styles.brand}>
          <Icon name="cloud" size={32} />
          luckyCloudFile
        </a>
        <div className={styles.storyContent}>
          <span className={styles.tag}>A LITTLE SPACE, ALL YOURS</span>
          <h2>
            让文件井然有序，
            <br />
            让灵感自在发生。
          </h2>
          <p>
            一个轻盈、专注的私人空间。
            <br />
            收藏日常，也安放每一个正在生长的想法。
          </p>
          <div className={styles.illustration} aria-hidden="true">
            <div className={styles.orbit} />
            <div className={`${styles.paper} ${styles.paperBack}`}>
              <Icon name="image" size={35} />
              <span />
              <span />
            </div>
            <div className={styles.paper}>
              <span className={styles.paperGlyph}>
                <Icon name="folder" size={40} />
              </span>
              <strong>所有重要的，都在这里。</strong>
              <span />
              <span />
            </div>
            <span className={styles.spark}>✳</span>
          </div>
        </div>
        <span className={styles.storyFooter}>YOUR FILES. YOUR SPACE. YOUR PACE.</span>
      </section>
      <section className={styles.login}>
        <div className={styles.formWrap}>
          <span className={styles.welcome}>欢迎回来</span>
          <h1>登录你的空间</h1>
          <p className={styles.subtitle}>从上一次的灵感，继续出发。</p>
          <LoginForm />
          <div className={styles.security}>
            <Icon name="shield" size={15} />
            账号独立隔离 · 仅当前标签页保留登录
          </div>
        </div>
        <footer>luckyCloudFile · 简单存放，安心珍藏。</footer>
      </section>
    </main>
  )
}
