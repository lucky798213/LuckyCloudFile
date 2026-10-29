# LuckyCloudFile

Go + React 文件管理项目，包含独立 API、后台 worker 和文件工作台。

## 代码结构

- `cmd/server`：HTTP API 服务。
- `cmd/createuser`：账号初始化命令。
- `cmd/worker`：独立后台任务 worker。
- `internal`：认证、文件与上传、存储、配额、任务等后端实现及自动化测试。
- `frontend`：React + TypeScript 前端及自动化测试。
- `configs`：不含真实凭据的配置示例。
- `scripts`：数据库初始化与自动化检查脚本。

## 本地运行

需要 Go 1.26、Docker Compose；前端需要 Node 24.x 和 pnpm 11.25.0。

```bash
git clone https://github.com/lucky798213/LuckyCloudFile.git
cd LuckyCloudFile
docker compose up -d --wait
cp configs/conf.example.ini configs/conf.ini
export CLOUDREVE_DB_PASSWORD=lucky
go run ./cmd/createuser --conf configs/conf.ini --username demo
# 按提示设置账号密码；服务启动不会自动创建账号。
go run ./cmd/server --conf configs/conf.ini
```

默认由操作系统分配 API 端口，以启动日志为准。另开终端启动前端：

```bash
cd LuckyCloudFile/frontend
pnpm install --frozen-lockfile
cp .env.example .env.local
# 将 .env.local 中的 PORT 替换为 API 实际监听端口。
pnpm dev
```

前端通过同源 `/api` 代理访问后端。需要独立 worker 时，使用相同配置和存储启动：

```bash
export CLOUDREVE_DB_PASSWORD=lucky
go run ./cmd/worker --conf configs/conf.ini --id worker-a
```

环境变量仍沿用代码中的 `CLOUDREVE_*` 命名。Compose 中的数据库密码仅供本机开发，不能用于生产。
S3/COS 使用前需要自行准备私有存储桶、权限与存储策略；COS 环境变量模板见 `configs/cos.env.example`，默认禁用。
不要提交实际配置、密钥或用户数据。

## 自动化检查

```bash
go test ./...
go build ./...
cd frontend
pnpm test
pnpm build
```

部分后端集成测试需要独立 MySQL 测试库及 `CLOUDREVE_TEST_MYSQL_DSN`，未设置时会跳过。
不要将测试 DSN 指向开发或生产数据库。

本仓库保留项目源码、自动化测试和运行所需示例；不包含个人笔记、开发计划、验收记录、运行数据或本地 Git 历史。
