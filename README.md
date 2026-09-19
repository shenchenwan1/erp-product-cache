# 运行

依赖：Node.js 20+、pnpm、Docker 和 Docker Compose。macOS 使用 Colima 时，先运行 `colima start`。

```bash
./run.sh install  # 安装锁定依赖，生成 Prisma Client
./run.sh build    # 编译
./run.sh unit     # 原有工具函数回归
./run.sh test     # 启动独立数据库和 Redis，执行基础测试
./run.sh down     # 停止并移除本目录的测试容器
```

集成测试使用真实 PostgreSQL、Redis 和独立 Node.js 子进程。只有平台商品查询和订阅服务使用本地固定接口，不需要真实店铺账号。测试会重置本目录专属容器内的数据；不会连接现有 ERP 数据库。

每个目录使用不同的 Compose 项目和动态端口，可分别运行。修改数据模型后先执行 `./run.sh install`；`test` 会同步数据库结构。首次运行需要下载容器镜像。

`test` 运行基础检查；通过这些检查不代表全部业务要求已经满足。
