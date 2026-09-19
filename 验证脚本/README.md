# 验证脚本

用于独立检查 ERP 商品缓存刷新模块的修复结果。A、B 使用同一套验收用例，分别验证各自目录中的当前代码。

## 文件用途

| 文件 | 用途 |
| --- | --- |
| `check.sh` | 验收入口：复制目标代码到临时目录、放入验收用例并执行测试，结束后清理临时目录及测试容器。 |
| `refresh.spec.ts` | 检查完整刷新、失败回滚、取消、重复执行、崩溃重跑、实时更新、生命周期、增量及分页等行为。 |
| `harness.ts` | 准备测试数据、协调独立工作进程，并读取数据库中的实际结果。 |
| `worker.ts` | 在独立 Node.js 进程中调用目标项目的刷新服务。 |
| `clock.cjs` | 固定部分测试的时钟，检查多个操作时间戳相同时的行为。 |

## 依赖

Node.js 20+、pnpm、Docker、Docker Compose，以及 Bash、rsync。先启动 Docker；macOS 使用 Colima 时运行 `colima start`。首次安装需要联网下载依赖和容器镜像。

测试使用真实 PostgreSQL、Redis 和独立 Node.js 进程；平台商品查询及订阅等外部接口使用本地测试替身，不需要真实店铺账号。

## 分别验证 A 和 B

以下命令对应当前本地目录布局。每次先执行 `install`，确保 Prisma Client 与目标目录的数据模型一致；安装成功后才开始验收。

A：

```bash
cd /Users/wanghongfei/Documents/DEV/ZQZL
./erp-cache-refresh-A/run.sh install &&
./erp-cache-refresh/验证脚本/check.sh ./erp-cache-refresh-A
```

B：

```bash
cd /Users/wanghongfei/Documents/DEV/ZQZL
./erp-cache-refresh-B/run.sh install &&
./erp-cache-refresh/验证脚本/check.sh ./erp-cache-refresh-B
```

其他目录也可使用，传入待验证项目的路径：

```bash
/path/to/main/验证脚本/check.sh /path/to/target-project
```

其中路径需要替换成实际路径；目标项目必须先运行 `./run.sh install`。

## 运行方式与结果

脚本会复制目标目录的当前代码（包括尚未提交的修改），在临时副本中放入验收文件，再调用副本的 `./run.sh test` 完成数据库准备、编译、工具函数测试和集成测试。目标目录的源代码不会被改写；临时副本复用目标目录已安装的 `node_modules`。

临时副本中同名的 `tests/refresh.spec.ts`、`tests/harness.ts`、`tests/worker.ts`、`tests/clock.cjs` 会被本目录版本替换，其他测试保留。目标目录原有文件不受影响。

- 所有阶段成功、所有执行的测试通过且退出码为 `0`：通过这套验收。
- 编译或业务断言失败：本次验收未通过，按终端错误定位原因。
- Docker 未启动、镜像下载或依赖安装失败：环境未准备好，处理后重跑，不能据此判断修复效果。

运行后可立即执行 `echo $?` 查看上一条命令的退出码。测试数量可能随目标分支自带的测试而变化，通过数量不能单独作为 A/B 优劣结论。验收只覆盖已编写的场景。

每次验收使用临时目录对应的独立数据库及 Redis，测试会重置其中的数据；结束时脚本清理本次容器和临时目录。

## 分支与初始快照

本目录随 `main` 分支保存，A、B 产物分别保存在 `result-a`、`result-b`。两次运行的共同初始快照仍是 [`38e4bc8b2a6e42ec512c73dc605b1a49d167c133`](https://github.com/shenchenwan1/erp-product-cache/commit/38e4bc8b2a6e42ec512c73dc605b1a49d167c133)；提交表中的初始快照应使用该固定链接。
