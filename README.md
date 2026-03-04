# astrbot_plugin_shitjournal_daily

每日定时抓取 `shitjournal` 最新论文并推送到群聊：
- 推送文本元信息
- 推送 PDF 第 1 页预览图
- 可选附带 PDF 原文

## 特性
- 使用 Supabase API 直接获取最新论文（不依赖 Playwright）
- 多时点定时任务（每日多个 `HH:MM`）
- 去重推送（按 `zone + paper_id`）
- 支持群内绑定/解绑推送目标
- 指令管理员鉴权开关（默认开启）

## 指令
- `/shitjournal bind`：绑定当前会话为推送目标
- `/shitjournal unbind`：解绑当前会话
- `/shitjournal targets`：查看当前目标列表
- `/shitjournal run`：手动执行一次抓取并推送
- `/shitjournal run force`：忽略去重强制推送
- `/我要赤石`：抓取最新论文并推送到当前群聊（默认所有人可用）

## 配置
配置项定义在 `_conf_schema.json`，常用项如下：
- `zone`：默认 `septic`
- `schedule_times`：默认 `["09:00","21:00"]`
- `timezone`：默认 `Asia/Shanghai`
- `target_sessions`：会话列表（UMO）
- `send_pdf`：是否附 PDF，默认 `false`
- `pdf_dpi`：转图 DPI，默认 `170`
- `send_concurrency`：并发推送会话数，默认 `3`（建议 `1-5`，最大 `20`）
- `supabase_url`：Supabase API 地址
- `supabase_publishable_key`：Supabase Publishable Key（已内置默认值，可覆盖；也可用环境变量 `SUPABASE_PUBLISHABLE_KEY`）
- `command_admin_only`：仅管理员可用命令，默认 `true`
- `command_no_permission_reply`：无权限是否提示，默认 `true`
- `chi_shi_group_cooldown_sec`：`/我要赤石` 群聊冷却秒数，默认 `60`（不同群独立计时）

## 依赖
`requirements.txt`:
- `requests`
- `PyMuPDF`

## 说明
- 管理员身份依赖 AstrBot 全局 `admins_id` 配置。
- 仅“命令”受 `command_admin_only` 控制，定时任务不受影响。
- 内置了默认 Supabase key；若需要可在插件配置或环境变量中覆盖。
- `/我要赤石` 不受 `command_admin_only` 影响，默认所有人都可触发。
