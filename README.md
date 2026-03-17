# astrbot_plugin_shitjournal_daily

每日定时抓取 `shitjournal` 最新论文并推送到会话：
- 推送文本元信息
- 推送 PDF 第 1 页预览图
- 可选附带 PDF 原文
- 可选以合并转发形式发送上述内容（OneBot v11 群聊/私聊）

## 特性
- 使用 Supabase API 直接获取最新论文（不依赖 Playwright）
- 多时点定时任务（每日多个 `HH:MM`）
- 定时自动推送可切换为“只检查最新一篇论文”
- 去重推送（按 `zone + paper_id`，已推送会自动回退到下一篇未推送论文）
- 支持目标分区无新稿时按顺序回退到候补分区
- 支持会话内绑定/解绑推送目标
- 指令管理员鉴权开关（默认开启）

## 指令
- `/shitjournal bind`：绑定当前会话为推送目标
- `/shitjournal unbind`：解绑当前会话
- `/shitjournal targets`：查看当前目标列表
- `/shitjournal run`：手动执行一次抓取并推送
- `/shitjournal run force`：忽略去重强制推送
- `/我要赤石`：抓取最新论文并推送到当前会话（默认所有人可用；按会话独立去重，已推送会自动回退到更早未推送论文）

## 配置
配置项定义在 `_conf_schema.json`，常用项如下：
- `zone`：默认 `stone`
- `enable_zone_fallback`：目标分区没有可推送新稿时，是否尝试候补分区，默认 `false`
- `fallback_zones`：候补分区列表，按填写顺序尝试，默认 `["septic"]`
- `schedule_times`：默认 `["09:00","21:00"]`
- `schedule_latest_only`：仅影响定时任务；开启后按会话只检查各分区最新一篇论文，默认 `false`
- `detail_hide_domain`：开启后“详情”仅显示 `/preprints/xxxx` 路径，默认 `false`
- `timezone`：默认 `Asia/Shanghai`
- `target_sessions`：会话列表（UMO）
- `send_merge_forward`：是否优先使用 OneBot v11 合并转发发送，默认 `false`
- `send_pdf`：是否附 PDF，默认 `false`
- `pdf_dpi`：转图 DPI，默认 `170`
- `pdf_max_size_mb`：允许处理的 PDF 最大体积（MB），默认 `50`
- `send_concurrency`：并发推送会话数，默认 `3`（建议 `1-5`，最大 `20`）
- `supabase_url`：Supabase API 地址
- `supabase_publishable_key`：Supabase Publishable Key（已内置默认值，可覆盖；也可用环境变量 `SUPABASE_PUBLISHABLE_KEY`）
- `command_admin_only`：仅管理员可用命令，默认 `true`
- `command_no_permission_reply`：无权限是否提示，默认 `true`
- `chi_shi_group_cooldown_sec`：`/我要赤石` 会话冷却秒数，默认 `60`（不同会话独立计时）
- `chi_shi_group_fail_cooldown_sec`：`/我要赤石` 失败后冷却秒数，默认 `10`
- `chi_shi_keep_full_history`：`/我要赤石` 是否全量保留已推送历史，默认 `true`
- `chi_shi_history_limit`：`/我要赤石` 已推送历史保留上限，默认 `30`，仅在关闭全量保留时生效
- `pdf_expire_days`：PDF 临时文件过期天数，默认 `0`；仅删除未占用且超时的 PDF，`0` 表示关闭按时间删除

## 依赖
`requirements.txt`:
- `aiofiles`
- `httpx`
- `PyMuPDF`

## 说明
- 插件最低要求 AstrBot `4.9.2+`，并依赖该版本提供的官方插件 KV 存储能力。
- 管理员身份依赖 AstrBot 全局 `admins_id` 配置。
- 内置了默认 Supabase key；若需要可在插件配置或环境变量中覆盖。
- `/我要赤石` 不受 `command_admin_only` 影响，默认所有人都可触发。
- 开启 `enable_zone_fallback` 后，定时推送、`/shitjournal run` 和 `/我要赤石` 都会先尝试 `zone`，只有当前分区从新到旧都没有可推送论文时才依次尝试 `fallback_zones`。
- 定时推送和 `/shitjournal run` 会按目标会话独立从新到旧查找目标分区第一篇未推送论文；某个会话命中后，本轮不会继续为该会话检查更早论文或候补分区；`/我要赤石` 仍保持按会话独立去重逻辑。
- 开启 `schedule_latest_only` 后，定时推送会改为按会话只检查各分区最新一篇论文：若某个会话在主分区最新一篇已推送，则继续检查候补分区最新一篇，不再回补同分区更早论文；`/shitjournal run` 和 `/我要赤石` 仍保持原逻辑。
- 开启 `detail_hide_domain` 后，定时推送、`/shitjournal run` 的执行结果以及 `/我要赤石` 推送里的“详情”会显示为 `/preprints/xxxx`，不再带 `https://shitjournal.org` 域名。
- `chi_shi_keep_full_history=true` 时会完整保留每个会话、每个分区的已推送历史；关闭后仅保留最近 `chi_shi_history_limit` 条，更省存储，但更早的论文后续会被视为“未推送”。
- 开启 `send_merge_forward` 后，定时推送、`/shitjournal run` 的论文内容推送以及 `/我要赤石` 会在运行时识别目标是否为 OneBot v11 群聊或私聊；命中时将正文、预览图与可选 PDF 放进同一个合并转发消息中，未命中或发送失败时会自动回退为普通消息。
- `pdf_expire_days` 只影响 PDF；PNG 预览图仍按 `temp_keep_files` 数量上限清理。
