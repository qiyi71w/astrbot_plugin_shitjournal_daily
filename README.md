# astrbot_plugin_shitjournal_daily

## SHIT期刊似乎死了？目前插件需要科学上网才能正常使用

每日定时抓取 `shitjournal` 最新论文并推送到会话：
- 推送论文文本元信息
- 推送 PDF 第 1 页预览图
- 可选附带 PDF 原文
- 可选以合并转发形式发送论文/课题内容（OneBot v11 群聊/私聊；群聊可把论文 PDF 一并放进合并转发，私聊会单独补发 PDF）
- 可选独立推送课题，或在同会话同轮与论文合并发送

## 特性
- 使用站点 API 获取最新论文/课题，并通过 PDF API 下载论文文件
- 多时点定时任务（每日多个 `HH:MM`）
- 定时自动推送可切换为“只检查最新一篇内容”
- 去重推送（按 `zone + paper_id`，已推送会自动回退到下一篇未推送论文）
- 支持目标分区无新稿时按顺序回退到候补分区
- 论文流支持 `latrine`、`septic`、`sediment`、`stone`、`referendum`、`published`
- 课题流支持 `latrine`、`septic`、`sediment`、`stone`
- 论文和课题都按前端分区白名单校验排序配置
- 支持文章与课题按会话独立发送，或同会话同轮合并发送
- 支持会话内绑定/解绑推送目标
- 指令管理员鉴权开关（默认开启）

## 指令
- `/shitjournal bind`：绑定当前会话为推送目标
- `/shitjournal unbind`：解绑当前会话
- `/shitjournal targets`：查看当前目标列表
- `/shitjournal run`：手动执行一次抓取并推送
- `/shitjournal run force`：忽略去重强制推送
- `/我要赤石`：抓取最新论文并推送到当前会话（默认所有人可用；按会话独立去重，已推送会自动回退到更早未推送论文；AstrBot 管理员可无视会话冷却）

## 配置
配置项定义在 `_conf_schema.json`，常用项如下：
- `zone`：默认 `stone`
- `enable_zone_fallback`：目标分区没有可推送新稿时，是否尝试候补分区，默认 `false`
- `fallback_zones`：候补分区列表，按填写顺序尝试，默认 `["septic"]`
- `enable_article_push`：是否启用论文推送流，默认 `true`
- `article_sort_latrine` / `article_sort_septic` / `article_sort_sediment` / `article_sort_stone`：论文基础分区排序配置；`latrine` 可选 `newest`、`hottest`、`random`，`septic` 可选 `random`、`hottest`、`highest_rated`，`sediment` 仅支持 `newest`，`stone` 可选 `highest_rated`、`random`
- `enable_questions_push`：是否启用课题推送流，默认 `false`
- `article_question_bundle_mode`：文章与课题发送组合模式，默认 `separate`；可选 `separate`、`bundle_by_session`
- `questions_zone`：课题主分区，默认 `latrine`
- `questions_enable_zone_fallback`：课题主分区没有可推送新课题时是否尝试候补分区，默认 `false`
- `questions_fallback_zones`：课题候补分区列表，默认 `["septic"]`
- `questions_sort_latrine` / `questions_sort_septic` / `questions_sort_sediment` / `questions_sort_stone`：课题各分区排序配置；`latrine` 可选 `newest`、`hottest`、`random`，`septic` 可选 `hottest`、`highest_rated`、`random`，`sediment` 仅支持 `newest`，`stone` 可选 `highest_rated`、`random`
- `schedule_times`：默认 `["09:00","21:00"]`
- `schedule_latest_only`：仅影响定时任务；开启后按会话只检查各分区最新一篇内容，默认 `false`
- `detail_hide_domain`：开启后“详情”仅显示 `/article/xxxx` 或 `/question/xxxx` 路径，默认 `false`
- `timezone`：默认 `Asia/Shanghai`
- `target_sessions`：会话列表（UMO）
- `send_merge_forward`：是否优先使用 OneBot v11 合并转发发送，默认 `true`
- `send_pdf`：是否附 PDF，默认 `true`
- `pdf_dpi`：转图 DPI，默认 `170`
- `pdf_max_size_mb`：允许处理的 PDF 最大体积（MB），默认 `50`
- `send_concurrency`：并发推送会话数，默认 `3`（建议 `1-5`，最大 `20`）
- `api_base_url`：站点 API 地址，默认 `https://shitspace.xyz`
- `pdf_base_url`：PDF API 地址，默认 `https://files.shitspace.xyz`
- `proxy_url`：插件访问 `api_base_url` / `pdf_base_url` 的代理地址；留空时沿用 `httpx` 环境代理行为，非空时优先使用该代理并禁用环境代理继承；仅支持 `http://` / `https://`，`socks5://` 会报错
- `command_admin_only`：仅管理员可用命令，默认 `true`
- `command_no_permission_reply`：无权限是否提示，默认 `true`
- `chi_shi_group_cooldown_sec`：`/我要赤石` 会话冷却秒数，默认 `60`（不同会话独立计时；AstrBot 管理员触发时不受此冷却限制）
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
- 配置迁移规则：将 `supabase_url` 手动改为 `api_base_url`，并新增/使用 `pdf_base_url`；`supabase_publishable_key` 与 `supabase_bucket` 已删除且不再兼容。
- `/我要赤石` 不受 `command_admin_only` 影响，默认所有人都可触发。
- `/shitjournal run` 和定时推送会依次执行“论文”和“课题”两条流；它们共享 `target_sessions`、`schedule_times`、`schedule_latest_only`，各自维护独立去重状态。
- 开启 `enable_zone_fallback` 后，定时推送、`/shitjournal run` 和 `/我要赤石` 都会先尝试 `zone`，只有当前分区从新到旧都没有可推送论文时才依次尝试 `fallback_zones`。
- 开启 `questions_enable_zone_fallback` 后，课题流会在 `questions_zone` 无可推送课题时依次尝试 `questions_fallback_zones`。
- 定时推送和 `/shitjournal run` 会按目标会话独立从新到旧查找目标分区第一篇未推送论文；某个会话命中后，本轮不会继续为该会话检查更早论文或候补分区；`/我要赤石` 仍保持按会话独立去重逻辑。
- 开启 `schedule_latest_only` 后，定时推送会改为按会话只检查各分区最新一篇内容；论文流与课题流都会按主分区和候补分区顺序检查最新项；`/shitjournal run` 和 `/我要赤石` 仍保持原逻辑。
- `article_sort_*` 与 `questions_sort_*` 都按站点前端分区白名单校验；非法排序配置会显式报错，不会静默降级。`referendum` 与 `published` 仍走站点特殊接口，不使用 `article_sort_*`。
- 开启 `detail_hide_domain` 后，定时推送、`/shitjournal run` 的执行结果以及 `/我要赤石` 推送里的“详情”会显示为 `/article/xxxx` 或 `/question/xxxx`。
- 论文推送文本字段为：分区、标题、作者、提交时间、学科、评分（平均分与票数）、详情。
- 课题推送为纯文本消息，不使用 `html_render` / 文转图，也不附带 PDF；字段为：分区、标题、作者、提交时间、学科、标签、评分（平均分 / 票数 / 评论数）、详情、正文摘录。
- 学科枚举值：`science`、`engineering`、`agriculture`、`medicine`、`economics`、`management`、`law`、`social`、`literature`、`history`、`philosophy`、`art`、`business`、`mathematics`、`interdisciplinary`。
- `chi_shi_keep_full_history=true` 时会完整保留每个会话、每个分区的已推送历史；关闭后仅保留最近 `chi_shi_history_limit` 条，更省存储，但更早的论文后续会被视为“未推送”。
- 开启 `send_merge_forward` 后，定时推送、`/shitjournal run` 的论文/课题发送以及 `/我要赤石` 会在运行时识别目标是否为 OneBot v11 群聊或私聊；`separate` 模式下，论文与课题会各自优先尝试合并转发；`bundle_by_session` 模式下，同一会话同一轮命中的论文与课题会按“论文在前、课题在后”统一发送。群聊命中时可把论文 PDF 一并放进合并转发，私聊命中时会把论文 PDF 作为后续普通文件消息补发；未命中或发送失败时会自动回退为普通消息。
- 对 `aiocqhttp` 发送 PDF 时会优先尝试 NapCat Stream API（`upload_file_stream`）；若 Stream 接口不可用、上传异常或返回缺少 `file_path`，会记录 warning 并显式回退 `File(url=...)` 发送。
- `pdf_expire_days` 只影响 PDF；PNG 预览图仍按 `temp_keep_files` 数量上限清理。
- `proxy_url` 只影响插件内部抓取/下载链路（访问 `api_base_url` / `pdf_base_url` 的 HTTP 请求）；不会改变 NapCat Stream 上传、下游 `File(url=...)` 发送和“详情”链接构造逻辑。
- Docker 注意：容器内的 `localhost` / `127.0.0.1` 指向容器自身，不是宿主机。若宿主机上有代理，请在 `proxy_url` 使用容器内可达地址，例如 `host.docker.internal` 或 Docker 网关地址。
