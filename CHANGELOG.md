# CHANGELOG

## v1.3.0

> 基于 `master / v1.2.3` 之后的变更整理。

### 新增

- 新增独立的“课题”推送流，可和论文流分别开关。
- 新增课题抓取、选择、去重、发送与报告链路。
- 新增文章与课题发送组合模式：
  - `separate`：论文和课题分开发送。
  - `bundle_by_session`：同一轮、同一会话命中的论文和课题合并发送。
- 新增 `referendum`、`published` 两个论文分区支持。
- 新增论文和课题各分区排序配置，并按站点前端白名单严格校验。
- 新增插件内部配置迁移机制：
  - 支持旧 flat 配置自动迁移到新的 nested 分层配置。
  - 迁移完成后始终以新分层配置为准。

### 变更

- 默认站点地址切换为：
  - `api_base_url = https://shitspace.xyz`
  - `pdf_base_url = https://files.shitspace.xyz`
- 论文详情链接统一为 `/article/{id}`，课题详情链接为 `/question/{id}`。
- 课题发送改为纯文本链路，不再依赖文转图。
- OneBot v11 目标下，论文与课题都支持优先尝试合并转发；失败后会显式回退普通消息。
- 配置文件 `_conf_schema.json` 改为真正的分层 schema：
  - `article`
  - `question`
  - `delivery`
  - `schedule`
  - `site`
  - `assets`
  - `commands`
- 插件更新日志现在可由 AstrBot 直接读取本文件显示。

### 修复

- 修复站点 API 更新后，旧 `discipline=all` 参数导致的 `422` 抓取失败问题。
- 修复文章详情接口新旧返回结构兼容问题。
- 修复课题发送把裸 `list` 传给 `context.send_message()` 的合约错误。
- 修复 `bundle_by_session` 下合并转发失败不回退普通消息的问题。
- 修复 `bundle_by_session` 下论文 PDF 尾包与 NapCat Stream 上传链路不一致的问题。
- 修复课题非法排序配置被折叠成通用抓取失败的问题，现会显式报配置错误。
- 修复 `published` 特殊接口分页处理问题。
- 修复分层配置升级后，旧 flat 配置被 nested 默认值遮蔽的问题。
- 修复用户把分层配置改回默认值/空值时，被旧 flat 历史值反向覆盖的问题。

### 配置迁移

- 旧配置键仍保留兼容，但在新 schema 中会隐藏显示。
- 以下旧配置已迁移或替换：
  - `supabase_url` -> `api_base_url`
  - 新增 `pdf_base_url`
  - `supabase_publishable_key` / `supabase_bucket` 不再使用
- 若你从旧版本升级：
  - 现有 flat 配置会在首次加载时自动迁移到新的分层配置。
  - 迁移完成后，后续应以分层配置界面中的值为准。

### 升级提示

- 课题推送默认仍为关闭，需要手动开启 `enable_questions_push`。
- 如果你使用候补分区或排序配置，建议升级后检查论文和课题的分区、排序是否符合预期。
- 如果你之前使用旧站点域名或旧 Supabase 配置，请确认已切换到新的 `shitspace.xyz` 体系地址。
