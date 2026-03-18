from __future__ import annotations

import re
from typing import Any, Awaitable, Callable


class CronScheduler:
    def __init__(
        self,
        *,
        context_getter: Callable[[], Any],
        plugin_id_getter: Callable[[], str],
        cfg_getter: Callable[[str, Any], Any],
        cfg_bool_getter: Callable[[str, bool], bool],
        kv_getter: Callable[[str, Any], Awaitable[Any]],
        kv_putter: Callable[[str, Any], Awaitable[None]],
        get_cron_job_ids: Callable[[], list[str]],
        set_cron_job_ids: Callable[[list[str]], None],
        run_cycle: Callable[..., Awaitable[Any]],
        render_report: Callable[..., str],
        logger: Any,
        default_schedule_times: list[str],
    ):
        self._context_getter = context_getter
        self._plugin_id_getter = plugin_id_getter
        self._cfg_getter = cfg_getter
        self._cfg_bool_getter = cfg_bool_getter
        self._kv_getter = kv_getter
        self._kv_putter = kv_putter
        self._get_cron_job_ids = get_cron_job_ids
        self._set_cron_job_ids = set_cron_job_ids
        self._run_cycle = run_cycle
        self._render_report = render_report
        self._logger = logger
        self._default_schedule_times = list(default_schedule_times)

    async def register_cron_jobs(self) -> None:
        cron_manager = getattr(self._context_getter(), "cron_manager", None)
        if cron_manager is None:
            self._logger.error("cron_manager 不可用，已跳过定时任务注册。")
            return

        timezone = str(self._cfg_getter("timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai"
        times = self.resolve_schedule_times()
        plugin_id = self._plugin_id_getter()
        created_job_ids: list[str] = []
        try:
            for idx, time_text in enumerate(times):
                parsed = self.parse_hhmm(time_text)
                if parsed is None:
                    self._logger.warning("已跳过无效的定时时间：%s", time_text)
                    continue
                await self._register_single_job(
                    cron_manager=cron_manager,
                    plugin_id=plugin_id,
                    timezone=timezone,
                    index=idx,
                    time_text=time_text,
                    parsed=parsed,
                    created_job_ids=created_job_ids,
                )
        except Exception:
            failed_ids = await self.delete_cron_jobs(cron_manager, created_job_ids)
            self._set_cron_job_ids(failed_ids)
            await self._kv_putter("cron_job_ids", failed_ids)
            raise

        self._set_cron_job_ids(created_job_ids)
        await self._kv_putter("cron_job_ids", created_job_ids)

    async def _register_single_job(
        self,
        *,
        cron_manager: Any,
        plugin_id: str,
        timezone: str,
        index: int,
        time_text: str,
        parsed: tuple[int, int],
        created_job_ids: list[str],
    ) -> None:
        hour, minute = parsed
        cron_expression = f"{minute} {hour} * * *"
        job = await cron_manager.add_basic_job(
            name=f"{plugin_id}_shitjournal_{index}",
            cron_expression=cron_expression,
            handler=self.scheduled_tick,
            description=f"shitjournal 定时推送 {time_text}",
            timezone=timezone,
            payload={"schedule_time": time_text},
            enabled=True,
            persistent=False,
        )
        created_job_ids.append(job.job_id)
        self._logger.info(
            "已注册定时任务：任务ID=%s 时间=%s 表达式=%s 时区=%s",
            job.job_id,
            time_text,
            cron_expression,
            timezone,
        )

    async def clear_cron_jobs(self) -> None:
        ids = await self.load_cron_job_ids()
        cron_manager = getattr(self._context_getter(), "cron_manager", None)
        if cron_manager is None:
            self._set_cron_job_ids(ids)
            await self._kv_putter("cron_job_ids", ids)
            return

        failed_ids = await self.delete_cron_jobs(cron_manager, ids)
        self._set_cron_job_ids(failed_ids)
        await self._kv_putter("cron_job_ids", failed_ids)

    async def load_cron_job_ids(self) -> list[str]:
        ids = [str(job_id).strip() for job_id in self._get_cron_job_ids() if str(job_id).strip()]
        if ids:
            return ids
        loaded = await self._kv_getter("cron_job_ids", [])
        if not isinstance(loaded, list):
            return []
        return [str(job_id).strip() for job_id in loaded if str(job_id).strip()]

    async def delete_cron_jobs(self, cron_manager: Any, ids: list[str]) -> list[str]:
        failed_ids: list[str] = []
        for job_id in ids:
            try:
                await cron_manager.delete_job(job_id)
                self._logger.info("已删除旧定时任务：%s", job_id)
            except Exception:
                failed_ids.append(job_id)
                self._logger.warning("删除定时任务失败，已忽略：%s", job_id, exc_info=True)
        return failed_ids

    def resolve_schedule_times(self) -> list[str]:
        marker = object()
        raw = self._cfg_getter("schedule_times", marker)
        if raw is marker:
            return self._default_schedule_times.copy()
        return self.normalize_schedule_times(raw)

    async def scheduled_tick(self, schedule_time: str = "") -> None:
        report = await self._run_cycle(
            force=False,
            source=f"定时:{schedule_time or '未知'}",
            latest_only=self._cfg_bool_getter("schedule_latest_only", False),
        )
        self._logger.info(
            "定时推送执行完成：%s",
            self._render_report(report, include_debug=True),
        )

    def parse_hhmm(self, value: str) -> tuple[int, int] | None:
        text = str(value).strip()
        match = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", text)
        if not match:
            return None
        return int(match.group(1)), int(match.group(2))

    def normalize_schedule_times(self, raw: Any) -> list[str]:
        values: list[str]
        if isinstance(raw, str):
            values = [item.strip() for item in re.split(r"[,\s]+", raw) if item.strip()]
        elif isinstance(raw, list):
            values = [str(item).strip() for item in raw if str(item).strip()]
        else:
            values = []

        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            parsed = self.parse_hhmm(value)
            if parsed is None:
                continue
            formatted = f"{parsed[0]:02d}:{parsed[1]:02d}"
            if formatted in seen:
                continue
            seen.add(formatted)
            normalized.append(formatted)
        return normalized
