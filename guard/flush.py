"""巡检异常会话并按有效期、批量与间隔补发积压。
确认恢复、发送试探与连续失败退避各自使用独立计数。"""


from __future__ import annotations

import asyncio
import time

from astrbot.api import logger


class Flusher:
    def __init__(self, real_context, store, core, cfg) -> None:
        self.context = real_context
        self.store = store
        self.core = core
        self._cfg = cfg
        self._busy: set[str] = set()
        self._flushing: set[str] = set()

    async def tick(self) -> None:
        for umo in self.store.list_umos():
            try:
                await self._check_one(umo)
            except Exception:
                logger.exception("[guard] 巡检 %s 出错", umo)

    async def _check_one(self, umo: str) -> None:
        if not self.core.in_scope(umo):
            return
        status = self.store.get_status(umo)
        now = time.time()

        if status == "ok":
            self._probe_fail_reset(umo)
            await self._healthy_probe(umo, now)
            return


        try:
            last = float(self.store.get_state(umo, "last_probe", "0") or 0)
        except (TypeError, ValueError):
            last = 0.0
        if now - last < float(self._cfg("recovery_poll_seconds", 15)):
            return
        self.store.set_state(umo, "last_probe", repr(now))


        if status == "muted":


            until = self.store.get_mute_until(umo)
            ok = await self.core.probe(umo)
            if ok is True:
                pass
            elif ok is None:

                if until and now < until:
                    return
                logger.info("[guard] %s 禁言无权威真值且已到缓存时刻，放行试探", umo)
                self.core.set_ok(umo)
                await self._flush(umo)
                return
            else:


                if until and now >= until:
                    logger.info("[guard] %s 已过缓存解禁时刻但探测仍报禁言，放行试探", umo)
                    self.core.set_ok(umo)
                    await self._flush(umo)
                    return
                self.store.set_state(umo, "recover_streak", "0")

                self._note_probe_detail(umo)
                return
        else:
            ok = await self.core.probe(umo)
            if ok is False:
                self.store.set_state(umo, "recover_streak", "0")
                return
            if ok is None:


                if not self.store.count_outbox(umo):
                    return

                if not self._probe_retry_allowed(umo, now):
                    return
                self.core.set_ok(umo)
                await self._flush(umo)
                return


        n = self.store.bump_recover_streak(umo)
        need = int(self._cfg("recovery_confirm_times", 2))
        if n < need:
            logger.info("[guard] %s 恢复确认 %d/%d", umo, n, need)
            return
        logger.warning("[guard] %s 确认恢复，开始补发", umo)
        self.core.set_ok(umo)
        await self._flush(umo)

    def _note_probe_detail(self, umo: str) -> None:
        try:
            raw = float(self.store.get_state(umo, "probe_raw_until", "0") or 0)
        except (TypeError, ValueError):
            raw = 0.0
        now = time.time()
        last_raw = self.store.get_state(umo, "last_probe_raw", "")
        cur = f"{raw:.0f}"
        if last_raw != cur:
            self.store.set_state(umo, "last_probe_raw", cur)
            self.store.set_state(umo, "probe_raw_seen", repr(now))
        else:
            try:
                same_since = float(self.store.get_state(umo, "probe_raw_seen", "0") or 0)
            except (TypeError, ValueError):
                same_since = now
            stuck = (now - same_since) / 60.0
            if stuck >= 3:
                logger.warning(
                    "[guard] %s 探测时间戳持续 %.1f 分钟不变（%s），"
                    "疑似协议端缓存了陈旧禁言值",
                    umo, stuck,
                    time.strftime("%H:%M:%S", time.localtime(raw)) if raw else "?",
                )

    async def _healthy_probe(self, umo: str, now: float) -> None:
        interval = int(self._cfg("probe_interval_seconds", 120))
        if interval <= 0 or "GroupMessage" not in umo:
            return

        try:
            last_seen = float(self.store.get_state(umo, "last_seen", "0") or 0)
        except (TypeError, ValueError):
            last_seen = 0.0
        if now - last_seen > 6 * 3600:
            return
        try:
            last = float(self.store.get_state(umo, "last_probe", "0") or 0)
        except (TypeError, ValueError):
            last = 0.0
        if now - last < interval:
            return
        self.store.set_state(umo, "last_probe", repr(now))
        await self.core.probe(umo)


    def _probe_retry_allowed(self, umo: str, now: float) -> bool:
        """按失败门槛与下次试探时间判断是否允许重试。"""
        if not bool(self._cfg("probe_retry_enabled", True)):
            return True
        limit = self._probe_fail_limit()
        if limit <= 0 or self._probe_fail_streak(umo) < limit:
            return True
        minutes = self._probe_backoff_minutes()
        if minutes <= 0:
            return False
        try:
            nxt = float(self.store.get_state(umo, "probe_next_try", "0") or 0)
        except (TypeError, ValueError):
            nxt = 0.0
        return now >= nxt

    def _probe_fail_note(self, umo: str) -> None:
        if not bool(self._cfg("probe_retry_enabled", True)):
            return
        streak = self._probe_fail_streak(umo) + 1
        self.store.set_state(umo, "probe_fail_streak", str(streak))
        limit = self._probe_fail_limit()
        if limit <= 0 or streak < limit:
            return
        minutes = self._probe_backoff_minutes()
        if minutes > 0:
            self.store.set_state(
                umo, "probe_next_try", repr(time.time() + minutes * 60)
            )
            logger.warning("[guard] %s 试探补发连续失败 %d 次，转入退避 %.0f 分钟",
                           umo, streak, minutes)
        else:
            logger.warning("[guard] %s 试探补发连续失败 %d 次，已停止试探"
                           "（需 /哨兵 flush 或状态翻转才恢复）", umo, streak)

    def _probe_fail_reset(self, umo: str) -> None:
        if self.store.get_state(umo, "probe_fail_streak", "0") in ("", "0"):
            return
        self.store.set_state(umo, "probe_fail_streak", "0")
        self.store.set_state(umo, "probe_next_try", "0")

    def _probe_fail_streak(self, umo: str) -> int:
        try:
            return int(self.store.get_state(umo, "probe_fail_streak", "0") or 0)
        except (TypeError, ValueError):
            return 0

    def _probe_fail_limit(self) -> int:
        try:
            return int(self._cfg("probe_retry_fail_limit", 3))
        except (TypeError, ValueError):
            return 3

    def _probe_backoff_minutes(self) -> float:
        try:
            return float(self._cfg("probe_retry_backoff_minutes", 10))
        except (TypeError, ValueError):
            return 10.0


    async def _flush(self, umo: str, probe_run: bool = False) -> None:
        """逐条发送本批未过期内容，失败保留待发状态并结束当前批次。
        超出批量的内容仍留队列；probe_run只试发一条。"""
        if umo in self._busy:
            return
        self._busy.add(umo)
        try:
            ttl = float(self._cfg("outbox_ttl_minutes", 30)) * 60
            limit = int(self._cfg("flush_batch_max", 5))
            rows = self.store.due_outbox(umo, ttl, limit)
            if not rows:
                return
            gap = float(self._cfg("flush_throttle_seconds", 4))


            announce = str(self._cfg("flush_announce_text", "") or "").strip()
            first = umo not in self._flushing
            self._flushing.add(umo)
            if announce and first and not probe_run:
                if not await self._send_raw(umo, announce):
                    return
                await asyncio.sleep(gap)

            sent = 0
            for row in rows:
                ok = await self._send_raw(umo, row["content"])
                if not ok:

                    self.store.mark_outbox(row["id"], "pending", "补发失败")
                    self.core.on_send_failed_sync(umo)
                    self._probe_fail_note(umo)
                    return
                self.store.mark_outbox(row["id"], "sent")
                sent += 1
                if probe_run:
                    break
                if gap > 0:
                    await asyncio.sleep(gap)

            left = self.store.count_outbox(umo)
            if not left:
                self._flushing.discard(umo)
            logger.info("[guard] %s 补发 %d 条，剩余 %d 条", umo, sent, left)
            if left:
                logger.warning("[guard] %s 仍有 %d 条未补发（超出单批上限或已过期）",
                               umo, left)
        finally:
            self._busy.discard(umo)

    async def _send_raw(self, umo: str, text: str) -> bool:
        if not text or not text.strip():
            return True
        try:
            from astrbot.core.message.message_event_result import MessageChain
            from astrbot.api.message_components import Plain

            await self.context.send_message(umo, MessageChain(chain=[Plain(text)]))
        except Exception as e:
            logger.warning("[guard] 补发失败 %s：%s", umo, e)
            return False

        self.core.note_success(umo)

        self._probe_fail_reset(umo)
        return True
