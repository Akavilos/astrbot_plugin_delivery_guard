"""投递哨兵框架入口：拦截不可送达会话、保存缺席来信并追加恢复背景。
配置与数据独立保存，使用公开事件和协作发送接口，不替换框架发送方法。"""


from __future__ import annotations

import asyncio
from .guard import settings as settings_config
import time

import astrbot.api.star as star
from astrbot.api import logger
from astrbot.core.agent.message import TextPart
from astrbot.api.event import AstrMessageEvent, filter

from .guard.core import GuardCore, extract_text
from .guard.flush import Flusher
from .guard.send import GuardSender
from .guard.store import Store

PLUGIN_NAME = "astrbot_plugin_delivery_guard"


PRIORITY = 100


@star.register(PLUGIN_NAME, "Akavilos", "投递哨兵 · Delivery Guard", "0.1.3")
class DeliveryGuardPlugin(star.Star):
    def __init__(self, context: star.Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.config = config or {}
        settings_config.migrate(self.config)
        self.data_dir = star.StarTools.get_data_dir(PLUGIN_NAME)
        self.store = Store(self.data_dir / "guard.db")


        self.core = GuardCore(self.context, self.store, self._cfg)
        self.sender = GuardSender(self.context, self.store, self.core, self._cfg)
        self.flusher = Flusher(self.sender, self.store, self.core, self._cfg)

        self._tick_task: asyncio.Task | None = None


    def _cfg(self, key: str, default):
        return settings_config.value(self.config, key, default)

    def _enabled(self) -> bool:
        return bool(self._cfg("enabled", False))


    async def initialize(self) -> None:
        logger.info("[guard] 已加载。enabled=%s scope=%s",
                    self._cfg("enabled", False), self._cfg("scope", "all"))
        self._tick_task = asyncio.create_task(self._tick_loop())

    async def terminate(self) -> None:
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass


        self.store.close()
        logger.info("[guard] 已卸载。")

    async def _tick_loop(self) -> None:
        interval = max(5, int(self._cfg("tick_seconds", 15)))
        while True:
            try:
                await asyncio.sleep(interval)
                if self._enabled():
                    await self.flusher.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[guard] 巡检循环异常")


    @filter.on_llm_request(priority=PRIORITY)
    async def on_llm_request(self, event: AstrMessageEvent, req) -> None:
        """在受保护的异常会话记录缺席来信，并停止当前框架事件。"""
        try:
            if not self._enabled():
                return
            umo = event.unified_msg_origin
            self.core.remember_event(event)
            if not self.core.in_scope(umo):
                return


            self.store.set_state(umo, "last_seen", repr(time.time()))


            await self._maybe_probe(umo)

            blocked = self.core.should_block(umo)
            if self._cfg("debug", False) or blocked:
                logger.info("[guard] 本轮判定 umo=%s blocked=%s status=%s reason=%s",
                            umo, blocked, self.store.get_status(umo),
                            self.store.get_state(umo, "reason", ""))
            if not blocked:
                return


            text = (getattr(event, "message_str", "") or "").strip()
            try:
                sender = event.get_sender_name() or ""
            except Exception:
                sender = ""
            self.core.record_incoming(umo, sender, text)

            r = event.get_result()
            if r is not None:
                try:
                    r.chain = []
                except Exception:
                    pass
            event.stop_event()
            logger.info(
                "[guard] 会话 %s 不可送达（%s），本轮不调 LLM。缺席期已收 %d 条",
                umo, self.store.get_state(umo, "reason", "?"), self.store.count_inbox(umo),
            )
        except Exception:

            logger.exception("[guard] on_llm_request 出错，本轮放行")

    async def _maybe_probe(self, umo: str) -> None:
        if "GroupMessage" not in umo:
            return
        interval = int(self._cfg("recovery_poll_seconds", 15)) if self.core.should_block(umo) \
            else int(self._cfg("probe_interval_seconds", 120))
        if interval <= 0:
            return
        try:
            last = float(self.store.get_state(umo, "last_probe", "0") or 0)
        except (TypeError, ValueError):
            last = 0.0
        if time.time() - last < interval:
            return
        self.store.set_state(umo, "last_probe", repr(time.time()))
        await self.core.probe(umo)


    async def send_guarded(self, session: str, message_chain):
        """合作插件可显式调用本接口，获得暂存、失败记录与真实发送结果。"""
        return await self.sender.send_message(session, message_chain)

    @filter.on_decorating_result(priority=-1000)
    async def guard_result(self, event: AstrMessageEvent) -> None:
        """发送前再次核查已知不可达状态，暂存文字，不接管框架正常发送。"""
        if not self._enabled() or event.get_extra("_guard_management", False):
            return
        umo = event.unified_msg_origin
        if not self.core.in_scope(umo):
            return
        self.core.remember_event(event)
        await self._maybe_probe(umo)
        if not self.core.should_block(umo):
            return
        result = event.get_result()
        if result is None:
            return
        if not event.get_extra("_guard_result_queued", False):
            text = extract_text(result)
            if text:
                self.store.push_outbox(umo, text, "发送前确认会话不可送达")
            if any(type(part).__name__ != "Plain" for part in result.chain):
                logger.warning("[guard] 被拦消息包含媒体；待发队列只保存文字，媒体不会自动补发")
            event.set_extra("_guard_result_queued", True)
        result.chain = []
        event.stop_event()

    @filter.on_llm_request(priority=-1000)
    async def inject_brief(self, event: AstrMessageEvent, req) -> None:
        """在请求组装后追加一次恢复背景，不写入其他插件的数据。"""
        try:
            if not self._enabled():
                return
            umo = event.unified_msg_origin
            if not self.core.in_scope(umo):
                return
            brief = self.core.build_brief(umo)
            if not brief:
                return
            req.extra_user_content_parts.append(TextPart(text=brief).mark_as_temp())
        except Exception:
            logger.exception("[guard] 缺席期背景注入失败")


    @filter.command("哨兵")
    async def guard_cmd(self, event: AstrMessageEvent, action: str = ""):
        """查看、探测、补发或标记会话；clear处理收件，on/off控制送达状态。"""
        umo = event.unified_msg_origin
        self.core.remember_event(event)
        event.set_extra("_guard_management", True)
        a = (action or "").strip().lower()

        if a == "diag":
            import json as _json
            d = self.core.diagnostics(umo)
            yield event.plain_result(
                "投递哨兵 · 诊断\n" + _json.dumps(d, ensure_ascii=False, indent=2)
            )
            return

        if a == "probe":
            before = self.store.get_status(umo)
            ok = await self.core.probe(umo)
            tip = {True: "可送达", False: "不可送达（已标记）",
                   None: "探测未拿到真值 —— 见日志 [guard·DIAG] / [guard] 探测失败"}.get(ok, "未知")
            after = self.store.get_status(umo)
            extra = ""
            if before == "ok" and after != "ok":
                extra = "\n⚠️ 状态由正常变为异常，说明探测生效"
            elif ok is False and after == "ok":
                extra = "\n⚠️ 探到禁言但状态没落地，疑似缓存/写入问题"
            yield event.plain_result(f"探测结果：{tip}\n状态：{before} → {after}{extra}")
            return

        if a == "flush":
            n = self.store.count_outbox(umo)
            if not n:
                yield event.plain_result("没有待补发的内容。")
                return
            await self.flusher._flush(umo)
            yield event.plain_result(
                f"已尝试补发，剩余 {self.store.count_outbox(umo)} 条 / 原 {n} 条。"
            )
            return

        if a == "clear":
            self.store.mark_inbox_delivered(umo)
            yield event.plain_result("缺席期收件箱已清空（不再作为背景注入）。")
            return

        if a == "on":
            self.core.set_ok(umo)
            self.store.set_state(umo, "fail_streak", "0")
            yield event.plain_result("已把本会话标记为可送达。")
            return

        if a == "off":
            self.core.set_blocked(umo, "手动标记不可送达")
            yield event.plain_result("已把本会话标记为不可送达（拦截将生效）。")
            return

        status = self.store.get_status(umo)
        label = {"ok": "正常", "muted": "群禁言", "unreachable": "不可达（拒收/未知）"}
        out = self.store.recent_outbox(umo, 5)
        lines = [
            "投递哨兵状态",
            f"· 全局开关：{self._cfg('enabled', False)}",
            f"· 本会话：{label.get(status, status)}",
            f"· 判定原因：{self.store.get_state(umo, 'reason', '无') or '无'}",
            f"· 缺席期收件：{self.store.count_inbox(umo)} 条",
            f"· 待补发：{self.store.count_outbox(umo)} 条",
        ]
        if out:
            lines.append("〔最近未送达〕")
            for r in out:
                ts = time.strftime("%m-%d %H:%M", time.localtime(r["created_ts"]))
                lines.append(f"  #{r['id']} [{r['status']}] {ts} {r['content'][:20]}")
        yield event.plain_result("\n".join(lines))
