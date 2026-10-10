"""解析会话并检查群禁言，跟踪实际发送结果和不可送达状态。
私聊没有禁言查询结果，故障判断依赖发送失败与恢复试探。"""


from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from astrbot.api import logger


class DeliveryBlocked(Exception):
    pass


def extract_text(chain: Any) -> str:
    parts: list[str] = []
    try:
        for comp in getattr(chain, "chain", None) or []:
            text = getattr(comp, "text", None)
            if text and type(comp).__name__ == "Plain":
                parts.append(text)
    except Exception:
        pass
    return "".join(parts).strip()


@dataclass
class Target:
    platform: Any
    bot: Any
    is_group: bool
    group_id: str
    user_id: str
    self_id: str


def parse_umo(umo: str) -> tuple[str, str, str]:
    parts = str(umo).split(":")
    if len(parts) < 3:
        return "", "", ""
    return parts[0], parts[1], ":".join(parts[2:])


class GuardCore:
    def __init__(self, context, store, cfg) -> None:
        self.context = context
        self.store = store
        self._cfg = cfg
        self._real_context = context

        self._cache: dict[str, tuple[float, bool]] = {}


    @staticmethod
    def _bot_self_id(bot: Any) -> str:
        clients = getattr(bot, "_wsr_api_clients", None)
        if isinstance(clients, dict) and clients:
            for key in clients:
                s = str(key)
                if s.isdigit():
                    return s
        return ""

    def resolve(self, umo: str) -> Target | None:
        pname, mtype, sid = parse_umo(umo)
        if not pname:
            return None
        platform = None
        try:
            for p in self.context.platform_manager.platform_insts:
                try:
                    if p.meta().id == pname:
                        platform = p
                        break
                except Exception:
                    continue
        except Exception:
            return None
        if platform is None:
            return None
        bot = getattr(platform, "bot", None)
        self_id = self._bot_self_id(bot) or str(getattr(platform, "self_id", "") or "")
        is_group = mtype == "GroupMessage"
        return Target(
            platform=platform,
            bot=bot,
            is_group=is_group,
            group_id=sid if is_group else "",
            user_id="" if is_group else sid,
            self_id=self_id,
        )

    def _diag(self, umo: str, msg: str) -> None:
        logger.warning("[guard·DIAG] %s | %s", umo, msg)

    def diagnostics(self, umo: str) -> dict:
        t = self.resolve(umo)
        bot = getattr(t, "bot", None) if t else None
        api_clients = getattr(bot, "_wsr_api_clients", None) if bot is not None else None
        return {
            "umo": umo,
            "status": self.store.get_status(umo),
            "reason": self.store.get_state(umo, "reason", ""),
            "mute_until": self.store.get_mute_until(umo),
            "in_scope": self.in_scope(umo),
            "platform": getattr(t, "platform", None) is not None,
            "is_group": getattr(t, "is_group", None),
            "group_id": getattr(t, "group_id", ""),
            "self_id": getattr(t, "self_id", ""),
            "bot_own_id": str(getattr(bot, "own_id", "") or "") if bot is not None else "",
            "api_clients": (
                list(api_clients.keys()) if isinstance(api_clients, dict) else
                (f"<{type(api_clients).__name__}>" if api_clients is not None else None)
            ),
            "sent_today": self.store.sent_today(umo),
        }

    def in_scope(self, umo: str) -> bool:
        scope = str(self._cfg("scope", "all"))
        if scope == "all":
            return True
        is_group = "GroupMessage" in umo
        return (scope == "group") == is_group


    async def probe(self, umo: str) -> bool | None:
        """群聊查询禁言信息并返回真、假或未知；私聊没有该查询路径。"""
        t = self.resolve(umo)
        if t is None:
            self._diag(umo, "解析不到平台实例，跳过探测")
            return None
        if not t.is_group or t.bot is None:
            return None
        if not self._cfg("probe_groups", True):
            return None
        if not t.group_id.isdigit():
            self._diag(umo, f"群号不合法: {t.group_id!r}")
            return None
        if not t.self_id.isdigit():

            self._diag(umo, f"拿不到机器人 self_id: {t.self_id!r}")
            return None

        try:
            info = await t.bot.call_action(
                "get_group_member_info",
                group_id=int(t.group_id),
                user_id=int(t.self_id),
                no_cache=True,
            )
        except Exception as e:


            logger.warning(
                "[guard] 探测失败 %s（群 %s / QQ %s）：%s: %s",
                umo, t.group_id, t.self_id, type(e).__name__, str(e)[:160],
            )
            return None
        if not isinstance(info, dict):
            self._diag(umo, f"探测返回非字典: {type(info).__name__}")
            return None
        try:
            until = float(info.get("shut_up_timestamp", 0) or 0)
        except (TypeError, ValueError):
            until = 0.0
        now = time.time()

        self.store.set_state(umo, "probe_raw_until", repr(until))
        if until > now:

            was = self.store.get_status(umo)
            self.set_blocked(umo, "群禁言", mute_until=until)

            if was == "ok":
                logger.warning(
                    "[guard] %s 探测到群禁言，解禁于 %s",
                    umo,
                    time.strftime("%m-%d %H:%M:%S", time.localtime(until)),
                )
            return False


        if self._cfg("probe_all_shut", True):
            role = str(info.get("role", "") or "")
            if role not in ("owner", "admin"):
                try:
                    ginfo = await t.bot.call_action(
                        "get_group_info", group_id=int(t.group_id), no_cache=True
                    )
                except Exception:
                    ginfo = None
                if isinstance(ginfo, dict) and ginfo.get("group_all_shut"):

                    self.set_blocked(umo, "群全员禁言", mute_until=0.0)
                    logger.warning("[guard] %s 探测到全员禁言（机器人非管理员）", umo)
                    return False

        return True


    _CACHE_OK = 0.4
    _CACHE_BLOCK = 5.0

    def should_block(self, umo: str) -> bool:
        now = time.time()
        hit = self._cache.get(umo)
        if hit is not None and hit[0] > now:
            return hit[1]
        blocked = self._should_block_slow(umo, now)
        ttl = self._CACHE_BLOCK if blocked else self._CACHE_OK
        self._cache[umo] = (now + ttl, blocked)
        return blocked

    def _should_block_slow(self, umo: str, now: float) -> bool:
        if not self.in_scope(umo):
            return False
        status = self.store.get_status(umo)
        if status == "ok":
            return False
        if status == "muted":
            until = self.store.get_mute_until(umo)

            if until and now < until:
                return True
            return False


        try:
            last = float(self.store.get_state(umo, "last_canary", "0") or 0)
        except (TypeError, ValueError):
            last = 0.0
        if now - last >= float(self._cfg("recovery_poll_seconds", 15)):
            self.store.set_state(umo, "last_canary", repr(now))
            logger.info("[guard] %s 放行一条探针消息，用于试探是否恢复", umo)
            return False
        return True

    async def on_send_failed(self, umo: str, chain: Any, err: BaseException) -> None:
        """先核查群禁言；无探测结果时累计失败，达到门槛才标记不可达。"""
        self._cache.pop(umo, None)
        text = extract_text(chain)
        status = await self.probe(umo)
        if status is False:

            if text:
                oid = self.store.push_outbox(umo, text, "群禁言")
                logger.warning("[guard] %s 发送失败且探测确认被拦，已暂存 #%d：%.40s",
                               umo, oid, text)
            return
        if status is None:
            streak = self._bump_fail(umo)
            if streak >= int(self._cfg("max_fail_streak", 3)):
                if text:
                    self.store.push_outbox(umo, text, "发送失败")
                self.set_blocked(umo, f"发送连续失败 {streak} 次")
                logger.warning("[guard] %s 连续 %d 次发送失败，判定不可达", umo, streak)

    def _bump_fail(self, umo: str) -> int:
        n = int(self.store.get_state(umo, "fail_streak", "0") or 0) + 1
        self.store.set_state(umo, "fail_streak", str(n))
        return n

    def note_success(self, umo: str) -> None:
        """清除失败计数，累计恢复确认并登记发送数量。"""
        self.store.set_state(umo, "fail_streak", "0")
        if self.store.get_status(umo) != "ok":
            n = self.store.bump_recover_streak(umo)
            if n >= int(self._cfg("recovery_confirm_times", 2)):
                logger.info("[guard] %s 发送成功，判定已恢复", umo)
                self.set_ok(umo)
        self.store.bump_sent(umo)

    def on_send_failed_sync(self, umo: str) -> None:
        self._invalidate(umo)
        streak = self._bump_fail(umo)
        if streak >= int(self._cfg("max_fail_streak", 3)):
            self.set_blocked(umo, f"补发连续失败 {streak} 次")


    def _invalidate(self, umo: str) -> None:
        self._cache.pop(umo, None)

    def set_blocked(self, umo: str, reason: str, mute_until: float = 0.0) -> None:
        self.store.mark_blocked(umo, reason, mute_until)
        self._invalidate(umo)

    def set_ok(self, umo: str) -> None:
        self.store.mark_ok(umo)
        self._invalidate(umo)

        self.store.set_state(umo, "last_canary", "0")


    _orig_platform_send = None
    _patched_class = None

    def patch_platform_send(self) -> None:
        """包装QQ平台实际发送入口，成功与异常分别更新送达跟踪。"""
        if not self._cfg("track_outcome", True):
            return
        if self.__class__._patched_class is not None:
            return
        try:
            from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
                AiocqhttpMessageEvent,
            )
        except Exception:
            logger.warning("[guard] 找不到 AiocqhttpMessageEvent，主回复结果跟踪不可用")
            return


        raw = AiocqhttpMessageEvent.__dict__["send_message"].__func__
        guard = self

        async def patched(cls, bot, message_chain, event=None, is_group=False,
                          session_id=None):
            umo = guard.umo_from_send(is_group, session_id)
            try:
                result = await raw(
                    cls, bot, message_chain,
                    event=event, is_group=is_group, session_id=session_id,
                )
            except BaseException as e:
                await guard.on_send_failed(umo, message_chain, e)
                raise
            guard.note_success(umo)
            return result

        patched.__guard_patched__ = True
        AiocqhttpMessageEvent.send_message = classmethod(patched)
        type(self)._orig_platform_send = raw
        type(self)._patched_class = AiocqhttpMessageEvent
        logger.info("[guard] 已给 %s 打补丁，主回复发送结果可见",
                    AiocqhttpMessageEvent.__name__)

    def umo_from_send(self, is_group: bool, session_id: Any) -> str:
        sid = str(session_id or "")
        kind = "GroupMessage" if is_group else "FriendMessage"
        return f"default:{kind}:{sid}"

    def unpatch_platform_send(self) -> None:
        cls = type(self)._patched_class
        raw = type(self)._orig_platform_send
        if cls is not None and raw is not None:
            try:
                cls.send_message = classmethod(raw)
            except Exception:
                logger.exception("[guard] 还原平台类方法失败")
        type(self)._patched_class = None
        type(self)._orig_platform_send = None


    def record_incoming(self, umo: str, sender: str, text: str) -> None:
        if not text or not text.strip():
            return
        self.store.push_inbox(umo, sender, text)
        if self._cfg("debug", False):
            logger.info("[guard] %s 缺席期收件 <- %s：%.40s", umo, sender, text)

    def build_brief(self, umo: str) -> str:
        """读取恢复背景并消费对应收件标记，避免后续请求重复注入。"""
        if not self._cfg("brief_on_recovery", True):
            return ""
        if self.store.get_state(umo, "need_brief") != "1":
            return ""
        limit = int(self._cfg("max_brief_items", 8))
        rows = self.store.peek_inbox(umo, limit)
        self.store.mark_inbox_delivered(umo)
        self.store.set_state(umo, "need_brief", "0")
        if not rows:
            return ""
        lines = ["【投递哨兵】前面这段时间消息没能送达给你，现在恢复了。",
                 "下面这些是你没收到的那段里，对方发过来的："]
        for r in rows:
            who = (r["sender"] or "对方").strip()
            lines.append(f"· {who}：{r['text']}")
        lines.append("这些是刚补上的上下文，不用逐条回应，按平时的节奏来就好。")
        logger.info("[guard] %s 注入缺席期背景 %d 条", umo, len(rows))
        return "\n".join(lines)
