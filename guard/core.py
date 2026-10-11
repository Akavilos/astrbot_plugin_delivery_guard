"""解析会话并检查群禁言，跟踪实际发送结果和不可送达状态。
私聊没有禁言查询结果，故障判断依赖发送失败与恢复试探。"""


from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from astrbot.api import logger
from astrbot.api.message_components import Plain


class DeliveryBlocked(Exception):
    pass


def extract_text(chain: Any) -> str:
    return "".join(comp.text for comp in chain.chain if isinstance(comp, Plain)).strip()


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

        self._cache: dict[str, tuple[float, bool]] = {}


    def remember_event(self, event) -> None:
        """机器人身份来自公开事件，不读取连接库的内部字段。"""
        self.store.set_state(event.unified_msg_origin, "bot_self_id", str(event.get_self_id()))

    def resolve(self, umo: str) -> Target | None:
        pname, mtype, sid = parse_umo(umo)
        if not pname:
            return None
        platform = self.context.get_platform_inst(pname)
        if platform is None or platform.meta().name != "aiocqhttp":
            return None
        bot = platform.get_client()
        self_id = self.store.get_state(umo, "bot_self_id", "")
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
            "bot_own_id": getattr(t, "self_id", ""),
            "api_clients": None,
            "connection_registered": bot is not None,
            "send_tracking": "explicit_guarded_send_and_replay",
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
            try:
                login = await t.bot.call_action("get_login_info")
                t.self_id = str(login.get("user_id", ""))
            except Exception as error:
                logger.warning("[guard] 无法查询机器人身份：%s", error)
                return None
            if not t.self_id.isdigit():
                return None
            self.store.set_state(umo, "bot_self_id", t.self_id)
        routing = {"self_id": int(t.self_id)}

        try:
            info = await t.bot.call_action(
                "get_group_member_info",
                group_id=int(t.group_id),
                user_id=int(t.self_id),
                no_cache=True, **routing,
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
                        "get_group_info", group_id=int(t.group_id), no_cache=True, **routing
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
        # 恢复由后台查询或真实试发确认；不因时间流逝放行未确认的回复。
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
        streak = self._bump_fail(umo)
        if text:
            self.store.push_outbox(umo, text, "发送失败")
        if streak >= int(self._cfg("max_fail_streak", 3)):
            self.set_blocked(umo, f"发送连续失败 {streak} 次")
            logger.warning("[guard] %s 连续 %d 次发送失败，判定不可达", umo, streak)

    def _bump_fail(self, umo: str) -> int:
        n = int(self.store.get_state(umo, "fail_streak", "0") or 0) + 1
        self.store.set_state(umo, "fail_streak", str(n))
        return n

    def note_success(self, umo: str) -> None:
        """仅由本插件发送接口的成功返回确认通路，查询仍使用连续确认次数。"""
        self.store.set_state(umo, "fail_streak", "0")
        if self.store.get_status(umo) != "ok":
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
        lines = ["【我的通信恢复背景】此前一段时间，我的消息未能送达。", "以下是我尚未回应的历史来信："]
        for row in rows:
            who = (row["sender"] or "对方").strip()
            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row["ts"]))
            lines.append(f"· [{when}] {who}：{row['text']}")
        lines.append("我补看的是过去的来信，时间和人物归属保持原样。")
        logger.info("[guard] %s 注入缺席期背景 %d 条", umo, len(rows))
        return "\n".join(lines)
