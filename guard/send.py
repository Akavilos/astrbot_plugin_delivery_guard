"""显式协作发送：只管理主动调用本接口的消息，不修改宿主方法。"""
from __future__ import annotations

from .core import DeliveryBlocked, extract_text


class GuardSender:
    def __init__(self, context, store, core, cfg):
        self.context = context
        self.store = store
        self.core = core
        self.cfg = cfg

    async def send_message(self, session: str, message_chain, *, replay: bool = False):
        protected = bool(self.cfg("enabled", False)) and self.core.in_scope(session)
        if protected and not replay and self.core.should_block(session):
            text = extract_text(message_chain)
            if text:
                self.store.push_outbox(session, text, "会话不可送达")
            raise DeliveryBlocked("会话当前不可送达：" + session)
        try:
            result = await self.context.send_message(session, message_chain)
            if result is False:
                raise RuntimeError("没有找到目标平台，消息未发送")
        except Exception as error:
            # 补发失败仍由原队列处理，避免同一条消息重复入队或计数两次。
            if protected and not replay and self.cfg("track_outcome", True):
                await self.core.on_send_failed(session, message_chain, error)
            raise
        if protected and not replay and self.cfg("track_outcome", True):
            self.core.note_success(session)
        return result
