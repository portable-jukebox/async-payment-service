import logging

from faststream.middlewares import BaseMiddleware

logger = logging.getLogger(__name__)


class RejectUnhandledMessage(BaseMiddleware):
    """Cover decoding/validation failures before the manual-ACK handler."""

    async def after_processed(self, exc_type=None, exc_val=None, exc_tb=None):
        if isinstance(exc_val, Exception) and not self.msg.processed:
            logger.error("Rejected unhandled message: %s", type(exc_val).__name__)
            await self.msg.reject(requeue=False)
            return True
        return False
