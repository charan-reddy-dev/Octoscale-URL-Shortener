"""Buffered click analytics.

Redirects enqueue click events and return immediately; a single background writer drains the queue
every CLICK_FLUSH_INTERVAL_SECONDS (or sooner when CLICK_BATCH_SIZE events are waiting) and writes
each batch in one transaction: one UPDATE per short code and one bulk INSERT of analytics rows.
"""

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import insert, select, update

from app.config import settings
from app.logger import logger


@dataclass(frozen=True)
class ClickEvent:
    short_code: str
    referrer: Optional[str]
    user_agent: Optional[str]


class ClickRecorder:
    def __init__(self, max_queue: int = 100_000):
        self._queue: asyncio.Queue[ClickEvent] = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None
        self.dropped = 0
        self.written = 0

    def record(self, short_code: str, referrer: Optional[str], user_agent: Optional[str]) -> None:
        event = ClickEvent(short_code, referrer[:500] if referrer else None, user_agent[:1000] if user_agent else None)
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="click-recorder")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self.flush()

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(settings.CLICK_FLUSH_INTERVAL_SECONDS)
            try:
                while not self._queue.empty():
                    await self.flush()
            except Exception as exc:  # keep the writer alive; the batch is logged and dropped
                logger.error("click_flush_failed", error=str(exc))

    async def flush(self) -> int:
        """Write up to one batch of queued clicks. Returns the number of events written."""
        batch: list[ClickEvent] = []
        while len(batch) < settings.CLICK_BATCH_SIZE and not self._queue.empty():
            batch.append(self._queue.get_nowait())
        if not batch:
            return 0

        from app import database
        from app.models.url import URL, ClickAnalytic

        codes = {e.short_code for e in batch}
        async with database.AsyncSessionLocal() as session:
            rows = await session.execute(select(URL.id, URL.short_code).where(URL.short_code.in_(codes)))
            ids = {code: url_id for url_id, code in rows.all()}
            counts: dict[int, int] = {}
            analytics = []
            for e in batch:
                url_id = ids.get(e.short_code)
                if url_id is None:
                    continue
                counts[url_id] = counts.get(url_id, 0) + 1
                analytics.append({"url_id": url_id, "referrer": e.referrer, "user_agent": e.user_agent})
            for url_id, n in counts.items():
                await session.execute(update(URL).where(URL.id == url_id).values(clicks=URL.clicks + n))
            if analytics:
                await session.execute(insert(ClickAnalytic), analytics)
            await session.commit()
        self.written += len(analytics)
        return len(analytics)

    @property
    def pending(self) -> int:
        return self._queue.qsize()


click_recorder = ClickRecorder()
