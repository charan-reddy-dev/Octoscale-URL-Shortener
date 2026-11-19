"""Buffered click analytics.

Redirects enqueue click events and return immediately; a background writer per worker process drains
the queue every CLICK_FLUSH_INTERVAL_SECONDS (or sooner when CLICK_BATCH_SIZE events are waiting) and
writes each batch in one transaction: click counters are incremented in ascending id order (so
concurrent writers in other processes always lock rows in the same order and cannot deadlock) and the
analytics rows are inserted in bulk. A batch that fails is put back on the queue and retried up to
CLICK_MAX_ATTEMPTS times before it is counted as dropped.
"""

import asyncio
import contextlib
from dataclasses import dataclass, replace
from typing import Optional

from sqlalchemy import bindparam, insert, select, update

from app.config import settings
from app.logger import logger

CLICK_MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class ClickEvent:
    short_code: str
    referrer: Optional[str]
    user_agent: Optional[str]
    attempts: int = 0


class ClickRecorder:
    def __init__(self, max_queue: int = 200_000):
        self._queue: asyncio.Queue[ClickEvent] = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None
        self.written = 0
        self.retried = 0
        self.dropped = 0

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
        while self.pending:
            if await self.flush() == 0 and self.pending:
                break

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(settings.CLICK_FLUSH_INTERVAL_SECONDS)
            # Stop draining on a failed or empty write and wait for the next interval before retrying.
            while self.pending and await self.flush():
                pass

    async def flush(self) -> int:
        """Write up to one batch of queued clicks. Returns the number of click rows written."""
        batch: list[ClickEvent] = []
        while len(batch) < settings.CLICK_BATCH_SIZE and not self._queue.empty():
            batch.append(self._queue.get_nowait())
        if not batch:
            return 0
        try:
            written = await self._write(batch)
        except Exception as exc:
            self._requeue(batch, exc)
            return 0
        self.written += written
        return written

    async def _write(self, batch: list[ClickEvent]) -> int:
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
            if counts:
                stmt = (
                    update(URL.__table__)
                    .where(URL.__table__.c.id == bindparam("b_id"))
                    .values(clicks=URL.__table__.c.clicks + bindparam("b_n"))
                )
                # Ascending id order: every writer locks rows in the same order, so writers never deadlock.
                await session.execute(stmt, [{"b_id": i, "b_n": n} for i, n in sorted(counts.items())])
            if analytics:
                await session.execute(insert(ClickAnalytic), analytics)
            await session.commit()
        return len(analytics)

    def _requeue(self, batch: list[ClickEvent], exc: Exception) -> None:
        requeued = dropped = 0
        for e in batch:
            if e.attempts + 1 >= CLICK_MAX_ATTEMPTS:
                dropped += 1
                continue
            try:
                self._queue.put_nowait(replace(e, attempts=e.attempts + 1))
                requeued += 1
            except asyncio.QueueFull:
                dropped += 1
        self.retried += requeued
        self.dropped += dropped
        logger.error("click_flush_failed", error=str(exc)[:300], requeued=requeued, dropped=dropped)

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    def stats(self) -> dict:
        return {"pending": self.pending, "written": self.written, "retried": self.retried, "dropped": self.dropped}


click_recorder = ClickRecorder()
