"""Queue lifecycle shared by the GUI and deterministic headless tests."""


class QueueScheduler:
    def __init__(self, session, controller, launch):
        self.session = session
        self.controller = controller
        self.launch = launch
        self.active_id = None

    def resume(self, item):
        if self.controller.busy:
            return False
        self._settle_finished()
        if item.status == "complete":
            return False
        self.session.resume(item.id)
        try:
            started = self.launch(item)
        except (OSError, ValueError, RuntimeError):
            self.session.set_status(item.id, "error")
            self.session.set_queue_running(False)
            raise
        if not started:
            self.session.set_status(item.id, "error")
            self.session.set_queue_running(False)
            return False
        self.active_id = item.id
        return True

    def start(self):
        self.session.set_queue_running(True)
        self.poll()

    def stop(self, *, paused=False):
        self.session.set_queue_running(False)
        item = self.session.get(self.active_id) if self.active_id else None
        if item is not None and item.status == "downloading":
            self.session.set_status(item.id, "paused" if paused else "queued")
        if self.controller.busy:
            self.controller.cancel()

    def _settle_finished(self):
        # A terminal snapshot can appear before socket cleanup finishes.
        # Never start a new worker until the previous thread has exited.
        if self.controller.busy:
            return
        if self.active_id is not None:
            item = self.session.get(self.active_id)
            snap = self.controller.snapshot
            self.active_id = None
            if item is not None:
                if snap is not None and snap.status == "complete":
                    self.session.set_status(item.id, "complete")
                elif item.status == "downloading":
                    status = "error" if snap is None or snap.status != "cancelled" else "paused"
                    self.session.set_status(item.id, status)
                    self.session.set_queue_running(False)

    def poll(self):
        if self.controller.busy:
            return
        self._settle_finished()
        if not self.session.queue_running:
            return
        # Resume an interrupted active item before considering queue order.
        active = self.session.downloading_id()
        item = self.session.get(active) if active else next(
            (item for item in self.session.items() if item.status == "queued"), None)
        if item is None:
            self.session.set_queue_running(False)
        else:
            self.resume(item)

    def close(self):
        # Preserve downloading intent for a running queue. A manual download
        # stays paused on a normal close. Shutdown must never advance the queue.
        self._settle_finished()
        item = self.session.get(self.active_id) if self.active_id else None
        if item is not None:
            snap = self.controller.snapshot
            if snap is not None and snap.status == "complete":
                self.session.set_status(item.id, "complete")
            elif item.status == "downloading" and not self.session.queue_running:
                self.session.pause(item.id)
        self.session.save()
        if self.controller.busy:
            self.controller.cancel()
