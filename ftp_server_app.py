#!/usr/bin/env python3
"""
FlashDash FTP/TFTP Server — GUI Application for Linux
Backend: pyftpdlib / tftpy  |  Frontend: PyQt5

Usage:
  python ftp_server_app.py            # launch GUI
  python ftp_server_app.py --daemon   # headless daemon (used internally by pkexec)
"""

import sys
import os
import json
import logging
import socket
import threading
import subprocess
import argparse
import uuid
import queue
import time
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CLI arguments — parsed before PyQt5 so daemon mode runs without a display
# ─────────────────────────────────────────────────────────────────────────────
_ap = argparse.ArgumentParser(add_help=False)
_ap.add_argument("--daemon", action="store_true",
                 help="Run as headless FTP daemon (called by pkexec)")
_ap.add_argument("--config", default="",
                 help="Path to JSON config file (daemon mode)")
_ap.add_argument("--socket", default="",
                 help="Path for Unix control socket (daemon mode)")
_ARGS, _ = _ap.parse_known_args()


# ─────────────────────────────────────────────────────────────────────────────
# Resource path helper (survives PyInstaller --onefile bundling)
# ─────────────────────────────────────────────────────────────────────────────
def _resource_path(relative: str) -> str:
    """Return absolute path to a bundled or source resource."""
    if hasattr(sys, "_MEIPASS"):          # PyInstaller extracted bundle
        return str(Path(sys._MEIPASS) / relative)
    return str(Path(__file__).parent / relative)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
if sys.platform == "win32":
    _CONFIG_DIR = Path(os.environ.get("APPDATA", Path.home())) / "FlashDash"
else:
    _CONFIG_DIR = Path.home() / ".config" / "flashdash"
_CONFIG_FILE = _CONFIG_DIR / "config.json"

_DEFAULTS: dict = {
    "port":        21,
    "root_folder": str(Path.home()),
    "auth_type":   "anonymous",   # "anonymous" | "user"
    "username":    "ftpuser",
    "password":    "",
    "protocol":    "ftp",         # "ftp" | "tftp"
}


def _load_config(path=None) -> dict:
    try:
        p = Path(path) if path else _CONFIG_FILE
        return {**_DEFAULTS, **json.loads(p.read_text())}
    except Exception:
        return _DEFAULTS.copy()


def _save_config(cfg: dict) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps(cfg, indent=2))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_bytes(n: int) -> str:
    """Human-readable byte count."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


# ─────────────────────────────────────────────────────────────────────────────
# FTP server factory
# ─────────────────────────────────────────────────────────────────────────────
def _make_server(cfg: dict, log_fn=None, progress_fn=None):
    """Return a configured pyftpdlib FTPServer instance."""
    from pyftpdlib.handlers import FTPHandler, DTPHandler  # type: ignore
    from pyftpdlib.servers import ThreadedFTPServer         # type: ignore
    from pyftpdlib.authorizers import DummyAuthorizer, AuthenticationFailed  # type: ignore

    # Silence pyftpdlib's default logger; we use our own callbacks instead.
    logging.getLogger("pyftpdlib").handlers = []
    logging.getLogger("pyftpdlib").setLevel(logging.CRITICAL)

    _fn   = log_fn        # capture for use inside method bodies
    _pfn  = progress_fn    # progress callback; may be None

    # Cisco IOS uses '*' as the anonymous FTP username instead of 'anonymous'.
    # Map it transparently so anonymous mode works without credentials.
    class _CiscoAuthorizer(DummyAuthorizer):
        _ANON_ALIASES = frozenset(('*', 'ftp'))

        def _resolve(self, username):
            if username in self._ANON_ALIASES and 'anonymous' in self.user_table:
                return 'anonymous'
            return username

        def validate_authentication(self, username, password, handler):
            super().validate_authentication(self._resolve(username), password, handler)

        def get_home_dir(self, username):
            return super().get_home_dir(self._resolve(username))

        def has_perm(self, username, perm, path=None):
            return super().has_perm(self._resolve(username), perm, path)

        def get_perms(self, username):
            return super().get_perms(self._resolve(username))

        def get_msg_login(self, username):
            return super().get_msg_login(self._resolve(username))

        def get_msg_quit(self, username):
            return super().get_msg_quit(self._resolve(username))

    _auth = _CiscoAuthorizer()
    root  = cfg["root_folder"]
    if cfg["auth_type"] == "anonymous":
        _auth.add_anonymous(root, perm="elr")
    else:
        _auth.add_user(
            cfg["username"], cfg["password"], root, perm="elradfmwMT"
        )

    def _emit(msg: str) -> None:
        if _fn:
            _fn(msg)

    class _FastDTPHandler(DTPHandler):
        # How much Python reads/writes per asyncore iteration (1 MiB).
        BUFFER_SIZE = 1 << 20  # 1 MiB

        def __init__(self, sock, cmd_channel):
            super().__init__(sock, cmd_channel)
            try:
                self.socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
                self.socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_SNDBUF, 8 << 20)
            except Exception:
                pass
            self._xfer_id    = id(self)
            self._xfer_start = time.monotonic()
            self._last_prog  = 0.0

        # ── Download (RETR) tracking ──────────────────────────────────
        # pyftpdlib on Linux uses os.sendfile() for RETR, replacing
        # initiate_send with initiate_sendfile at runtime.  Both paths
        # update tot_bytes_sent, so we hook the two entry points:
        #   • send()              — used when sendfile is unavailable
        #   • initiate_sendfile() — used on Linux via os.sendfile()
        # handle_write still calls one of these, so we keep it too as a
        # safety net.

        def send(self, data):
            result = super().send(data)
            self._push_progress()
            return result

        def initiate_sendfile(self):
            super().initiate_sendfile()
            self._push_progress()

        def handle_write(self):
            super().handle_write()
            self._push_progress()

        # ── Upload (STOR) tracking ────────────────────────────────────
        def handle_read(self):
            super().handle_read()
            self._push_progress()

        # ── Common progress helpers ───────────────────────────────────
        def _push_progress(self):
            if not _pfn:
                return
            now = time.monotonic()
            if now - self._last_prog < 0.1:   # throttle to ~10 updates/s
                return
            self._last_prog = now
            self._emit_progress(done=False)

        def _emit_progress(self, done: bool) -> None:
            try:
                info = getattr(self.cmd_channel, '_transfer_info', None)
                if not info:
                    return
                direction = info['direction']

                if direction == 'down':
                    # For downloads, progress is tracked against the whole-file
                    # session (stable across REST segments).  Never emit done=True
                    # here — that is handled by on_file_sent / on_incomplete_file_sent
                    # so the GUI bar doesn't flash off between segments.
                    if done:
                        return
                    key  = info.get('_key')
                    with _retr_lock:
                        sess = _retr_sessions.get(key) if key else None
                    if sess is None:
                        return
                    segment_sent = self.tot_bytes_sent
                    cumulative   = info['rest_pos'] + segment_sent
                    with _retr_lock:
                        if key in _retr_sessions:
                            _retr_sessions[key]['bytes_done'] = cumulative
                    elapsed = max(time.monotonic() - sess['start_t'], 1e-6)
                    speed   = cumulative / elapsed
                    _pfn({
                        'id':          sess['xfer_id'],
                        'ip':          self.cmd_channel.remote_ip,
                        'name':        info['name'],
                        'direction':   'down',
                        'bytes_done':  cumulative,
                        'total_bytes': info['size'],
                        'speed_bps':   speed,
                        'done':        False,
                    })
                else:
                    elapsed     = max(time.monotonic() - self._xfer_start, 1e-6)
                    transferred = self.tot_bytes_received
                    speed       = transferred / elapsed
                    _pfn({
                        'id':          self._xfer_id,
                        'ip':          self.cmd_channel.remote_ip,
                        'name':        info['name'],
                        'direction':   'up',
                        'bytes_done':  transferred,
                        'total_bytes': info['size'],
                        'speed_bps':   speed,
                        'done':        done,
                    })
                    if done:
                        self.cmd_channel._last_xfer_speed = speed
            except Exception:
                pass   # never let progress tracking break the transfer

        def handle_close(self):
            # pyftpdlib's built-in transfer_finished check uses
            # len(producer_fifo)==0, but when os.sendfile() is used
            # producer_fifo is always empty regardless of completion state.
            # Override: treat transfer as complete if we sent >= file size,
            # or if the cmd_channel has _quit_pending (Cisco sends QUIT
            # immediately after issuing RETR, before the data finishes).
            try:
                info = getattr(self.cmd_channel, '_transfer_info', None)
                if info and info['direction'] == 'down' and info.get('size'):
                    expected = info['size'] - info.get('rest_pos', 0)
                    if self.tot_bytes_sent >= expected:
                        self.transfer_finished = True
                    elif getattr(self.cmd_channel, '_quit_pending', False):
                        # QUIT was received while data was still flowing —
                        # this is normal Cisco behaviour.  Mark complete so
                        # pyftpdlib calls on_file_sent instead of
                        # on_incomplete_file_sent.
                        self.transfer_finished = True
            except Exception:
                pass
            try:
                self._emit_progress(done=True)
            except Exception:
                pass
            super().handle_close()

    # Tracks in-progress multi-segment (REST+RETR) downloads across connections.
    # Key: (remote_ip, filename)
    # Value: {'size', 'bytes_done', 'start_t', 'xfer_id'}
    # xfer_id is fixed at the FIRST segment so the GUI shows one continuous bar.
    _retr_sessions: dict = {}
    _retr_lock = threading.Lock()

    # Event shared with ThreadedFTPServer threads — setting it stops the loops.
    _ftp_exit = threading.Event()

    class _Handler(FTPHandler):
        authorizer    = _auth
        passive_ports = range(60000, 60100)
        banner        = "FTP Server ready."
        dtp_handler   = _FastDTPHandler
        # Give the per-connection _loop() the same exit event so Stop
        # kills in-flight transfers, not just the accept loop.
        _exit         = _ftp_exit

        # ── Connection / auth events ──────────────────────────────────
        def on_connect(self):
            _emit(f"[+] {self.remote_ip}  connected")

        def on_disconnect(self):
            _emit(f"[-] {self.remote_ip}  disconnected")

        def on_login(self, username):
            _emit(f"[✓] {self.remote_ip}  logged in  (user: {username!r})")

        def on_login_failed(self, username, password):
            _emit(f"[✗] {self.remote_ip}  login FAILED  (user: {username!r})")

        def on_logout(self, username):
            _emit(f"[~] {self.remote_ip}  logged out  (user: {username!r})")

        def ftp_QUIT(self, line):
            """Delay the 221 goodbye until data transfer is done.

            Cisco IOS sends QUIT immediately after issuing RETR (before
            data is finished), then closes the data connection as soon as
            it receives the 221 response.  By withholding the 221 until
            the data channel closes naturally, the switch keeps the data
            connection alive and receives the whole file in one go.
            """
            if self.data_channel:
                try:
                    msg = (self.authorizer.get_msg_quit(self.username)
                           if self.authenticated else "Goodbye.")
                except Exception:
                    msg = "Goodbye."
                self._delayed_goodbye = (
                    f"221 {msg}" if len(msg) <= 75 else f"221-{msg}\r\n221 "
                )
                self._quit_pending = True
                if self.authenticated and self.username:
                    self.on_logout(self.username)
                self.del_channel()
                # intentionally NOT responding with 221 here
            else:
                super().ftp_QUIT(line)

        def _send_delayed_goodbye(self):
            """Send the deferred 221 response if ftp_QUIT was called during transfer."""
            msg = getattr(self, '_delayed_goodbye', None)
            if msg:
                del self._delayed_goodbye
                try:
                    self.respond(msg)
                except Exception:
                    pass

        # ── Transfer start ────────────────────────────────────────────
        def ftp_RETR(self, file):
            """Client downloading a file from the server."""
            name     = os.path.basename(file)
            rest_pos = self._restart_position
            key      = (self.remote_ip, name)

            # Try to get the file size via ftp2fs; if path resolution fails
            # (can happen when Cisco sends an absolute filesystem path as the
            # FTP argument), proceed without a known size — the bar will show
            # bytes/speed only.  We also try reading the size from the real
            # file after super() opens it (see below).
            size = None
            try:
                full = self.fs.ftp2fs(file)
                if os.path.isfile(full):
                    size = os.path.getsize(full)
            except Exception:
                pass

            with _retr_lock:
                if key not in _retr_sessions:
                    _retr_sessions[key] = {
                        'size':       size,
                        'bytes_done': rest_pos,
                        'start_t':    time.monotonic(),
                        'xfer_id':    id(self),
                        'new':        True,
                    }
                else:
                    sess = _retr_sessions[key]
                    if rest_pos > 0:
                        sess['bytes_done'] = rest_pos
                    # If size wasn't known before but is now, update it
                    if size and not sess.get('size'):
                        sess['size'] = size

                sess = _retr_sessions[key]

            self._transfer_info = {
                'name':      name,
                'size':      sess.get('size'),
                'direction': 'down',
                'rest_pos':  rest_pos,
                '_key':      key,
            }

            if sess.get('new'):
                sess['new'] = False
                size_str = f"  ({_fmt_bytes(size)})" if size else ""
                _emit(f"[↓] {self.remote_ip}  download start  {name}{size_str}")
                if _pfn:
                    _pfn({
                        'id':          sess['xfer_id'],
                        'ip':          self.remote_ip,
                        'name':        name,
                        'direction':   'down',
                        'bytes_done':  sess['bytes_done'],
                        'total_bytes': sess.get('size'),
                        'speed_bps':   0,
                        'done':        False,
                    })

            super().ftp_RETR(file)

            # After super() opens and sets up the data channel, grab the real
            # file size from the open file descriptor (works regardless of how
            # Cisco formatted the RETR path argument).
            try:
                if self.data_channel and self.data_channel.file_obj:
                    real_size = os.path.getsize(self.data_channel.file_obj.name)
                    if real_size and not sess.get('size'):
                        sess['size'] = real_size
                        self._transfer_info['size'] = real_size
            except Exception:
                pass

        def ftp_STOR(self, file, mode="w"):
            """Client uploading a file to the server."""
            self._transfer_info = {
                'name':      os.path.basename(file),
                'size':      None,   # unknown until transfer completes
                'direction': 'up',
            }
            _emit(
                f"[↑] {self.remote_ip}  upload start  "
                f"{os.path.basename(file)}"
            )
            super().ftp_STOR(file, mode)

        # ── Transfer complete ─────────────────────────────────────────
        def on_file_sent(self, file):
            """Called when a RETR segment completes cleanly."""
            name = os.path.basename(file)
            key  = (self.remote_ip, name)
            with _retr_lock:
                sess = _retr_sessions.get(key)
                if sess:
                    # Accumulate this segment
                    sess['bytes_done'] = (
                        getattr(self, '_transfer_info', None) or {}
                    ).get('rest_pos', 0) + self.tot_bytes_sent
                    done = sess['bytes_done'] >= sess['size']
                else:
                    done = True  # no session tracking — single-connection transfer

            if done:
                try:
                    size     = os.path.getsize(file)
                    size_str = f"  ({_fmt_bytes(size)})"
                except Exception:
                    size_str = ""
                spd = getattr(self, '_last_xfer_speed', None)
                speed_str = f"  @ {_fmt_bytes(int(spd))}/s" if spd else ""
                self._transfer_info = None
                # Emit final progress
                if _pfn:
                    with _retr_lock:
                        sess2 = _retr_sessions.get(key)
                    if sess2:
                        elapsed = max(time.monotonic() - sess2['start_t'], 1e-6)
                        _pfn({
                            'id':          sess2['xfer_id'],
                            'ip':          self.remote_ip,
                            'name':        name,
                            'direction':   'down',
                            'bytes_done':  sess2['size'],
                            'total_bytes': sess2['size'],
                            'speed_bps':   sess2['size'] / elapsed,
                            'done':        True,
                        })
                with _retr_lock:
                    _retr_sessions.pop(key, None)
                _emit(f"[✔] {self.remote_ip}  download done   {name}{size_str}{speed_str}")
            else:
                # Mid-transfer segment — keep session alive for next segment
                self._transfer_info = None
            self._send_delayed_goodbye()

        def on_file_received(self, file):
            """Upload finished."""
            try:
                size = os.path.getsize(file)
                size_str = f"  ({_fmt_bytes(size)})"
            except Exception:
                size_str = ""
            spd = getattr(self, '_last_xfer_speed', None)
            speed_str = f"  @ {_fmt_bytes(int(spd))}/s" if spd else ""
            self._transfer_info = None
            _emit(
                f"[✔] {self.remote_ip}  upload done     "
                f"{os.path.basename(file)}{size_str}{speed_str}"
            )

        def on_incomplete_file_sent(self, file):
            name = os.path.basename(file)
            key  = (self.remote_ip, name)

            # Accumulate bytes from this segment
            seg_bytes = 0
            if self.data_channel:
                seg_bytes = self.data_channel.tot_bytes_sent

            with _retr_lock:
                sess = _retr_sessions.get(key)
                if sess and seg_bytes:
                    sess['bytes_done'] += seg_bytes

            is_segment = bool(sess) or getattr(self, '_quit_pending', False)

            if is_segment:
                if _pfn and sess:
                    elapsed = max(time.monotonic() - sess['start_t'], 1e-6)
                    _pfn({
                        'id':          sess['xfer_id'],
                        'ip':          self.remote_ip,
                        'name':        name,
                        'direction':   'down',
                        'bytes_done':  sess['bytes_done'],
                        'total_bytes': sess['size'],
                        'speed_bps':   sess['bytes_done'] / elapsed,
                        'done':        False,
                    })
            else:
                if _pfn:
                    _pfn({'id': id(self), 'ip': self.remote_ip,
                          'name': name, 'direction': 'down',
                          'bytes_done': 0, 'total_bytes': None,
                          'speed_bps': 0, 'done': True})
                _emit(f"[!] {self.remote_ip}  download INTERRUPTED  {name}")
            self._send_delayed_goodbye()

        def on_incomplete_file_received(self, file):
            try:
                os.remove(file)
            except Exception:
                pass
            _emit(f"[!] {self.remote_ip}  upload INTERRUPTED  {os.path.basename(file)}")

    server = ThreadedFTPServer(("0.0.0.0", int(cfg["port"])), _Handler)
    # Share our exit event with the server and all handler threads so that
    # close_all() also terminates any in-flight per-connection loops.
    server._exit   = _ftp_exit
    _Handler._exit = _ftp_exit

    _orig_close_all = server.close_all
    def _close_all_patched():
        _ftp_exit.set()          # signal all per-connection threads to stop
        with _retr_lock:
            _retr_sessions.clear()
        _orig_close_all()
        _ftp_exit.clear()        # reset so server can be restarted
    server.close_all = _close_all_patched

    return server


# ─────────────────────────────────────────────────────────────────────────────
# TFTP server factory
# ─────────────────────────────────────────────────────────────────────────────
def _make_tftp_server(cfg: dict, log_fn=None, progress_fn=None):
    """Return a configured tftpy TftpServer instance."""
    try:
        import tftpy  # type: ignore
    except ImportError:
        raise RuntimeError(
            "tftpy is required for TFTP mode.\n"
            "Install with:  pip install tftpy"
        )

    if log_fn:
        class _TftpLogHandler(logging.Handler):
            def emit(self, record):
                msg = record.getMessage()
                # Cisco sends errorcode=0 'Session terminated' to end each
                # block segment — this is normal, not a real error.
                if 'Session terminated' in msg or ('ERR packet' in msg and 'errorcode: 0' in msg):
                    return
                prefix = "[!]" if record.levelno >= logging.WARNING else "[~]"
                log_fn(f"{prefix} TFTP: {msg}")

        hdlr = _TftpLogHandler()
        hdlr.setLevel(logging.DEBUG)
        lg = logging.getLogger("tftpy")
        lg.handlers = [hdlr]
        lg.setLevel(logging.INFO)
    else:
        logging.getLogger("tftpy").setLevel(logging.CRITICAL)

    server = tftpy.TftpServer(cfg["root_folder"])

    if progress_fn:
        # tftpy has no callback API, so we poll server.sessions every 250 ms.
        # Cisco TFTP transfers each file in multiple short sessions (one per
        # block), so we accumulate bytes across sessions keyed by (ip, name)
        # and only emit done=True when the whole file is received or 5 s idle.
        _stop_poll = threading.Event()
        _IDLE_DONE = 5.0
        # (ip, name) -> {'xfer_id', 'start_t', 'size', 'bytes_done', 'idle_since'}
        _seen: dict = {}

        def _poll():
            while not _stop_poll.is_set():
                try:
                    sessions = dict(server.sessions)
                except Exception:
                    sessions = {}

                now = time.monotonic()
                active_uids = set()

                for key, ctx in sessions.items():
                    try:
                        name   = os.path.basename(getattr(ctx, 'file_to_transfer', '') or str(key))
                        ip     = getattr(ctx, 'host', str(key))
                        done_b = ctx.metrics.bytes
                        uid    = (ip, name)
                        active_uids.add(uid)

                        total = _seen[uid]['size'] if uid in _seen and _seen[uid].get('size') else None
                        if not total:
                            try:
                                fp = os.path.join(cfg['root_folder'], getattr(ctx, 'file_to_transfer', ''))
                                if os.path.isfile(fp):
                                    total = os.path.getsize(fp)
                            except Exception:
                                pass

                        if uid not in _seen:
                            _seen[uid] = {
                                'xfer_id':    id(ctx),
                                'start_t':    now,
                                'size':       total,
                                'bytes_done': 0,
                                'idle_since': None,
                            }
                            if log_fn:
                                size_str = f"  ({_fmt_bytes(total)})" if total else ""
                                log_fn(f"[↓] {ip}  TFTP download start  {name}{size_str}")
                        else:
                            entry = _seen[uid]
                            if done_b > entry['bytes_done']:
                                entry['bytes_done'] = done_b
                            if total and not entry['size']:
                                entry['size'] = total
                            entry['idle_since'] = None

                        entry = _seen[uid]
                        elapsed = max(now - entry['start_t'], 1e-6)
                        progress_fn({
                            'id':          entry['xfer_id'],
                            'ip':          ip,
                            'name':        name,
                            'direction':   'down',
                            'bytes_done':  entry['bytes_done'],
                            'total_bytes': entry['size'],
                            'speed_bps':   entry['bytes_done'] / elapsed,
                            'done':        False,
                        })
                    except Exception:
                        pass

                # Handle idle entries (sessions that ended)
                for uid, entry in list(_seen.items()):
                    if uid not in active_uids:
                        if entry['idle_since'] is None:
                            entry['idle_since'] = now
                        size   = entry.get('size')
                        done_b = entry['bytes_done']
                        complete = (
                            (size and done_b >= size) or
                            (now - entry['idle_since'] >= _IDLE_DONE)
                        )
                        if complete:
                            elapsed = max(now - entry['start_t'], 1e-6)
                            progress_fn({
                                'id':          entry['xfer_id'],
                                'ip':          uid[0],
                                'name':        uid[1],
                                'direction':   'down',
                                'bytes_done':  done_b,
                                'total_bytes': size,
                                'speed_bps':   done_b / elapsed if done_b else 0,
                                'done':        True,
                            })
                            if log_fn:
                                spd = _fmt_bytes(int(done_b / elapsed)) if done_b else '?'
                                log_fn(f"[✔] {uid[0]}  TFTP download done   {uid[1]}"
                                       f"  ({_fmt_bytes(done_b)})  @ {spd}/s")
                            del _seen[uid]

                _stop_poll.wait(0.25)

        t = threading.Thread(target=_poll, daemon=True)
        t.start()

        _orig_stop = server.stop
        def _stop_patched(now=False):
            _stop_poll.set()
            _orig_stop(now)
        server.stop = _stop_patched

    return server


# ─────────────────────────────────────────────────────────────────────────────
# SCP server factory  (SSH transport via paramiko)
# SCP always requires password authentication — there is no anonymous mode.
# ─────────────────────────────────────────────────────────────────────────────
def _make_scp_server(cfg: dict, log_fn=None, progress_fn=None):
    """SSH/SCP server using paramiko."""
    try:
        import paramiko  # type: ignore
    except ImportError:
        raise RuntimeError(
            "paramiko is required for SCP mode.\n"
            "Install with:  pip install paramiko"
        )

    root     = cfg["root_folder"]
    username = cfg.get("username", "")
    password = cfg.get("password", "")

    def _log(msg: str) -> None:
        if log_fn:
            log_fn(msg)

    logging.getLogger("paramiko").setLevel(logging.CRITICAL)

    # ── Host key (generated once, stored persistently) ────────────────────────
    key_path = _CONFIG_DIR / "scp_host_key"
    if key_path.exists():
        try:
            host_key = paramiko.RSAKey.from_private_key_file(str(key_path))
        except Exception:
            host_key = paramiko.RSAKey.generate(2048)
            host_key.write_private_key_file(str(key_path))
    else:
        host_key = paramiko.RSAKey.generate(2048)
        _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        host_key.write_private_key_file(str(key_path))

    # ── SSH server interface ───────────────────────────────────────────────────
    class _SSHServer(paramiko.ServerInterface):
        def __init__(self):
            self.exec_event   = threading.Event()
            self.exec_command = b""

        def check_channel_request(self, kind, chanid):
            if kind == "session":
                return paramiko.OPEN_SUCCEEDED
            return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

        def check_auth_password(self, uname, passwd):
            if uname == username and passwd == password:
                return paramiko.AUTH_SUCCESSFUL
            _log(f"[✗] {uname!r}  SCP login FAILED")
            return paramiko.AUTH_FAILED

        def get_allowed_auths(self, uname):
            return "password"

        def check_channel_exec_request(self, channel, command):
            self.exec_command = command
            self.exec_event.set()
            return True

    # ── SCP wire-protocol helpers ─────────────────────────────────────────────
    def _recv_line(chan):
        """Read bytes until newline; returns bytes or None on closed channel."""
        buf = b""
        while True:
            b = chan.recv(1)
            if not b:
                return None
            if b == b"\n":
                return buf
            buf += b

    def _sink_file(chan, header, dest_dir, ip):
        """Receive one file (C-header already read) into dest_dir."""
        parts = header[1:].split(b" ", 2)
        if len(parts) < 3:
            chan.sendall(b"\x01scp: bad C header\n")
            return
        try:
            size = int(parts[1])
        except ValueError:
            chan.sendall(b"\x01scp: bad size\n")
            return
        filename = parts[2].decode(errors="replace").strip()
        # Reject traversal attempts inside the filename itself
        if "/" in filename or filename in (".", ".."):
            chan.sendall(b"\x01scp: invalid filename\n")
            return
        file_path = os.path.join(dest_dir, filename)

        _log(f"[↑] {ip}  upload start  {filename}  ({_fmt_bytes(size)})")
        xfer_id = id(chan) ^ hash(filename)
        start_t = time.monotonic()
        last_prog = [0.0]

        chan.sendall(b"\x00")   # ready to receive data
        received = 0
        try:
            with open(file_path, "wb") as f:
                while received < size:
                    chunk = chan.recv(min(1 << 20, size - received))
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
                    now = time.monotonic()
                    if progress_fn and (now - last_prog[0]) >= 0.4:
                        last_prog[0] = now
                        elapsed = max(now - start_t, 1e-6)
                        progress_fn({
                            'id': xfer_id, 'ip': ip, 'name': filename,
                            'direction': 'up', 'bytes_done': received,
                            'total_bytes': size,
                            'speed_bps': received / elapsed, 'done': False,
                        })
        except OSError as exc:
            chan.sendall(f"\x01scp: {exc}\n".encode())
            if progress_fn:
                progress_fn({'id': xfer_id, 'ip': ip, 'name': filename,
                             'direction': 'up', 'bytes_done': received,
                             'total_bytes': size, 'speed_bps': 0, 'done': True})
            return

        elapsed = max(time.monotonic() - start_t, 1e-6)
        speed   = received / elapsed
        if progress_fn:
            progress_fn({'id': xfer_id, 'ip': ip, 'name': filename,
                         'direction': 'up', 'bytes_done': received,
                         'total_bytes': size, 'speed_bps': speed, 'done': True})
        trailer = chan.recv(1)
        if trailer == b"\x00":
            chan.sendall(b"\x00")
            _log(
                f"[✔] {ip}  upload done     {filename}"
                f"  ({_fmt_bytes(received)})  @ {_fmt_bytes(int(speed))}/s"
            )
        else:
            try:
                os.remove(file_path)
            except OSError:
                pass
            _log(f"[!] {ip}  upload INTERRUPTED  {filename}")

    def _sink_loop(chan, dest_path, ip):
        """Process all SCP messages for a sink (-t) session."""
        chan.sendall(b"\x00")   # initial ready
        stack = [dest_path]
        while stack:
            line = _recv_line(chan)
            if line is None:
                break
            if not line:
                continue
            first = line[0:1]
            if first == b"\x01":    # warning
                _log(f"[!] {ip}  SCP: {line[1:].decode(errors='replace')}")
                chan.sendall(b"\x00")
                continue
            if first == b"\x02":    # fatal error from client
                break
            if first == b"E":       # end of directory
                chan.sendall(b"\x00")
                stack.pop()
            elif first == b"D":     # enter directory
                parts = line[1:].split(b" ", 2)
                dname = (parts[2].decode(errors="replace").strip()
                         if len(parts) >= 3 else "_dir")
                new_dir = os.path.join(stack[-1], dname)
                os.makedirs(new_dir, exist_ok=True)
                chan.sendall(b"\x00")
                stack.append(new_dir)
            elif first == b"C":     # file
                _sink_file(chan, line, stack[-1], ip)

    def _source_file(chan, file_path, ip):
        """Send one file to the client."""
        filename = os.path.basename(file_path)
        try:
            size = os.path.getsize(file_path)
        except OSError as exc:
            chan.sendall(f"\x01scp: {exc}\n".encode())
            return

        _log(f"[↓] {ip}  download start  {filename}  ({_fmt_bytes(size)})")
        xfer_id = id(chan) ^ hash(filename)
        start_t = time.monotonic()
        last_prog = [0.0]

        chan.sendall(f"C0644 {size} {filename}\n".encode())
        ack = chan.recv(1)
        if not ack or ack != b"\x00":
            return

        # Use 32 KiB chunks — well within any Cisco SSH window size, so each
        # send completes quickly and doesn't hold the window-wait lock long.
        CHUNK = 32768
        sent = 0
        interrupted = False
        with open(file_path, "rb") as f:
            while sent < size:
                data = f.read(CHUNK)
                if not data:
                    break
                buf = memoryview(data)
                pos = 0
                while pos < len(buf):
                    if chan.closed:
                        interrupted = True
                        break
                    try:
                        n = chan.send(bytes(buf[pos:]))
                    except OSError:
                        interrupted = True
                        break
                    if n <= 0:
                        interrupted = True
                        break
                    pos  += n
                    sent += n
                if interrupted:
                    break
                now = time.monotonic()
                if progress_fn and (now - last_prog[0]) >= 0.4:
                    last_prog[0] = now
                    elapsed = max(now - start_t, 1e-6)
                    progress_fn({
                        'id': xfer_id, 'ip': ip, 'name': filename,
                        'direction': 'down', 'bytes_done': sent,
                        'total_bytes': size,
                        'speed_bps': sent / elapsed, 'done': False,
                    })

        elapsed = max(time.monotonic() - start_t, 1e-6)
        speed   = sent / elapsed
        if progress_fn:
            progress_fn({'id': xfer_id, 'ip': ip, 'name': filename,
                         'direction': 'down', 'bytes_done': sent,
                         'total_bytes': size, 'speed_bps': speed, 'done': True})

        if interrupted:
            _log(f"[!] {ip}  download INTERRUPTED  {filename}  ({_fmt_bytes(sent)} of {_fmt_bytes(size)})")
            return

        try:
            chan.sendall(b"\x00")
            chan.recv(1)
        except OSError:
            pass  # switch may close cleanly before we can exchange final acks

        _log(
            f"[✔] {ip}  download done   {filename}"
            f"  ({_fmt_bytes(sent)})  @ {_fmt_bytes(int(speed))}/s"
        )

    def _source_dir(chan, dir_path, ip):
        """Send a directory tree to the client (recursive)."""
        chan.sendall(f"D0755 0 {os.path.basename(dir_path)}\n".encode())
        if chan.recv(1) != b"\x00":
            return
        try:
            entries = sorted(os.listdir(dir_path))
        except OSError:
            chan.sendall(b"E\n")
            chan.recv(1)
            return
        for entry in entries:
            full = os.path.join(dir_path, entry)
            if os.path.isfile(full):
                _source_file(chan, full, ip)
            elif os.path.isdir(full):
                _source_dir(chan, full, ip)
        chan.sendall(b"E\n")
        chan.recv(1)

    def _handle_scp(chan, cmd, ip):
        import shlex
        try:
            parts = shlex.split(cmd)
        except Exception:
            parts = cmd.split()

        if not parts or parts[0] != "scp":
            chan.sendall(b"\x01scp: unrecognised command\n")
            return

        sink      = "-t" in parts
        source    = "-f" in parts
        recursive = "-r" in parts
        raw_path  = parts[-1]

        # Resolve to an absolute path and verify it stays within root
        root_real = os.path.realpath(root)
        full      = os.path.realpath(os.path.join(root, raw_path.lstrip("/")))
        if full != root_real and not full.startswith(root_real + os.sep):
            chan.sendall(b"\x01scp: permission denied\n")
            return

        if sink:
            dest = full if os.path.isdir(full) else os.path.dirname(full)
            os.makedirs(dest, exist_ok=True)
            _sink_loop(chan, dest, ip)
        elif source:
            if chan.recv(1) != b"\x00":
                return
            if os.path.isfile(full):
                _source_file(chan, full, ip)
            elif os.path.isdir(full) and recursive:
                _source_dir(chan, full, ip)
            else:
                chan.sendall(f"\x01scp: {raw_path}: no such file\n".encode())
        else:
            chan.sendall(b"\x01scp: missing -t or -f flag\n")

    # ── Listening socket + server object ──────────────────────────────────────
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    srv_sock.bind(("0.0.0.0", int(cfg["port"])))
    srv_sock.listen(20)
    srv_sock.settimeout(1.0)
    stop_ev = threading.Event()

    # Track active transports so close_all() can abort in-flight transfers.
    _active_transports: list = []
    _transports_lock = threading.Lock()

    def _handle_conn(client_sock, addr):
        ip = addr[0]
        _log(f"[+] {ip}  connected (SCP/SSH)")
        transport = paramiko.Transport(client_sock)

        # ── Throughput tuning ─────────────────────────────────────────
        # Prefer AES-CTR: hardware-accelerated via OpenSSL (AES-NI).
        # Do NOT override window_size or max_packet_size — Cisco IOS has
        # strict limits and deviating from paramiko's defaults breaks transfers.
        transport._preferred_ciphers = (
            "aes128-ctr", "aes192-ctr", "aes256-ctr",
            "aes128-cbc", "aes256-cbc",
        )
        try:
            transport.packetizer.REKEY_BYTES   = 2 ** 33
            transport.packetizer.REKEY_PACKETS = 2 ** 33
        except AttributeError:
            pass
        # Keepalive every 15 s so the Cisco SSH client doesn't idle-close
        # the channel while waiting for window credit.
        transport.set_keepalive(15)
        # ─────────────────────────────────────────────────────────────

        with _transports_lock:
            _active_transports.append(transport)
        try:
            transport.add_server_key(host_key)
            iface = _SSHServer()
            try:
                transport.start_server(server=iface)
            except Exception as exc:
                _log(f"[!] {ip}  SSH negotiation failed: {exc}")
                return
            chan = transport.accept(20)
            if chan is None:
                _log(f"[!] {ip}  no channel opened (auth failed?)")
                return
            if not iface.exec_event.wait(10):
                _log(f"[!] {ip}  no exec command received")
                chan.close()
                return
            cmd = iface.exec_command.decode(errors="replace").strip()
            try:
                _handle_scp(chan, cmd, ip)
            except Exception as exc:
                if not stop_ev.is_set():
                    _log(f"[!] {ip}  SCP error: {exc}")
            finally:
                try:
                    chan.send_exit_status(0)
                except Exception:
                    pass
                chan.close()
        finally:
            transport.close()
            with _transports_lock:
                try:
                    _active_transports.remove(transport)
                except ValueError:
                    pass
        _log(f"[-] {ip}  disconnected (SCP/SSH)")

    class _SCPServer:
        def serve_forever(self):
            while not stop_ev.is_set():
                try:
                    client_sock, addr = srv_sock.accept()
                    threading.Thread(
                        target=_handle_conn,
                        args=(client_sock, addr),
                        daemon=True,
                    ).start()
                except socket.timeout:
                    continue
                except Exception:
                    if not stop_ev.is_set():
                        _log("[!] SCP accept error")
                    break

        def close_all(self):
            stop_ev.set()
            try:
                srv_sock.close()
            except Exception:
                pass
            # Close all active SSH transports — this aborts any in-flight
            # file transfers immediately rather than waiting for them to finish.
            with _transports_lock:
                for t in list(_active_transports):
                    try:
                        t.close()
                    except Exception:
                        pass

    return _SCPServer()


def _is_admin() -> bool:
    """Return True if running with root/administrator privileges."""
    if sys.platform == "win32":
        try:
            import ctypes
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False
    return os.geteuid() == 0


def _get_all_ipv4s() -> list:
    """Return all local IPv4 addresses (excludes loopback). Cross-platform."""
    try:
        hostname = socket.gethostname()
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET)
        ips = list(dict.fromkeys(
            a[4][0] for a in infos if not a[4][0].startswith("127.")
        ))
        if ips:
            return ips
    except Exception:
        pass
    # Fallback: UDP-trick (works even when hostname doesn't resolve nicely)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return [ip]
    except Exception:
        return ["127.0.0.1"]


# ─────────────────────────────────────────────────────────────────────────────
# Daemon mode  (Linux only — launched via pkexec for privileged ports < 1024)
# ─────────────────────────────────────────────────────────────────────────────
def _run_daemon() -> None:
    if sys.platform == "win32":
        sys.exit("Daemon mode is not supported on Windows.")
    import signal as _sig

    cfg       = _load_config(_ARGS.config)
    sock_path = _ARGS.socket
    protocol  = cfg.get("protocol", "ftp")

    def _log(msg: str) -> None:
        print(msg, flush=True)

    def _progress(evt: dict) -> None:
        """Stream progress events to the GUI via stdout."""
        try:
            import json as _json
            print(f"\x00PROGRESS:{_json.dumps(evt)}", flush=True)
        except Exception:
            pass

    try:
        if protocol == "scp":
            server = _make_scp_server(cfg, log_fn=_log, progress_fn=_progress)
        elif protocol == "tftp":
            server = _make_tftp_server(cfg, log_fn=_log, progress_fn=_progress)
        else:
            server = _make_server(cfg, log_fn=_log, progress_fn=_progress)
    except Exception as exc:
        _log(f"ERROR: {exc}")
        sys.exit(1)

    stop_ev = threading.Event()
    _sig.signal(_sig.SIGTERM, lambda *_: stop_ev.set())

    # World-writable Unix socket so the normal-user GUI can send "STOP"
    ctrl = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    ctrl.bind(sock_path)
    os.chmod(sock_path, 0o666)
    ctrl.listen(1)
    ctrl.settimeout(0.5)

    if protocol == "tftp":
        def _tftp_serve():
            try:
                server.listen("0.0.0.0", int(cfg["port"]))
            except Exception:
                pass
        threading.Thread(target=_tftp_serve, daemon=True).start()
    else:
        threading.Thread(target=server.serve_forever, daemon=True).start()

    _log(f"Daemon listening on port {cfg['port']} ({protocol.upper()})")

    while not stop_ev.is_set():
        try:
            conn, _ = ctrl.accept()
            msg = conn.recv(16).decode().strip()
            conn.close()
            if msg == "STOP":
                stop_ev.set()
        except socket.timeout:
            continue
        except Exception:
            break

    if protocol == "tftp":
        server.stop()
    else:
        server.close_all()
    ctrl.close()
    try:
        os.unlink(sock_path)
    except OSError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# GUI
# ─────────────────────────────────────────────────────────────────────────────
def _run_gui() -> None:

    # ── Imports ───────────────────────────────────────────────────────────────
    try:
        from PyQt5.QtWidgets import (           # type: ignore
            QApplication, QMainWindow, QWidget,
            QVBoxLayout, QHBoxLayout, QFormLayout,
            QLabel, QLineEdit, QPushButton, QFileDialog,
            QRadioButton, QButtonGroup, QGroupBox,
            QTextEdit, QSpinBox, QSystemTrayIcon,
            QMenu, QAction, QMessageBox, QFrame, QScrollArea,
        )
        from PyQt5.QtCore import Qt, pyqtSignal, QObject, QTimer  # type: ignore
        from PyQt5.QtGui import (               # type: ignore
            QIcon, QPixmap, QPainter, QColor, QFont, QCursor, QTextCursor,
        )

    except ImportError:
        sys.exit("PyQt5 is required.  Install with:  pip install PyQt5")

    try:
        import pyftpdlib  # noqa: F401  # type: ignore
    except ImportError:
        sys.exit("pyftpdlib is required.  Install with:  pip install pyftpdlib")

    import html as _html  # for escaping log messages

    # ── Global stylesheet ─────────────────────────────────────────────────────
    _APP_STYLE = """
    QWidget {
        background-color: #16172a;
        color: #dde1ff;
        font-size: 13px;
    }
    QGroupBox {
        border: 1px solid #272848;
        border-radius: 8px;
        margin-top: 14px;
        padding: 10px 8px 8px 8px;
        font-weight: 600;
        font-size: 12px;
        color: #9d8fff;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 10px;
        padding: 0 6px;
    }
    QLineEdit, QSpinBox {
        background-color: #0f101d;
        border: 1px solid #272848;
        border-radius: 5px;
        padding: 5px 8px;
        color: #dde1ff;
    }
    QLineEdit:focus, QSpinBox:focus {
        border-color: #7c6af7;
    }
    QLineEdit:disabled, QSpinBox:disabled {
        color: #454565;
        background-color: #131425;
    }
    QTextEdit {
        background-color: #0f101d;
        border: 1px solid #272848;
        border-radius: 5px;
        color: #dde1ff;
        selection-background-color: #4a4b80;
    }
    QPushButton {
        background-color: #222340;
        color: #c8ccff;
        border: 1px solid #363760;
        border-radius: 5px;
        padding: 5px 14px;
    }
    QPushButton:hover {
        background-color: #2e3060;
        border-color: #7c6af7;
        color: #ffffff;
    }
    QPushButton:pressed {
        background-color: #4a4b80;
    }
    QPushButton:disabled {
        color: #363660;
        background-color: #14152a;
        border-color: #1e1f38;
    }
    QRadioButton {
        spacing: 8px;
        padding: 3px 0;
    }
    QRadioButton::indicator {
        width: 14px; height: 14px;
        border-radius: 7px;
        border: 2px solid #454580;
        background-color: #0f101d;
    }
    QRadioButton::indicator:checked {
        background-color: #7c6af7;
        border-color: #7c6af7;
    }
    QRadioButton:disabled { color: #454565; }
    QScrollBar:vertical {
        background: #0f101d;
        width: 7px;
        border-radius: 3px;
    }
    QScrollBar::handle:vertical {
        background: #363760;
        border-radius: 3px;
        min-height: 20px;
    }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
    QScrollBar:horizontal { height: 0; }
    QScrollArea { border: none; }
    QMenu {
        background-color: #1e1f35;
        border: 1px solid #363760;
        border-radius: 6px;
        padding: 4px;
    }
    QMenu::item { padding: 6px 22px; border-radius: 4px; }
    QMenu::item:selected { background-color: #2e3060; }
    QMenu::separator { height: 1px; background: #272848; margin: 3px 8px; }
    QFrame#appHeader {
        background-color: #10111f;
        border-bottom: 1px solid #272848;
    }
    QFrame#hSep { color: #272848; }
    QFrame#controlFrame {
        background-color: #111224;
        border: 1px solid #272848;
        border-radius: 8px;
    }
    QLabel#appTitle { font-size: 15px; font-weight: bold; color: #dde1ff; }
    QLabel#noteLabel { color: #60618a; font-size: 11px; }
    QTextEdit#logView  { background-color: #0a0b18; border-color: #1e1f38; }
    QTextEdit#connInfo { background-color: #0a0b18; border: none; }
    """

    _BTN_START = (
        "QPushButton {"
        "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1,"
        "    stop:0 #34c97e, stop:1 #27ae60);"
        "  color: white; font-weight: bold; font-size: 13px;"
        "  padding: 9px 28px; border: none; border-radius: 6px;"
        "}"
        "QPushButton:hover { background: #2ecc71; }"
        "QPushButton:pressed { background: #1e8449; }"
        "QPushButton:disabled { background: #1a4d35; color: #6a8a6a; }"
    )
    _BTN_STOP = (
        "QPushButton {"
        "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1,"
        "    stop:0 #e95b4c, stop:1 #c0392b);"
        "  color: white; font-weight: bold; font-size: 13px;"
        "  padding: 9px 28px; border: none; border-radius: 6px;"
        "}"
        "QPushButton:hover { background: #e74c3c; }"
        "QPushButton:pressed { background: #922b21; }"
        "QPushButton:disabled { background: #4a1a1a; color: #8a7a7a; }"
    )

    # ── App icon ──────────────────────────────────────────────────────────────
    _ICON_PATH = _resource_path("assets/old-icon.png")

    def _make_icon(running: bool) -> QIcon:
        size = 64
        px = QPixmap(size, size)
        px.fill(Qt.transparent)
        p = QPainter(px)
        p.setRenderHint(QPainter.Antialiasing)
        src = QPixmap(_ICON_PATH)
        if not src.isNull():
            scaled = src.scaled(size, size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            x = (size - scaled.width()) // 2
            y = (size - scaled.height()) // 2
            p.drawPixmap(x, y, scaled)
        else:
            # Fallback: plain circle
            p.setBrush(QColor("#2980b9"))
            p.setPen(Qt.NoPen)
            p.drawEllipse(4, 4, size - 8, size - 8)
        # Running state: prominent green badge with white outline in bottom-right
        if running:
            badge = 22
            bx = size - badge - 2
            by = size - badge - 2
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(255, 255, 255, 230))   # white ring for contrast
            p.drawEllipse(bx - 2, by - 2, badge + 4, badge + 4)
            p.setBrush(QColor(39, 174, 96))           # solid green
            p.drawEllipse(bx, by, badge, badge)
        p.end()
        return QIcon(px)

    # ── Worker: manages the FTP server lifecycle ─────────────────────────────
    class Worker(QObject):
        """
        Lives in the main thread; runs FTP logic in daemon threads.
        Log messages are passed via a thread-safe queue, drained by a QTimer.
        """
        log     = pyqtSignal(str)
        started = pyqtSignal()
        stopped = pyqtSignal()
        error   = pyqtSignal(str)

        def __init__(self) -> None:
            super().__init__()
            self._server: object   = None     # pyftpdlib FTPServer or tftpy TftpServer (local)
            self._proc: object     = None     # Popen (privileged mode)
            self._sock_path: str   = ""       # Unix socket path (privileged mode)
            self._protocol: str    = "ftp"
            self._log_q: queue.SimpleQueue = queue.SimpleQueue()
            self._progress_q: queue.SimpleQueue = queue.SimpleQueue()

        # ── Public API ────────────────────────────────────────────────
        def start(self, cfg: dict) -> None:
            self._protocol = cfg.get("protocol", "ftp")
            needs_priv = int(cfg["port"]) < 1024 and not _is_admin()
            if needs_priv and sys.platform == "win32":
                self.error.emit(
                    f"Port {cfg['port']} may require administrator privileges on Windows.\n\n"
                    "Right-click FlashDash and choose 'Run as administrator',\n"
                    "or use a port \u2265 1024 (e.g. 2121)."
                )
            elif needs_priv:
                self._start_privileged(cfg)
            else:
                self._start_local(cfg)

        def stop(self) -> None:
            if self._sock_path:
                self._stop_privileged()
            elif self._server:
                srv, self._server = self._server, None
                if self._protocol == "tftp":
                    threading.Thread(target=srv.stop, daemon=True).start()
                else:
                    threading.Thread(target=srv.close_all, daemon=True).start()

        # ── Local (non-root) server ───────────────────────────────────
        def _start_local(self, cfg: dict) -> None:
            protocol = cfg.get("protocol", "ftp")
            try:
                if protocol == "scp":
                    srv = _make_scp_server(cfg, log_fn=self._log_q.put,
                                           progress_fn=self._progress_q.put)
                elif protocol == "tftp":
                    srv = _make_tftp_server(cfg, log_fn=self._log_q.put,
                                            progress_fn=self._progress_q.put)
                else:
                    srv = _make_server(cfg, log_fn=self._log_q.put,
                                       progress_fn=self._progress_q.put)
            except Exception as exc:
                self.error.emit(str(exc))
                return

            self._server = srv

            def _serve() -> None:
                try:
                    if protocol == "tftp":
                        srv.listen("0.0.0.0", int(cfg["port"]))
                    else:
                        srv.serve_forever()
                except Exception:
                    pass
                if self._server is srv:
                    self._server = None
                self.stopped.emit()

            threading.Thread(target=_serve, daemon=True).start()
            self.started.emit()

        # ── Privileged server via pkexec ──────────────────────────────
        def _start_privileged(self, cfg: dict) -> None:
            import tempfile

            # Write config to a temp file that the daemon will read
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            )
            json.dump(cfg, tmp)
            tmp.close()

            sp = f"/tmp/.ftp-srv-{uuid.uuid4().hex}.sock"
            self._sock_path = sp

            cmd = [
                "pkexec",
                sys.executable,
                os.path.abspath(__file__),
                "--daemon",
                "--config", tmp.name,
                "--socket", sp,
            ]

            try:
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
            except FileNotFoundError:
                self._sock_path = ""
                self.error.emit(
                    "pkexec not found.\n\n"
                    "To bind ports below 1024, either:\n"
                    "  • Run this application as root (sudo)\n"
                    "  • Use a port ≥ 1024 (e.g. 2121)"
                )
                return
            except Exception as exc:
                self._sock_path = ""
                self.error.emit(str(exc))
                return

            # Wait up to 5 s for the daemon's control socket to appear
            import time
            for _ in range(50):
                if os.path.exists(sp):
                    break
                if self._proc.poll() is not None:
                    out = self._proc.stdout.read().decode(errors="replace")
                    self._sock_path = ""
                    self.error.emit(
                        f"The elevated daemon exited early:\n\n{out}"
                    )
                    return
                time.sleep(0.1)
            else:
                self._sock_path = ""
                self.error.emit("Elevated daemon failed to start (timeout).")
                return

            self.log.emit(
                f"Elevated FTP daemon started  (pid {self._proc.pid})"
            )
            self.started.emit()

            # Read daemon stdout — log lines go to log_q,
            # PROGRESS: JSON lines go to progress_q.
            proc_ref   = self._proc
            log_q      = self._log_q
            progress_q = self._progress_q

            def _read_stdout() -> None:
                try:
                    import json as _json
                    for raw in proc_ref.stdout:
                        line = raw.decode(errors="replace").rstrip()
                        if not line:
                            continue
                        if line.startswith("\x00PROGRESS:"):
                            try:
                                evt = _json.loads(line[len("\x00PROGRESS:"):])
                                progress_q.put(evt)
                            except Exception:
                                pass
                        else:
                            log_q.put(line)
                except Exception:
                    pass

            threading.Thread(target=_read_stdout, daemon=True).start()

            # Background thread: detect if daemon dies unexpectedly
            def _watch() -> None:
                self._proc.wait()
                if self._sock_path == sp:   # not stopped via our command
                    self._sock_path = ""
                    self._proc = None
                    self.stopped.emit()

            threading.Thread(target=_watch, daemon=True).start()

        # ── Stop privileged daemon ────────────────────────────────────
        def _stop_privileged(self) -> None:
            sp, self._sock_path = self._sock_path, ""
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect(sp)
                s.sendall(b"STOP")
                s.close()
            except Exception:
                if self._proc:
                    self._proc.terminate()
            self._proc = None
            self.stopped.emit()

    # ── Main window ──────────────────────────────────────────────────────────
    class MainWindow(QMainWindow):
        def __init__(self) -> None:
            super().__init__()
            self._running = False
            self._active_xfers: dict = {}  # xfer_id -> QLabel
            self._cfg     = _load_config()
            self._worker  = Worker()
            self._worker.log    .connect(self._on_log)
            self._worker.started.connect(self._on_started)
            self._worker.stopped.connect(self._on_stopped)
            self._worker.error  .connect(self._on_error)
            self._build_ui()
            self._build_tray()
            self._cfg_to_ui()

            self._log_timer = QTimer(self)
            self._log_timer.timeout.connect(self._drain_log_queue)
            self._log_timer.start(100)

        # ── UI construction ───────────────────────────────────────────
        def _build_ui(self) -> None:
            self.setWindowTitle("FlashDash FTP/TFTP/SCP Server")
            self.setMinimumWidth(530)
            self.setMinimumHeight(420)
            self.resize(580, 920)
            self.setWindowIcon(_make_icon(False))

            central = QWidget()
            self.setCentralWidget(central)
            root_v = QVBoxLayout(central)
            root_v.setSpacing(0)
            root_v.setContentsMargins(0, 0, 0, 0)

            # ── Header ────────────────────────────────────────────────
            header = QFrame()
            header.setObjectName("appHeader")
            hl = QHBoxLayout(header)
            hl.setContentsMargins(14, 10, 14, 10)
            hl.setSpacing(10)
            self._header_icon = QLabel()
            self._header_icon.setPixmap(_make_icon(False).pixmap(30, 30))
            title_col = QVBoxLayout()
            title_col.setSpacing(1)
            title_lbl = QLabel("FlashDash FTP / TFTP / SCP Server")
            title_lbl.setObjectName("appTitle")
            self._header_sub = QLabel("●  Stopped")
            self._header_sub.setStyleSheet("color:#e74c3c; font-size:11px;")
            title_col.addWidget(title_lbl)
            title_col.addWidget(self._header_sub)
            hl.addWidget(self._header_icon)
            hl.addLayout(title_col)
            hl.addStretch()
            root_v.addWidget(header)

            sep = QFrame()
            sep.setFrameShape(QFrame.HLine)
            sep.setObjectName("hSep")
            root_v.addWidget(sep)

            # ── Scrollable body ───────────────────────────────────────
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.NoFrame)
            body = QWidget()
            vbox = QVBoxLayout(body)
            vbox.setSpacing(10)
            vbox.setContentsMargins(14, 14, 14, 14)
            scroll.setWidget(body)
            root_v.addWidget(scroll, 1)

            # ── Protocol ──────────────────────────────────────────────
            gp = QGroupBox("  🔌  Protocol")
            vp = QHBoxLayout(gp)
            vp.setContentsMargins(8, 16, 8, 8)
            vp.setSpacing(24)
            self._w_proto_ftp  = QRadioButton("FTP")
            self._w_proto_tftp = QRadioButton("TFTP")
            self._w_proto_scp  = QRadioButton("SCP")
            bg_proto = QButtonGroup(gp)
            bg_proto.addButton(self._w_proto_ftp,  0)
            bg_proto.addButton(self._w_proto_tftp, 1)
            bg_proto.addButton(self._w_proto_scp,  2)
            self._w_proto_ftp.setChecked(True)
            vp.addWidget(self._w_proto_ftp)
            vp.addWidget(self._w_proto_tftp)
            vp.addWidget(self._w_proto_scp)
            vp.addStretch()
            for _btn in (self._w_proto_ftp, self._w_proto_tftp, self._w_proto_scp):
                _btn.toggled.connect(lambda _: self._on_proto_changed())
            vbox.addWidget(gp)

            # ── Root folder ───────────────────────────────────────────
            gf = QGroupBox("  📁  Root Folder")
            hf = QHBoxLayout(gf)
            hf.setContentsMargins(8, 16, 8, 8)
            self._w_folder = QLineEdit(
                placeholderText="Select the folder to share…"
            )
            btn_browse = QPushButton("Browse…")
            btn_browse.setFixedWidth(82)
            btn_browse.clicked.connect(self._browse)
            hf.addWidget(self._w_folder)
            hf.addWidget(btn_browse)
            vbox.addWidget(gf)

            # ── Network ───────────────────────────────────────────────
            gn = QGroupBox("  🌐  Network")
            vn = QVBoxLayout(gn)
            vn.setContentsMargins(8, 16, 8, 8)
            vn.setSpacing(8)
            port_row = QHBoxLayout()
            port_row.setSpacing(6)
            port_row.addWidget(QLabel("Port:"))
            self._w_port = QSpinBox()
            self._w_port.setRange(1, 65535)
            self._w_port.setValue(21)
            self._w_port.setFixedWidth(72)
            port_row.addWidget(self._w_port)
            port_row.addSpacing(4)
            for _pv, _pl in [(21, "21  FTP"), (2121, "2121"), (990, "990  FTPS"), (2222, "2222  SCP")]:
                _pb = QPushButton(_pl)
                _pb.setFixedHeight(26)
                _pb.setStyleSheet(
                    "QPushButton{font-size:11px;padding:1px 8px;"
                    "color:#9d8fff;border-color:#363760;}"
                    "QPushButton:hover{color:#fff;border-color:#7c6af7;}"
                )
                _pb.clicked.connect(lambda _, v=_pv: self._w_port.setValue(v))
                port_row.addWidget(_pb)
            port_row.addStretch()
            vn.addLayout(port_row)
            self._w_port_note = QLabel(
                "Passive ports 60000–60100 must be open in your firewall "
                "for passive-mode FTP clients."
            )
            self._w_port_note.setObjectName("noteLabel")
            self._w_port_note.setWordWrap(True)
            vn.addWidget(self._w_port_note)
            vbox.addWidget(gn)

            # ── Authentication ────────────────────────────────────────
            ga = QGroupBox("  🔒  Authentication")
            self._w_auth_box = ga
            va = QVBoxLayout(ga)
            va.setContentsMargins(8, 16, 8, 8)
            va.setSpacing(6)
            self._w_anon = QRadioButton(
                "Anonymous  —  read-only, no password required"
            )
            self._w_user = QRadioButton(
                "Username / Password  —  full read/write access"
            )
            bg = QButtonGroup(ga)
            bg.addButton(self._w_anon, 0)
            bg.addButton(self._w_user, 1)
            va.addWidget(self._w_anon)
            va.addWidget(self._w_user)
            cframe = QFrame()
            cf = QFormLayout(cframe)
            cf.setContentsMargins(24, 4, 8, 4)
            cf.setSpacing(6)
            self._w_uname  = QLineEdit()
            self._w_passwd = QLineEdit()
            self._w_passwd.setEchoMode(QLineEdit.Password)
            cf.addRow("Username:", self._w_uname)
            cf.addRow("Password:", self._w_passwd)
            va.addWidget(cframe)
            self._w_creds = cframe
            self._w_anon.toggled.connect(
                lambda checked: cframe.setEnabled(not checked)
            )
            vbox.addWidget(ga)

            # ── Control bar ───────────────────────────────────────────
            ctrl = QFrame()
            ctrl.setObjectName("controlFrame")
            hctrl = QHBoxLayout(ctrl)
            hctrl.setContentsMargins(14, 10, 14, 10)
            self._w_status = QLabel("●  Stopped")
            self._w_status.setStyleSheet(
                "color:#e74c3c; font-weight:bold; font-size:14px;"
            )
            self._w_btn = QPushButton("▶  Start Server")
            self._w_btn.setStyleSheet(_BTN_START)
            self._w_btn.setMinimumWidth(148)
            self._w_btn.clicked.connect(self._toggle)
            hctrl.addWidget(self._w_status)
            hctrl.addStretch()
            hctrl.addWidget(self._w_btn)
            vbox.addWidget(ctrl)

            # ── Connection info ───────────────────────────────────────
            self._w_conn_box = QGroupBox("  💻  Connect from your FTP client")
            vc = QVBoxLayout(self._w_conn_box)
            vc.setContentsMargins(8, 16, 8, 8)
            vc.setSpacing(4)
            self._w_url_container = QWidget()
            self._w_url_layout = QVBoxLayout(self._w_url_container)
            self._w_url_layout.setContentsMargins(0, 0, 0, 0)
            self._w_url_layout.setSpacing(4)
            vc.addWidget(self._w_url_container)
            self._w_conn_box.setVisible(False)
            vbox.addWidget(self._w_conn_box)

            # ── Active Transfers ──────────────────────────────────────
            self._w_xfer_box = QGroupBox("  ⚡  Active Transfers")
            self._w_xfer_vbox = QVBoxLayout(self._w_xfer_box)
            self._w_xfer_vbox.setContentsMargins(8, 16, 8, 8)
            self._w_xfer_vbox.setSpacing(4)
            self._w_xfer_box.setVisible(False)
            vbox.addWidget(self._w_xfer_box)

            # ── Activity log ──────────────────────────────────────────
            gl = QGroupBox("  📋  Activity Log")
            vl = QVBoxLayout(gl)
            vl.setContentsMargins(8, 16, 8, 8)
            vl.setSpacing(6)
            log_hdr = QHBoxLayout()
            log_hdr.addStretch()
            btn_clear = QPushButton("Clear")
            btn_clear.setFixedWidth(52)
            btn_clear.setFixedHeight(22)
            btn_clear.setStyleSheet(
                "QPushButton{font-size:11px;padding:1px 6px;}"
            )
            log_hdr.addWidget(btn_clear)
            self._w_log = QTextEdit(readOnly=True)
            self._w_log.setFont(QFont("monospace", 9))
            self._w_log.setMinimumHeight(100)
            self._w_log.setObjectName("logView")
            btn_clear.clicked.connect(self._w_log.clear)
            vl.addLayout(log_hdr)
            vl.addWidget(self._w_log)
            vbox.addWidget(gl)

        def _build_tray(self) -> None:
            if not QSystemTrayIcon.isSystemTrayAvailable():
                self._tray = None
                return

            self._tray = QSystemTrayIcon(self)
            self._tray.setIcon(_make_icon(False))
            self._tray.setToolTip("FlashDash — Stopped")

            menu = QMenu()

            self._tray_act = QAction("Start Server", menu)
            self._tray_act.triggered.connect(self._toggle)
            menu.addAction(self._tray_act)

            menu.addSeparator()

            a_show = QAction("Show Window", menu)
            a_show.triggered.connect(self._show_window)
            menu.addAction(a_show)

            menu.addSeparator()

            a_quit = QAction("Close App", menu)
            a_quit.triggered.connect(self._quit)
            menu.addAction(a_quit)

            self._tray.setContextMenu(menu)
            self._tray.activated.connect(self._tray_activated)
            self._tray.show()
            self._tray_menu = menu

        # ── Config helpers ────────────────────────────────────────────
        def _cfg_to_ui(self) -> None:
            c = self._cfg
            self._w_folder.setText(c.get("root_folder", str(Path.home())))
            self._w_port.setValue(int(c.get("port", 21)))
            proto = c.get("protocol", "ftp")
            if proto == "tftp":
                self._w_proto_tftp.setChecked(True)
            elif proto == "scp":
                self._w_proto_scp.setChecked(True)
            else:
                self._w_proto_ftp.setChecked(True)
            # SCP always requires user auth
            if proto == "scp" or c.get("auth_type", "anonymous") != "anonymous":
                self._w_user.setChecked(True)
            else:
                self._w_anon.setChecked(True)
            self._w_uname.setText(c.get("username", ""))
            self._w_passwd.setText(c.get("password", ""))
            self._w_creds.setEnabled(self._w_user.isChecked())
            self._w_auth_box.setVisible(proto not in ("tftp",))
            if proto == "scp":
                self._w_anon.setEnabled(False)

        def _ui_to_cfg(self) -> dict:
            return {
                "port":        self._w_port.value(),
                "root_folder": self._w_folder.text().strip(),
                "protocol":    (
                    "scp"  if self._w_proto_scp.isChecked()  else
                    "tftp" if self._w_proto_tftp.isChecked() else "ftp"
                ),
                "auth_type":   "anonymous" if self._w_anon.isChecked() else "user",
                "username":    self._w_uname.text().strip(),
                "password":    self._w_passwd.text(),
            }

        # ── Slots ─────────────────────────────────────────────────────
        def _browse(self) -> None:
            d = QFileDialog.getExistingDirectory(
                self, "Select Root Folder", self._w_folder.text()
            )
            if d:
                self._w_folder.setText(d)

        def _on_proto_changed(self) -> None:
            if self._w_proto_tftp.isChecked():
                if self._w_port.value() in (21, 2222):
                    self._w_port.setValue(69)
                self._w_auth_box.setVisible(False)
                self._w_anon.setEnabled(True)
                self._w_port_note.setVisible(False)
            elif self._w_proto_scp.isChecked():
                if self._w_port.value() in (21, 69):
                    self._w_port.setValue(2222)
                self._w_auth_box.setVisible(True)
                self._w_anon.setEnabled(False)
                self._w_user.setChecked(True)
                self._w_creds.setEnabled(True)
                self._w_port_note.setVisible(False)
            else:   # FTP
                if self._w_port.value() in (69, 2222):
                    self._w_port.setValue(21)
                self._w_auth_box.setVisible(True)
                self._w_anon.setEnabled(True)
                self._w_port_note.setVisible(True)


        def _toggle(self) -> None:
            if self._running:
                self._w_btn.setEnabled(False)
                self._worker.stop()
                return

            cfg = self._ui_to_cfg()
            protocol = cfg.get("protocol", "ftp")

            if not os.path.isdir(cfg["root_folder"]):
                QMessageBox.warning(
                    self, "FlashDash", "The root folder does not exist."
                )
                return

            if protocol == "scp":
                if not cfg["username"]:
                    QMessageBox.warning(
                        self, "FlashDash", "SCP requires a username."
                    )
                    return
                if not cfg["password"]:
                    if (
                        QMessageBox.question(
                            self,
                            "FlashDash",
                            "The password is empty. Continue anyway?",
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        != QMessageBox.Yes
                    ):
                        return
            elif protocol == "ftp":
                if cfg["auth_type"] == "user" and not cfg["username"]:
                    QMessageBox.warning(
                        self, "FlashDash", "Please enter a username."
                    )
                    return

                if cfg["auth_type"] == "user" and not cfg["password"]:
                    if (
                        QMessageBox.question(
                            self,
                            "FlashDash",
                            "The password is empty. Continue anyway?",
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        != QMessageBox.Yes
                    ):
                        return

            _save_config(cfg)
            self._cfg = cfg
            self._set_inputs_enabled(False)

            if int(cfg["port"]) < 1024 and not _is_admin() and sys.platform != "win32":
                self._on_log(
                    f"Port {cfg['port']} requires administrator privileges — "
                    "a system password prompt will appear."
                )

            self._worker.start(cfg)

        def _set_inputs_enabled(self, enabled: bool) -> None:
            for w in (
                self._w_folder, self._w_port,
                self._w_anon, self._w_user,
                self._w_uname, self._w_passwd,
                self._w_proto_ftp, self._w_proto_tftp, self._w_proto_scp,
            ):
                w.setEnabled(enabled)

        def _on_started(self) -> None:
            self._running = True
            port      = int(self._cfg["port"])
            auth_type = self._cfg["auth_type"]
            username  = self._cfg.get("username", "")
            protocol  = self._cfg.get("protocol", "ftp")
            ips       = _get_all_ipv4s()

            if protocol == "tftp":
                def _url(ip: str) -> str:
                    port_part = f":{port}" if port != 69 else ""
                    return f"copy tftp://{ip}{port_part}/<filename> flash:"
                self._w_conn_box.setTitle("  📡  Copy IOS image from switch (TFTP)")
            elif protocol == "scp":
                def _url(ip: str) -> str:
                    port_part = f":{port}" if port != 22 else ""
                    return f"copy scp://{username}@{ip}{port_part}/<filename> flash:"
                self._w_conn_box.setTitle("  🔐  Copy IOS image from switch (SCP)")
            else:
                def _url(ip: str) -> str:
                    user_part = f"{username}@" if auth_type != "anonymous" and username else ""
                    port_part = f":{port}" if port != 21 else ""
                    return f"copy ftp://{user_part}{ip}{port_part}/<filename> flash:"
                self._w_conn_box.setTitle("  💻  Copy IOS image from switch (FTP)")

            urls = [_url(ip) for ip in ips]

            self._w_status.setText("●  Running")
            self._w_status.setStyleSheet(
                "color:#2ecc71; font-weight:bold; font-size:14px;"
            )
            self._w_btn.setText("■  Stop Server")
            self._w_btn.setStyleSheet(_BTN_STOP)
            self._w_btn.setEnabled(True)
            self._header_sub.setText("●  Running")
            self._header_sub.setStyleSheet("color:#2ecc71; font-size:11px;")
            self._header_icon.setPixmap(_make_icon(True).pixmap(30, 30))

            if self._tray:
                self._tray_act.setText("Stop Server")
                self._tray.setIcon(_make_icon(True))
                self._tray.setToolTip(f"FlashDash — {protocol.upper()} port {port}")

            # Rebuild per-URL rows
            while self._w_url_layout.count():
                item = self._w_url_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            for url in urls:
                row_w = QWidget()
                row_h = QHBoxLayout(row_w)
                row_h.setContentsMargins(0, 0, 0, 0)
                row_h.setSpacing(6)
                le = QLineEdit(url)
                le.setReadOnly(True)
                le.setFont(QFont("monospace", 11))
                le.setObjectName("connInfo")
                btn_cp = QPushButton("⎘ Copy")
                btn_cp.setFixedWidth(72)
                btn_cp.clicked.connect(
                    lambda _, u=url: QApplication.clipboard().setText(u)
                )
                row_h.addWidget(le)
                row_h.addWidget(btn_cp)
                self._w_url_layout.addWidget(row_w)
            self._w_conn_box.setVisible(True)

            self._on_log(
                f"Server started  |  protocol: {protocol.upper()}  "
                f"|  port: {port}  "
                f"|  root: {self._cfg['root_folder']}"
                + (f"  |  auth: {auth_type}" if protocol == "ftp" else "")
            )

        def _on_stopped(self) -> None:
            self._running = False
            for lbl in list(self._active_xfers.values()):
                lbl.deleteLater()
            self._active_xfers.clear()
            # Clear any lingering labels that were waiting for their 2s delay
            while self._w_xfer_vbox.count():
                item = self._w_xfer_vbox.takeAt(0)
                if item and item.widget():
                    item.widget().deleteLater()
            self._w_xfer_box.setVisible(False)
            self._w_status.setText("●  Stopped")
            self._w_status.setStyleSheet(
                "color:#e74c3c; font-weight:bold; font-size:14px;"
            )
            self._w_btn.setText("▶  Start Server")
            self._w_btn.setStyleSheet(_BTN_START)
            self._w_btn.setEnabled(True)
            self._header_sub.setText("●  Stopped")
            self._header_sub.setStyleSheet("color:#e74c3c; font-size:11px;")
            self._header_icon.setPixmap(_make_icon(False).pixmap(30, 30))

            if self._tray:
                self._tray_act.setText("Start Server")
                self._tray.setIcon(_make_icon(False))
                self._tray.setToolTip("FlashDash — Stopped")

            self._w_conn_box.setVisible(False)
            while self._w_url_layout.count():
                item = self._w_url_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            self._set_inputs_enabled(True)
            self._on_log("Server stopped.")

        def _on_error(self, msg: str) -> None:
            self._on_stopped()
            QMessageBox.critical(self, "FlashDash Error", msg)
            self._on_log(f"ERROR: {msg}")

        def _drain_log_queue(self) -> None:
            q = self._worker._log_q
            while not q.empty():
                try:
                    self._on_log(q.get_nowait())
                except Exception:
                    break
            self._drain_progress_queue()

        def _drain_progress_queue(self) -> None:
            q = self._worker._progress_q
            while not q.empty():
                try:
                    p = q.get_nowait()
                except Exception:
                    break
                xid = p['id']
                if p['done']:
                    # Ensure the entry exists even for very short/interrupted
                    # transfers where done=True arrives before any done=False.
                    if xid not in self._active_xfers:
                        lbl = QLabel()
                        lbl.setFont(QFont("monospace", 9))
                        self._active_xfers[xid] = lbl
                        self._w_xfer_vbox.addWidget(lbl)
                        self._w_xfer_box.setVisible(True)
                    self._active_xfers[xid].setText(self._fmt_progress(p))
                    # Remove after a short delay so the final state is visible.
                    lbl_to_remove = self._active_xfers.pop(xid)
                    QTimer.singleShot(
                        2000,
                        lambda w=lbl_to_remove: self._remove_xfer_label(w),
                    )
                else:
                    if xid not in self._active_xfers:
                        lbl = QLabel()
                        lbl.setFont(QFont("monospace", 9))
                        self._active_xfers[xid] = lbl
                        self._w_xfer_vbox.addWidget(lbl)
                        self._w_xfer_box.setVisible(True)
                    self._active_xfers[xid].setText(self._fmt_progress(p))

        def _remove_xfer_label(self, lbl) -> None:
            try:
                lbl.deleteLater()
            except Exception:
                pass
            if not self._active_xfers and not self._w_xfer_vbox.count():
                self._w_xfer_box.setVisible(False)

        @staticmethod
        def _fmt_progress(p: dict) -> str:
            icon  = "↑" if p['direction'] == 'up' else "↓"
            name  = p['name']
            done  = p['bytes_done']
            total = p['total_bytes']
            speed = p['speed_bps']
            speed_str = _fmt_bytes(int(speed)) + "/s"
            if total:
                pct    = min(100, int(done * 100 / total))
                filled = int(16 * pct / 100)
                bar    = "█" * filled + "░" * (16 - filled)
                return (f"{icon}  {name}   {bar}  {pct:3d}%   "
                        f"{_fmt_bytes(done)} / {_fmt_bytes(total)}   {speed_str}")
            else:
                return f"{icon}  {name}   {_fmt_bytes(done)} received   {speed_str}"

        def _on_log(self, msg: str) -> None:
            esc = _html.escape(msg)
            if msg.startswith("[+]"):
                color = "#4fc3f7"
            elif msg.startswith("[-]"):
                color = "#607d8b"
            elif msg.startswith("[✓]"):
                color = "#66bb6a"
            elif msg.startswith("[✗]"):
                color = "#ef5350"
            elif msg.startswith("[~]"):
                color = "#607d8b"
            elif msg.startswith("[↓]"):
                color = "#42a5f5"
            elif msg.startswith("[↑]"):
                color = "#ffa726"
            elif msg.startswith("[✔]"):
                color = "#66bb6a"
            elif msg.startswith("[!]"):
                color = "#ff7043"
            elif "ERROR" in msg:
                color = "#ef5350"
            elif "started" in msg.lower():
                color = "#66bb6a"
            elif "stopped" in msg.lower():
                color = "#e57373"
            else:
                color = "#9e9ec8"

            self._w_log.moveCursor(QTextCursor.End)
            self._w_log.insertHtml(
                f'<span style="color:{color};">{esc}</span><br/>'
            )
            self._w_log.moveCursor(QTextCursor.End)

        def _tray_activated(self, reason) -> None:
            if reason == QSystemTrayIcon.DoubleClick:
                self._show_window()
            elif reason == QSystemTrayIcon.Context:
                self._tray_menu.exec_(QCursor.pos())

        def _show_window(self) -> None:
            self.show()
            self.raise_()
            self.activateWindow()

        def closeEvent(self, event) -> None:
            if self._tray:
                mb = QMessageBox(self)
                mb.setWindowTitle("FlashDash")
                mb.setText("What would you like to do?")
                btn_tray  = mb.addButton("Minimize to Tray", QMessageBox.AcceptRole)
                btn_close = mb.addButton("Close App",        QMessageBox.DestructiveRole)
                mb.addButton("Cancel",                       QMessageBox.RejectRole)
                mb.exec_()
                clicked = mb.clickedButton()
                if clicked == btn_tray:
                    self.hide()
                    event.ignore()
                elif clicked == btn_close:
                    event.ignore()
                    self._quit()
                else:
                    event.ignore()
            else:
                self._quit()

        def _quit(self) -> None:
            if self._running:
                reply = QMessageBox.question(
                    self,
                    "Quit FlashDash",
                    "The server is running. Stop it and quit?",
                    QMessageBox.Yes | QMessageBox.No,
                )
                if reply != QMessageBox.Yes:
                    return
                self._worker.stop()
            QApplication.quit()

    # ── Launch application ────────────────────────────────────────────────────
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)
    app.setApplicationName("FlashDash")
    if sys.platform != "win32":
        app.setDesktopFileName("flashdash")   # KDE/Wayland: links app to .desktop for icon
    app.setWindowIcon(_make_icon(False))  # set at app level for Wayland/Windows compositors
    app.setStyle("Fusion")
    app.setStyleSheet(_APP_STYLE)

    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if _ARGS.daemon:
        _run_daemon()
    else:
        _run_gui()
