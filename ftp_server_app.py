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
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
_CONFIG_DIR  = Path.home() / ".config" / "flashdash"
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
def _make_server(cfg: dict, log_fn=None):
    """Return a configured pyftpdlib FTPServer instance."""
    from pyftpdlib.handlers import FTPHandler       # type: ignore
    from pyftpdlib.servers import FTPServer         # type: ignore
    from pyftpdlib.authorizers import DummyAuthorizer  # type: ignore

    # Silence pyftpdlib's default logger; we use our own callbacks instead.
    logging.getLogger("pyftpdlib").handlers = []
    logging.getLogger("pyftpdlib").setLevel(logging.CRITICAL)

    _fn   = log_fn   # capture for use inside method bodies
    _auth = DummyAuthorizer()
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

    class _Handler(FTPHandler):
        authorizer    = _auth
        passive_ports = range(60000, 60100)
        banner        = "FTP Server ready."

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

        # ── Transfer start ────────────────────────────────────────────
        def ftp_RETR(self, file):
            """Client downloading a file from the server."""
            try:
                full = self.fs.ftp2fs(file)
                size = os.path.getsize(full)
                _emit(
                    f"[↓] {self.remote_ip}  download start  "
                    f"{os.path.basename(full)}  ({_fmt_bytes(size)})"
                )
            except Exception:
                pass
            super().ftp_RETR(file)

        def ftp_STOR(self, file, mode="w"):
            """Client uploading a file to the server."""
            _emit(
                f"[↑] {self.remote_ip}  upload start  "
                f"{os.path.basename(file)}"
            )
            super().ftp_STOR(file, mode)

        # ── Transfer complete ─────────────────────────────────────────
        def on_file_sent(self, file):
            """Download finished."""
            try:
                size = os.path.getsize(file)
                size_str = f"  ({_fmt_bytes(size)})"
            except Exception:
                size_str = ""
            _emit(
                f"[✔] {self.remote_ip}  download done   "
                f"{os.path.basename(file)}{size_str}"
            )

        def on_file_received(self, file):
            """Upload finished."""
            try:
                size = os.path.getsize(file)
                size_str = f"  ({_fmt_bytes(size)})"
            except Exception:
                size_str = ""
            _emit(
                f"[✔] {self.remote_ip}  upload done     "
                f"{os.path.basename(file)}{size_str}"
            )

        def on_incomplete_file_sent(self, file):
            _emit(f"[!] {self.remote_ip}  download INTERRUPTED  {os.path.basename(file)}")

        def on_incomplete_file_received(self, file):
            try:
                os.remove(file)
            except Exception:
                pass
            _emit(f"[!] {self.remote_ip}  upload INTERRUPTED  {os.path.basename(file)}")

    return FTPServer(("0.0.0.0", int(cfg["port"])), _Handler)


# ─────────────────────────────────────────────────────────────────────────────
# TFTP server factory
# ─────────────────────────────────────────────────────────────────────────────
def _make_tftp_server(cfg: dict, log_fn=None):
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
                prefix = "[!]" if record.levelno >= logging.WARNING else "[~]"
                log_fn(f"{prefix} TFTP: {record.getMessage()}")

        hdlr = _TftpLogHandler()
        hdlr.setLevel(logging.DEBUG)
        lg = logging.getLogger("tftpy")
        lg.handlers = [hdlr]
        lg.setLevel(logging.INFO)
    else:
        logging.getLogger("tftpy").setLevel(logging.CRITICAL)

    return tftpy.TftpServer(cfg["root_folder"])


def _get_all_ipv4s() -> list:
    """Return all local IPv4 addresses (excludes loopback)."""
    try:
        out = subprocess.check_output(
            ["hostname", "-I"], stderr=subprocess.DEVNULL
        ).decode().split()
        ips = [ip for ip in out if ":" not in ip and ip != "127.0.0.1"]
        return ips if ips else ["127.0.0.1"]
    except Exception:
        # Fallback: single UDP-trick
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return [ip]
        except Exception:
            return ["127.0.0.1"]


# ─────────────────────────────────────────────────────────────────────────────
# Daemon mode  (launched via pkexec for privileged ports < 1024)
# ─────────────────────────────────────────────────────────────────────────────
def _run_daemon() -> None:
    import signal as _sig

    cfg       = _load_config(_ARGS.config)
    sock_path = _ARGS.socket
    protocol  = cfg.get("protocol", "ftp")

    def _log(msg: str) -> None:
        print(msg, flush=True)

    try:
        if protocol == "tftp":
            server = _make_tftp_server(cfg, log_fn=_log)
        else:
            server = _make_server(cfg, log_fn=_log)
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

    # ── App icon from assets/icon.svg ─────────────────────────────────────────
    _ICON_PATH = str(Path(__file__).parent / "assets" / "old-icon.png")

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

        # ── Public API ────────────────────────────────────────────────
        def start(self, cfg: dict) -> None:
            self._protocol = cfg.get("protocol", "ftp")
            privileged = int(cfg["port"]) < 1024 and os.geteuid() != 0
            if privileged:
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
                if protocol == "tftp":
                    srv = _make_tftp_server(cfg, log_fn=self._log_q.put)
                else:
                    srv = _make_server(cfg, log_fn=self._log_q.put)
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

            # Read daemon stdout into the log queue
            proc_ref = self._proc
            log_q    = self._log_q

            def _read_stdout() -> None:
                try:
                    for raw in proc_ref.stdout:
                        line = raw.decode(errors="replace").rstrip()
                        if line:
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
            self.setWindowTitle("FlashDash FTP/TFTP Server")
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
            title_lbl = QLabel("FlashDash FTP/TFTP Server")
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
            self._w_proto_ftp  = QRadioButton("FTP  (TCP — port 21)")
            self._w_proto_tftp = QRadioButton("TFTP  (UDP — port 69)")
            bg_proto = QButtonGroup(gp)
            bg_proto.addButton(self._w_proto_ftp, 0)
            bg_proto.addButton(self._w_proto_tftp, 1)
            self._w_proto_ftp.setChecked(True)
            vp.addWidget(self._w_proto_ftp)
            vp.addWidget(self._w_proto_tftp)
            vp.addStretch()
            self._w_proto_tftp.toggled.connect(self._on_proto_changed)
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
            for _pv, _pl in [(21, "21  FTP"), (2121, "2121"), (990, "990  FTPS")]:
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
            note = QLabel(
                "Passive ports 60000–60100 must be open in your firewall "
                "for passive-mode clients."
            )
            note.setObjectName("noteLabel")
            note.setWordWrap(True)
            vn.addWidget(note)
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
            else:
                self._w_proto_ftp.setChecked(True)
            if c.get("auth_type", "anonymous") == "anonymous":
                self._w_anon.setChecked(True)
            else:
                self._w_user.setChecked(True)
            self._w_uname.setText(c.get("username", ""))
            self._w_passwd.setText(c.get("password", ""))
            self._w_creds.setEnabled(self._w_user.isChecked())
            self._w_auth_box.setVisible(proto != "tftp")

        def _ui_to_cfg(self) -> dict:
            return {
                "port":        self._w_port.value(),
                "root_folder": self._w_folder.text().strip(),
                "protocol":    "tftp" if self._w_proto_tftp.isChecked() else "ftp",
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

        def _on_proto_changed(self, tftp: bool) -> None:
            if tftp:
                if self._w_port.value() == 21:
                    self._w_port.setValue(69)
                self._w_auth_box.setVisible(False)
            else:
                if self._w_port.value() == 69:
                    self._w_port.setValue(21)
                self._w_auth_box.setVisible(True)


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

            if protocol == "ftp":
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

            if int(cfg["port"]) < 1024 and os.geteuid() != 0:
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
                self._w_proto_ftp, self._w_proto_tftp,
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
                    return f"tftp://{ip}{port_part}"
                self._w_conn_box.setTitle("  📡  Connect from your TFTP client")
            else:
                def _url(ip: str) -> str:
                    user_part = f"{username}@" if auth_type != "anonymous" and username else ""
                    port_part = f":{port}" if port != 21 else ""
                    return f"ftp://{user_part}{ip}{port_part}"
                self._w_conn_box.setTitle("  💻  Connect from your FTP client")

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
    app.setDesktopFileName("flashdash")   # KDE/Wayland: links app to .desktop for icon
    app.setWindowIcon(_make_icon(False))  # set at app level for Wayland compositors
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
