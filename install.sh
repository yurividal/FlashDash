#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FlashDash — installer
# Creates a virtualenv, installs dependencies, writes a launcher script
# and a .desktop entry so the app appears in your application menu.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$DIR/venv"
LAUNCHER="$DIR/flashdash"
DESKTOP="$HOME/.local/share/applications/flashdash.desktop"

# ── Dependency check ──────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 is not installed." >&2
    exit 1
fi

PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Using Python $PYVER"

# ── Virtual environment ───────────────────────────────────────────────────────
echo "Creating virtual environment…"
python3 -m venv "$VENV"

echo "Installing dependencies…"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q pyftpdlib PyQt5 tftpy

# ── Launcher script ───────────────────────────────────────────────────────────
cat > "$LAUNCHER" <<LAUNCHER_EOF
#!/usr/bin/env bash
DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
exec "\$DIR/venv/bin/python" "\$DIR/ftp_server_app.py" "\$@"
LAUNCHER_EOF
chmod +x "$LAUNCHER"

# ── Desktop entry ─────────────────────────────────────────────────────────────
mkdir -p "$(dirname "$DESKTOP")"
cat > "$DESKTOP" <<DESKTOP_EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=FlashDash
GenericName=FTP/TFTP Server
Comment=Simple FTP/TFTP server for network engineers
Exec=$LAUNCHER
Icon=$DIR/assets/old-icon.png
Terminal=false
Categories=Network;FileTransfer;Internet;
Keywords=ftp;tftp;server;file;transfer;network;
StartupNotify=true
DESKTOP_EOF

# Refresh desktop database if available
if command -v update-desktop-database &>/dev/null; then
    update-desktop-database "$HOME/.local/share/applications" 2>/dev/null || true
fi

echo ""
echo "Installation complete!"
echo ""
echo "  Run the app:   $LAUNCHER"
echo "  Or search for 'FTP Server' in your application launcher."
echo ""
echo "Note: to share on port 21 you will be prompted for your"
echo "      administrator password when starting the server."
echo "      Use port 2121 (or any port ≥ 1024) to avoid this."
