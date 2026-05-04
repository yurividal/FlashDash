"""Convert old-icon.png to flashdash.ico for PyInstaller."""
from pathlib import Path
from PIL import Image  # Pillow

src = Path(__file__).parent.parent.parent / "assets" / "old-icon.png"
dst = Path(__file__).parent.parent.parent / "assets" / "flashdash.ico"

img = Image.open(src).convert("RGBA")
img.save(
    dst,
    format="ICO",
    sizes=[(256, 256), (128, 128), (64, 64), (32, 32), (16, 16)],
)
print(f"Saved {dst}")
