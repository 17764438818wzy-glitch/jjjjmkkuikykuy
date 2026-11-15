"""Desktop launcher: preserve the old project, open its new upstream mode."""
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    try:
        from app import main
        main()
    except Exception:
        log = ROOT / "startup-error.log"
        log.write_text(traceback.format_exc(), encoding="utf-8")
        import ctypes
        ctypes.windll.user32.MessageBoxW(None, f"程序启动失败，错误记录：\n{log}", "论文知识库预处理", 16)
