"""Desktop status only. Execution requires an explicit bounded managed selection."""
from pathlib import Path
import ctypes

if __name__=='__main__':
    guide=Path(__file__).parent/'docs/OPERATOR_GUIDE.md'
    ctypes.windll.user32.MessageBoxW(None,
        '正式批量仍暂停。小批处理使用 paperflow managed 的显式选稿入口。\n\n操作说明：'+str(guide),
        'Paperflow 受管链路',0x40)
