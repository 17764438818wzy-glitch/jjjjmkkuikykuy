"""Actionable local failures without copying arbitrary exception payloads."""
import zipfile

from agent_client import AgentError


class LocalDocumentError(AgentError):
    def __init__(self,code,message,position=None):
        super().__init__(message)
        self.code=code
        self.position=position or {}


def describe_failure(exc):
    if isinstance(exc,LocalDocumentError):
        return {'code':exc.code,'message':str(exc),'position':exc.position}
    if isinstance(exc,zipfile.BadZipFile):
        if str(exc).startswith('Bad CRC-32 for file '):
            if not str(exc).startswith("Bad CRC-32 for file 'word/media/"):
                return {'code':'archive_crc_error','message':
                    '压缩包或文档内部成员 CRC 校验失败；请取得完整原文件后重试，原文件未修改。'}
            return {'code':'docx_media_crc', 'message':
                'DOCX 内部资源校验失败（内嵌图片或其他文件损坏）；请在 Office 中打开核对并另存为新文件，'
                '或取得完整原文件后重试。更换 AI 模型无法修复，原文件未修改。'}
        return {'code':'archive_invalid','message':
            '文档或压缩包内部结构损坏，无法完整读取；请核对实际格式并重新另存或取得完整原文件后重试。'}
    if isinstance(exc,AgentError):return {'code':'agent_or_input_error','message':str(exc)}
    code={ValueError:'processing_value_error',FileNotFoundError:'source_or_resource_missing',
          PermissionError:'file_access_denied',OSError:'file_io_error'}
    label=type(exc).__name__
    return {'code':next((v for t,v in code.items() if isinstance(exc,t)),'processing_error'),
            'message':'解析或处理异常：'+label+'；请结合处理环节核查文件和本地资源，不能据此判定论文不合格'}
