"""Identify unsupported DOCX containers before invoking the document parser."""
from pathlib import Path
import shutil
import zipfile

from agent_client import AgentError


def normalized_docx_text(source):
    """Use the same compatibility branch as the native evidence parser."""
    import io
    from xml.etree import ElementTree as ET
    from native_docx import W, select_compatibility
    from src.paper_cleaning.extract_text import paragraph_text, table_text
    with zipfile.ZipFile(source) as archive:xml=archive.read('word/document.xml')
    namespaces=dict(value for _,value in ET.iterparse(io.BytesIO(xml),events=('start-ns',)))
    document=ET.fromstring(xml)
    select_compatibility(document,namespaces)
    body=document.find(W+'body')
    if body is None:return ''
    lines=[]
    for child in body:
        if child.tag==W+'p':lines.extend(paragraph_text(child))
        elif child.tag==W+'tbl':lines.extend(table_text(child))
        elif child.tag==W+'sectPr':lines.append('[SECTION_BREAK]')
    return '\n'.join(line for line in lines if line!='')


def _compound_streams(source):
    """Recognize CFB before ZIP: a legacy Word file can embed DrawingML ZIPs."""
    with source.open('rb') as stream:
        if stream.read(8)==bytes.fromhex('d0cf11e0a1b11ae1'):
            # CFB directory records are 128-byte aligned. Inspect names and entry
            # types, not arbitrary occurrences of these words in document text.
            needed={'EncryptionInfo','EncryptedPackage','WordDocument'}
            found=set()
            stream.seek(0)
            while chunk:=stream.read(64*1024):
                for name in tuple(needed):
                    encoded=(name+'\0').encode('utf-16le')
                    offset=chunk.find(encoded)
                    while offset>=0:
                        if (offset%128==0 and offset+128<=len(chunk) and chunk[offset+66]==2 and
                                int.from_bytes(chunk[offset+64:offset+66],'little')==len(encoded)):
                            needed.remove(name)
                            found.add(name)
                            break
                        offset=chunk.find(encoded,offset+2)
                if {'EncryptionInfo','EncryptedPackage'}<=found:
                    raise AgentError('这是加密的 Office 文档；请用原密码在 Office 中打开，另存为解密后的 DOCX 再处理。原文件未修改。')
            return found
    return None


def normalize_docx_container(source, output_dir):
    """Convert mislabeled legacy Word through a private copy; keep encryption gated."""
    source=Path(source)
    streams=_compound_streams(source)
    if streams is None:
        validate_docx_container(source)
        return source
    if 'WordDocument' not in streams:
        raise AgentError('文件扩展名为 DOCX，但实际是无法识别的旧版 Office 文件；请从 Office 重新另存为 DOCX。原文件未修改。')
    from engine import convert_office,digest
    output_dir=Path(output_dir)
    output_dir.mkdir(parents=True,exist_ok=True)
    private=output_dir/'legacy_source.doc'
    before=digest(source)
    # Exclusive creation avoids overwriting any pre-existing diagnostic copy.
    with source.open('rb') as original,private.open('xb') as copied:
        shutil.copyfileobj(original,copied)
    if digest(private)!=before or digest(source)!=before:
        raise AgentError('复制旧版 Office 文件时源内容发生变化，请重试。原文件未修改。')
    converted=convert_office(private,output_dir/'converted')
    validate_docx_container(converted)
    return converted


def validate_docx_container(source):
    source=Path(source)
    if _compound_streams(source) is not None:
        raise AgentError('文件扩展名为 DOCX，但实际格式是旧版 Office 复合文件；请在 Office 中重新另存为 DOCX 后处理。')
    if zipfile.is_zipfile(source):
        with zipfile.ZipFile(source) as archive:
            if 'word/document.xml' not in archive.namelist():
                raise AgentError('文件虽是 ZIP 容器，但缺少 Word 正文部件；不是可读取的论文 DOCX，请从 Office 重新另存为 DOCX。原文件未修改。')
        return
    raise AgentError('文件不是有效的 DOCX 容器；请确认实际格式或从 Office 重新另存为 DOCX。原文件未修改。')
