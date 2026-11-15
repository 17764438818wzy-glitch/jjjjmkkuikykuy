"""Local OCR candidates only; no model conclusions and no unreviewed OCR in ingest."""
import difflib
import json
import os
import subprocess
from pathlib import Path


def inspect_image(path,redact):
    if os.name!='nt': return {'status':'unavailable','text':'','verified':False}
    from PIL import Image, ImageDraw
    path=Path(path)
    with Image.open(path) as source:
        source.load()
        image=source.convert('RGB')
    # Windows OCR has a bounded input size; keep the original evidence dimensions.
    scale=min(1,2400/max(image.size))
    preview=image.resize((max(1,int(image.width*scale)),max(1,int(image.height*scale)))) if scale<1 else image.copy()
    ocr_path=path.with_name(path.stem+'_ocr_input.png')
    preview.save(ocr_path)
    try:
        result=subprocess.run(['powershell.exe','-NoProfile','-ExecutionPolicy','Bypass','-File',str(Path(__file__).with_name('windows_ocr.ps1')),
                               '-ImagePath',str(ocr_path.resolve())],capture_output=True,encoding='utf-8',errors='replace',timeout=40,
                              creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        if result.returncode: return {'status':'failed','text':'','verified':False,'error':result.stderr[-1000:]}
        data=json.loads(result.stdout.lstrip('\ufeff'))
    finally:
        ocr_path.unlink(missing_ok=True)
    draw=ImageDraw.Draw(image);boxes=[];lines=[]
    for line in data['lines']:
        raw=line['text'];clean=redact(raw);lines.append(clean)
        edits=[(a,b) for op,a,b,_,_ in difflib.SequenceMatcher(None,raw,clean).get_opcodes() if op!='equal' and b>a]
        cursor=0
        for word in line['words']:
            start=raw.find(word['text'],cursor)
            if start<0: continue
            end=start+len(word['text']);cursor=end
            if any(start<b and end>a for a,b in edits):
                rect=[max(0,int(word['x']/scale)-2),max(0,int(word['y']/scale)-2),
                      min(image.width,int((word['x']+word['width'])/scale)+2),min(image.height,int((word['y']+word['height'])/scale)+2)]
                draw.rectangle(rect,fill='black');boxes.append(rect)
    # Normalise to PNG (without metadata), preserving original pixels outside detected PII boxes.
    output=path.with_suffix('.png');image.save(output)
    if output!=path: path.unlink()
    return {'status':'candidate','text':'\n'.join(lines),'verified':False,'source':'ocr',
            'engine':data['engine'],'redaction_boxes':boxes,'image_path':str(output),
            'limitations':['OCR 文字与数值未核对','规则未识别的身份或单位信息仍需人工检查']}
