"""Render original documents; screenshot associations remain candidates until reviewed."""
from pathlib import Path
import subprocess
import tempfile
import difflib

VECTOR_RENDERER = r'C:\Program Files\LibreOffice\program\soffice.exe'


def render_office_pdf(source, output_dir, *, timeout=120):
    """One renderer seam for pages, tables and vector images in the original flow."""
    from material_conversion import current
    managed=current()
    if managed is not None:return managed.convert(source,output_dir,target_format='pdf',timeout=timeout)
    import shutil
    from office_wps import available, convert
    if available():
        return convert(Path(source), Path(output_dir), target_format='pdf', timeout=timeout)
    source, output_dir = Path(source), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    executable = Path(VECTOR_RENDERER)
    if not executable.is_file():
        raise RuntimeError('未找到可用的 Office 页面转换程序。')
    with tempfile.TemporaryDirectory(prefix='paper_pdf_') as temporary:
        folder = Path(temporary).resolve()
        if folder.parent != Path(tempfile.gettempdir()).resolve() or not folder.name.startswith('paper_pdf_'):
            raise RuntimeError('临时转换目录校验失败')
        local = folder / ('source' + source.suffix.lower())
        shutil.copy2(source, local)
        command = [str(executable), '-env:UserInstallation=' + (folder / 'profile').as_uri(),
                   '--headless', '--convert-to', 'pdf', '--outdir', str(folder), str(local)]
        result = subprocess.run(command, capture_output=True, timeout=timeout,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        (output_dir / 'conversion.log').write_bytes(result.stdout + b'\n' + result.stderr)
        generated = folder / 'source.pdf'
        if result.returncode or not generated.is_file():
            raise RuntimeError('原文转换为 PDF 失败；原稿已保留。')
        target = output_dir / (source.stem + '.pdf')
        shutil.copy2(generated, target)
        return target


def load_visual_image(source):
    """Rasterize metafiles with a font-aware renderer, preserving all visible ink."""
    import io
    import math
    import shutil
    import fitz
    from PIL import Image, ImageChops
    from agent_client import AgentError
    source=Path(source)
    if source.suffix.lower() not in {'.wmf','.emf'}:
        with Image.open(source) as image:
            # Word displays transparent image ink on the white document page.
            # Dropping alpha directly can turn black text and its background black.
            if 'A' in image.getbands() or 'transparency' in image.info:
                rgba=image.convert('RGBA')
                return Image.alpha_composite(Image.new('RGBA',rgba.size,'white'),rgba).convert('RGB')
            return image.convert('RGB')
    try:
        with tempfile.TemporaryDirectory(prefix='paper_vector_',ignore_cleanup_errors=True) as directory:
            folder=Path(directory).resolve()
            if folder.parent!=Path(tempfile.gettempdir()).resolve() or not folder.name.startswith('paper_vector_'):
                raise AgentError('矢量图临时目录检查失败')
            local=folder/('source'+source.suffix.lower());shutil.copy2(source,local)
            pdf_path=render_office_pdf(local,folder/'rendered',timeout=50)
            with fitz.open(pdf_path) as pdf:
                if len(pdf)!=1:raise AgentError('矢量图转换产生多页或空文件，未截断研究内容；请核对原图')
                page=pdf[0]
                if page.rect.is_empty or page.rect.is_infinite:raise AgentError('矢量图页面尺寸无效')
                scale=min(3.0, math.sqrt(12_000_000/page.rect.get_area()))
                image=Image.open(io.BytesIO(page.get_pixmap(matrix=fitz.Matrix(scale,scale),alpha=False).tobytes('png'))).convert('RGB')
            # Remove only pure-white page margins. Every non-white pixel,
            # including pale lines and small subscripts, remains in the image.
            bounds=ImageChops.difference(image,Image.new('RGB',image.size,'white')).getbbox()
            if bounds:
                x0,y0,x1,y1=bounds
                image=image.crop((max(0,x0-4),max(0,y0-4),min(image.width,x1+4),min(image.height,y1+4)))
            return image
    except (AgentError,InterruptedError):raise
    except subprocess.TimeoutExpired:
        raise AgentError('矢量图渲染超时，未调用 AI；可稍后重试或核对原图格式') from None
    except Exception:
        raise AgentError('矢量图无法可靠转换为视觉输入；请核对原图，未自动放行') from None


def redact_page(page):
    from evidence import redactor
    clean=redactor();rectangles=[]
    for line in page.get_text().splitlines():
        changed=clean(line)
        for op,a,b,_,_ in difflib.SequenceMatcher(None,line,changed).get_opcodes():
            if op!='equal' and b>a:
                for rect in page.search_for(line[a:b]):
                    page.add_redact_annot(rect,fill=(0,0,0));rectangles.append(list(rect))
    if rectangles: page.apply_redactions(images=0,graphics=0)
    return rectangles


def capture_docx_tables(source,blocks,resource,issue):
    import fitz
    with tempfile.TemporaryDirectory(prefix='ev_pdf_') as work:
        folder=Path(work).resolve()
        if folder.parent!=Path(tempfile.gettempdir()).resolve() or not folder.name.startswith('ev_pdf_'):
            raise RuntimeError('临时目录检查失败')
        pdf_path=render_office_pdf(source,folder/'pdf')
        with fitz.open(pdf_path) as pdf:
            redactions={i:redact_page(page) for i,page in enumerate(pdf)}
            # A table can continue across pages. Keep every candidate page, never call a match verified.
            for b in blocks:
                if b['type']!='table' or not b['complex']: continue
                needles=list(dict.fromkeys(c['text'].strip() for c in b['cells'] if len(c['text'].strip())>=2))
                hits=[]
                for pi,page in enumerate(pdf):
                    count=sum(bool(page.search_for(text)) for text in needles)
                    if count>=min(3,len(needles)) and count>0:
                        hits.append((pi,page))
                if not hits:
                    issue('table_page_unlocated','表格原文页无法可靠定位，需要人工定位',b['position']);continue
                b['screenshot_ids']=[]
                for pi,page in hits:
                    rid=f"source_page_{pi+1:04d}"
                    r=resource(rid,page.get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png'),'.png','table_source_page',{'page':pi+1})
                    r['source']='original_page_render';r['redaction_boxes']=redactions[pi]
                    b['screenshot_ids'].append(r['id'])
                b['source_page_candidates']=[pi+1 for pi,_ in hits]
                b['screenshot_match_verified']=False
                issue('table_screenshot_match_review','截图为原文候选整页，表格对应关系和页面脱敏需要核对',b['position'])
