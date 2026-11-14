"""Use the text cleanup decisions on temporary page evidence, never the original.

PDF positions are authoritative. DOCX has no page positions: only exact rendered
lines belonging to removed text, and not also to retained research, may be masked.
Ambiguous lines/graphics remain visible. No OCR inference or model is involved.
"""
from __future__ import annotations

import re
import unicodedata
import hashlib


def _normal(text):
    text=re.sub(r'\[\[[^\]]+\]\]','',str(text or ''))
    return re.sub(r'\s+','',unicodedata.normalize('NFKC',text))


def _lines(page):
    import fitz
    return [dict(text=''.join(s.get('text','') for s in line['spans']),bbox=line['bbox'])
            for b in page.get_text('dict',flags=fitz.TEXTFLAGS_DICT & ~fitz.TEXT_PRESERVE_IMAGES)['blocks']
            for line in b.get('lines',[]) if any(s.get('text','').strip() for s in line['spans'])]


def _graphic_key(drawing):
    return (str(drawing['rect']),str(drawing['items']),drawing.get('type'),
            drawing.get('width'),drawing.get('color'),drawing.get('fill'))


def _image_key(image):
    return (image.get('digest'),image['width'],image['height'],tuple(image['bbox']))


def _page_images(page):
    # get_image_info(hashes=True) can alter subsequent cold-cache rendering of
    # some transparent PDF images in MuPDF. Hash image/mask bytes for identity.
    # Even get_text(dict) can affect that cache, so callers use this only on the
    # analysis document and render pristine pixels from a separate document.
    return [dict(width=b['width'],height=b['height'],bbox=b['bbox'],
                 digest=hashlib.sha256(b['image']+(b.get('mask') or b'')).digest())
            for b in page.get_text('dict')['blocks'] if b['type']==1]


class PageCleanup:
    def __init__(self,kept,removed):
        from preextract_material import _block_text
        self.kept=[(b,_normal(_block_text(b))) for b in kept]
        self.removed=[(r,_normal(_block_text(r['block']))) for r in removed
                      if r['reason'] in {'bounded_cover_before_abstract','bounded_nonresearch_section','repeated_page_margin'}]
        self.records=[]
        self.matched=set()
        self._margins=None
        self._margin_images=set()
        self._margin_graphics=set()
        cover=[r['block'] for r,_ in self.removed if r['reason']=='bounded_cover_before_abstract']
        self.has_cover=bool(cover)
        self.cover_images=any('[[IMAGE:' in _block_text(b) or b.get('type')=='image' for b in cover)
        last=max(((b.get('position') or {}).get('body_index',-1) for b in cover),default=-1)
        table_top=min(((b.get('position') or {})['bbox'][1] for b in cover
                       if b.get('type')=='table' and (b.get('position') or {}).get('page')==1
                       and (b.get('position') or {}).get('bbox')),default=-1)
        from content_rules import valid_title,generic_title
        self.cover_titles=set()
        for b,_ in self.kept:
            pos=b.get('position') or {}
            before_form=(pos.get('part')=='word/document.xml' and 0<=pos.get('body_index',-1)<last
                         or pos.get('page')==1 and pos.get('bbox') and pos['bbox'][3]<table_top)
            if b.get('type')!='paragraph' or not before_form:continue
            lines=[t.strip() for t in _block_text(b).splitlines() if t.strip()]
            if lines and all(valid_title(t,min_length=3) or generic_title(t) for t in lines):
                self.cover_titles.add(b.get('id'))

    def _margin_texts(self,page):
        if self._margins is None:
            found={};pictures={};drawings={}
            for p in page.parent:
                for line in _lines(p):
                    box=line['bbox'];text=_normal(line['text'])
                    zone='top' if box[3]<p.rect.height*.08 else 'bottom' if box[1]>p.rect.height*.92 else None
                    if zone and text:found.setdefault((zone,text),set()).add(p.number)
                for item in _page_images(p):
                    box=item['bbox']
                    if box[3]<p.rect.height*.08 or box[1]>p.rect.height*.92:
                        pictures.setdefault(_image_key(item),set()).add(p.number)
                for item in p.get_drawings():
                    box=item['rect']
                    if box.y1<p.rect.height*.08 or box.y0>p.rect.height*.92:
                        drawings.setdefault(_graphic_key(item),set()).add(p.number)
            self._margins={key for key,pages in found.items() if len(pages)>=3}
            self._margin_images={key for key,pages in pictures.items() if len(pages)>=3}
            self._margin_graphics={key for key,pages in drawings.items() if len(pages)>=3}
        return self._margins

    @staticmethod
    def _at_line(block,page,box):
        import fitz
        pos=block.get('position') or {}
        if 'page' not in pos:return None  # DOCX: text matching is necessary.
        if pos.get('page')!=page.number+1 or not pos.get('bbox'):return False
        area=fitz.Rect(box)
        return (area & fitz.Rect(pos['bbox'])).get_area()>=area.get_area()*.8

    def __call__(self,page):
        import fitz
        from content_rules import generic_title
        lines=_lines(page);margins=self._margin_texts(page)
        full=_normal('\n'.join(line['text'] for line in lines))
        degree=[line for line in lines if generic_title(line['text']) and re.search(r'毕业|学位',line['text'])]
        cover=(self.has_cover and page.number==0 and degree
               and ('学号' in full or '姓名' in full) and ('指导教师' in full or '导师' in full)
               and ('专业' in full or '学院' in full))
        images=_page_images(page)
        image_boxes=[fitz.Rect(im['bbox']) for im in images]
        # A horizontal/vertical stroke has a zero-area rect; account for its
        # actual ink width before any overlap check. Margins alone prove nothing.
        drawings=page.get_drawings()
        graphics=[fitz.Rect(d['rect'])+(-pad,-pad,pad,pad) for d in drawings
                  for pad in [max(.5,float(d.get('width') or 0)/2)]]
        anchors=[]
        for line in lines:
            text=_normal(line['text']);box=fitz.Rect(line['bbox'])
            if len(text)<8 or any(text in value and self._at_line(b,page,box) is not False for b,value in self.kept):continue
            anchors.extend((r['block'].get('id'),box) for r,value in self.removed
                           if text in value and self._at_line(r['block'],page,box) is not False)
        masks=[];protected=[];ids=set();uncertain=[];mask_ids={}
        for line in lines:
            box=fitz.Rect(line['bbox']);text=_normal(line['text'])
            zone='top' if box.y1<page.rect.height*.08 else 'bottom' if box.y0>page.rect.height*.92 else None
            if zone and ((zone,text) in margins or zone=='bottom' and re.fullmatch(r'(?:第)?[\dIVXivx]+(?:页)?',text)):
                continue
            keep=[b for b,value in self.kept if text and text in value and self._at_line(b,page,box) is not False]
            matches=[r for r,value in self.removed if text and text in value
                     and self._at_line(r['block'],page,box) is not False]
            # A repeated research phrase wins over a nonresearch match. A title
            # is only dispensable on a wholly proven cover, not a mixed page.
            if keep:
                if cover and any(b.get('id') in self.cover_titles for b in keep) and not any(b.get('type')=='table' for b in keep):continue
                protected.append(box);continue
            if not matches:
                protected.append(box);continue
            exact_heading=bool(re.fullmatch(r'致谢|致謝|Acknowledg(?:e)?ments?|(?:独创性|原创性|诚信|授权)声明',text,re.I))
            short_unbound=len(text)<8 and not cover and not exact_heading and all(
                self._at_line(r['block'],page,box) is None and text!=value
                for r,value in self.removed if r in matches)
            if short_unbound and any(r['block'].get('id')==bid
                                     and abs(box.y0-area.y1)<=max(box.height,area.height)*3
                                     for r in matches for bid,area in anchors):short_unbound=False
            if short_unbound or any((box & area).get_area()>0 for area in image_boxes+graphics):
                protected.append(box);uncertain.append(list(box));continue
            masks.append(box)
            mask_ids[tuple(box)]={r['block'].get('id','') for r in matches}
        # Do not obscure adjacent retained text even if a PDF glyph box overlaps.
        # Font overhang and antialiasing can extend slightly outside text bboxes.
        # Pad only where that cannot touch retained text or any original image.
        safe=[]
        for box in masks:
            padded=box+(-1,-1,1,1)
            if any((padded & area).get_area()>0 for area in protected+image_boxes+graphics):
                protected.append(box);uncertain.append(list(box));continue
            safe.append(padded);ids.update(mask_ids[tuple(box)])
        masks=safe
        self.matched.update(ids)
        # Unknown visual content prevents whole-page omission. Cover logos are
        # allowed only above the explicit degree heading on the first page.
        cover_logos=bool(cover and self.cover_images and len(image_boxes)<=1
                         and all(box.y1<=min(line['bbox'][1] for line in degree) for box in image_boxes))
        # Repeated marginal ornaments use the same >=3-page proof as existing
        # header/footer text cleanup. A unique marginal graphic is protected.
        unknown_images=[im for im in images if _image_key(im) not in self._margin_images]
        unknown_graphics=[d for d in drawings if _graphic_key(d) not in self._margin_graphics]
        omit=bool(masks and not protected and (not unknown_images or cover_logos) and not unknown_graphics)
        record=dict(page=page.number+1,action='omit_nonresearch_page' if omit else 'mask_nonresearch_regions' if masks else 'retain',
                    boxes=[list(box) for box in masks],source_block_ids=sorted(ids),uncertain_boxes=uncertain,
                    render_boxes=[list(box*page.rotation_matrix) for box in masks],
                    render_size=[page.rect.width,page.rect.height])
        self.records.append(record)
        if omit:return False
        return True

    def apply(self,page_number,blob):
        """Mask only the recorded rectangles on a copy of pristine page pixels."""
        import io
        import math
        from PIL import Image,ImageDraw
        record=next(r for r in self.records if r['page']==page_number)
        if record['action']!='mask_nonresearch_regions':return blob
        with Image.open(io.BytesIO(blob)) as source:
            image=source.copy();draw=ImageDraw.Draw(image)
            sx=image.width/record['render_size'][0];sy=image.height/record['render_size'][1]
            boxes=[]
            for x0,y0,x1,y1 in record['render_boxes']:
                box=[max(0,math.floor(x0*sx)),max(0,math.floor(y0*sy)),
                     min(image.width,math.ceil(x1*sx)),min(image.height,math.ceil(y1*sy))]
                if box[2]<=box[0] or box[3]<=box[1]:continue
                draw.rectangle((box[0],box[1],box[2]-1,box[3]-1),fill='white');boxes.append(box)
            record['pixel_boxes']=boxes
            output=io.BytesIO();image.save(output,'PNG');return output.getvalue()

    def report(self):
        return dict(method='exact_source_text_or_pdf_position',pages=self.records,
                    unlocated_removed_block_ids=[r['block'].get('id') for r,value in self.removed
                        if value and r['block'].get('id') not in self.matched])
