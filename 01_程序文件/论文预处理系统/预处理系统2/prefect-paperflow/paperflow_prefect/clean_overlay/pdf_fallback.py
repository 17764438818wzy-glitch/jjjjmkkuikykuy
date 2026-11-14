"""Page-local fallback when the existing PDF layout parser cannot finish."""


def parse_pdf_pages(source, redact, resource, issue, check_stop):
    import fitz
    from evidence import _pdf_image_payload
    blocks, images = [], []
    with fitz.open(source) as pdf:
        for page in pdf:
            check_stop()
            number = page.number + 1
            position = {'page': number}
            try:
                page_blocks = page.get_text('dict', sort=True)['blocks']
            except InterruptedError:
                raise
            except Exception:
                position['bbox'] = list(page.rect * page.derotation_matrix)
                issue('pdf_page_incomplete', '该页原生解析失败，后续页面继续本地解析；建议核对原页。', position)
                blocks.append({'type':'unresolved_object','text':'[页面待核]','position':position,
                               'unresolved_object':True})
                continue
            issue('pdf_visual_incomplete', '使用逐页文字回退；表格和图形结构建议核对。', position)
            for index, block in enumerate(page_blocks):
                check_stop()
                pos = {'page':number,'bbox':list(block['bbox']),'block_index':index}
                if block.get('type') == 1:
                    try:
                        blob, extension = _pdf_image_payload(block, page)
                        rid = f'fallback_image_{number}_{index}'
                        row = resource(rid, blob, extension, 'image', pos)
                    except InterruptedError:
                        raise
                    except Exception:
                        issue('pdf_visual_incomplete', '本页图片未能恢复；其他内容继续处理。', pos)
                        continue
                    image = {'id':rid,'type':'image','position':pos,'resource_id':row['id'],
                             'privacy_status':'pending','ocr':{'status':'unavailable','verified':False},
                             'source':'original','caption':'','inline':False}
                    images.append(image);blocks.append(image)
                else:
                    lines = [redact(''.join(span.get('text','') for span in line.get('spans',[])))
                             for line in block.get('lines',[])]
                    if lines:
                        blocks.append({'type':'paragraph','text':'\n'.join(lines),
                                       'heading_level':0,'position':pos})
    return blocks, images, []
