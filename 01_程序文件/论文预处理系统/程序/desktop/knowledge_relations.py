"""Relations supported by original identities and explicit textual references."""
import re
import unicodedata

REFERENCE=re.compile(r'(图|表)\s*(\d+(?:\s*[.\-—－]\s*\d+)*)')


def references(text):
    text=unicodedata.normalize('NFKC',text)
    return list(dict.fromkeys(m[1]+re.sub(r'\s+','',m[2]).replace('—','-').replace('－','-') for m in REFERENCE.finditer(text)))


def caption_key(text,kind):
    keys=references(text)
    return keys[0] if keys and keys[0].startswith('表' if kind=='table' else '图') else None


def explicit_relations(blocks,tables,figures):
    targets={}
    captions=set()
    for kind,objects in [('table',tables),('figure',figures)]:
        for obj in objects:
            if kind=='table' and obj.get('source')=='native_chart_cache':continue
            key=caption_key(obj.get('caption',''),kind)
            if key:
                targets.setdefault(key,[]).append(obj['id'])
                captions.add(re.sub(r'\s+','',unicodedata.normalize('NFKC',obj.get('caption',''))))
    result=[]
    for block in blocks:
        if block['type']!='paragraph' or block.get('heading_level'):continue
        # Captions describe an object; they are not analysis paragraphs citing it.
        normalized=re.sub(r'\s+','',unicodedata.normalize('NFKC',block.get('text','')))
        if normalized in captions:continue
        for key in references(block.get('text','')):
            found=targets.get(key,[])
            result.append({'type':'explicit_reference' if len(found)==1 else 'unresolved_reference',
                'from_id':block['id'],'to_id':found[0] if len(found)==1 else None,'candidate_ids':found,
                'reference':key,'section_id':block['section_id'],'source':'original_text'})
    return result


def native_data_tables(figure,source_blocks,tables):
    pos=figure.get('position',{})
    target=next((b for b in source_blocks if b.get('id')==pos.get('after_id')),None)
    obj=pos.get('object_ref') or (target or {}).get('position',{}).get('object_ref')
    if not obj:return []
    matches=[t for t in tables if t.get('source')=='native_chart_cache' and t.get('position',{}).get('object_ref')==obj]
    # A container with several charts is not a proven one-to-one data source.
    return matches if len(matches)==1 else []
