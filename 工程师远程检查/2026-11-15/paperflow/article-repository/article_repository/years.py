"""The user's archive convention: the student-number year plus four years."""
from datetime import date
import re


STUDENT_NUMBER=re.compile(r'(?<![a-zA-Z0-9])F?([0-9]{12}|[0-9]{10}|[0-9]{8}|[0-9]{7})(?![a-zA-Z0-9])',re.I)


def _years(text,*,short_ten=False):
    values=set()
    current=date.today().year
    for match in STUDENT_NUMBER.finditer(text):
        number=match.group(1)
        if len(number) in (8,10) and int(number[:4])>=1980:
            try:
                date.fromisoformat(number[:8])
                if (len(number)==8 or int(number[8:])<24) and not re.search(r'学\s*号\s*[:：]?\s*$',text[:match.start()]):
                    continue
            except ValueError:
                pass
        four=int(number[:4])
        if short_ten and len(number)==10 and 10<=int(number[:2])<=current%100:
            admission=2000+int(number[:2])
        elif len(number)==12 and 1980<=four<=current:
            admission=four
        elif len(number)==10 and 1980<=four<=current:
            admission=four
        elif (len(number)==10 and 10<=int(number[:2])<=current%100
              or len(number)==7 and 17<=int(number[:2])<=current%100
              or len(number)==8 and number.startswith('19')):
            admission=2000+int(number[:2])
        else:
            continue
        values.add(str(admission+4))
    return values


def infer_year(record):
    """Use clear source names; dates, hashes and attachments do not supply an ID."""
    path=str(record.get('original_path') or record.get('source_key') or '').replace('\\','/')
    # This corpus's Baoding source consistently uses YY at the start of ten-digit IDs.
    short_ten='保定学院' in path
    folder=_years(str(record.get('folder_name') or ''),short_ten=short_ten)
    if folder:return next(iter(folder)) if len(folder)==1 else None
    names=[str(record.get('original_name') or '')]
    names.extend(str(f.get('name') or '') for f in record.get('files',[])
                 if f.get('kind') in ('原文','原稿'))
    original=set().union(*(_years(name,short_ten=short_ten) for name in names))
    if original:return next(iter(original)) if len(original)==1 else None
    parents=_years('/'.join(path.split('/')[-4:]),short_ten=short_ten)
    return next(iter(parents)) if len(parents)==1 else None
