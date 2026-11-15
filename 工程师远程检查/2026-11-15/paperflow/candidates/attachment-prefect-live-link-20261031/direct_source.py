"""Read original sources directly; temporary copies belong only to conversion."""

import importlib
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile

from reading_cleanup import ScopeUncertain

FIRST_STEP = Path(__file__).resolve().parent.parent / 'first-step-control-20261029' / 'paperflow_prefect'
NAMESPACE = '_clean_material_first_step'


def detect_format(source):
    if NAMESPACE not in sys.modules:
        spec = importlib.util.spec_from_file_location(NAMESPACE, FIRST_STEP / '__init__.py',
                                                      submodule_search_locations=[str(FIRST_STEP)])
        if spec is None or spec.loader is None:
            raise RuntimeError('第一步格式识别器不可用')
        module = importlib.util.module_from_spec(spec)
        sys.modules[NAMESPACE] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(NAMESPACE, None)
            raise
    reader = importlib.import_module(NAMESPACE + '.group_evidence')
    kind, evidence = reader.detect_format(Path(source))
    if kind == 'unknown':
        try:
            from PIL import Image
            with Image.open(source) as image:
                name = (image.format or '').lower()
            if name in {'bmp', 'tiff', 'webp'}:
                return name, 'image_header'
        except (OSError, ValueError):
            pass
    return kind, evidence


def prepare_readable_source(source, private, detected_format, conversion_dir):
    source, private = Path(source), Path(private)
    kind = detected_format or detect_format(source)[0]
    if kind in {'docx', 'pdf', 'txt', 'md', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp'}:
        if kind == 'docx':
            from office_input import validate_docx_container
            validate_docx_container(source)
        return source, kind
    if kind in {'doc', 'rtf', 'wps', 'odt'}:
        from engine import convert_office
        from material_conversion import preparation_context
        conversion_dir = Path(conversion_dir)
        target = conversion_dir / 'converted'
        # An outer managed pool can persist full converted files. Disable it
        # only for this conversion and restore its context afterwards.
        with preparation_context(conversion_dir, enabled=False):
            if source.suffix.lower() == '.' + kind:
                converted = convert_office(source, target)
            else:
                # Some Office tools rely on suffixes.  This is a conversion-only
                # source copy in a temporary directory, authorized by the user.
                temporary = conversion_dir / ('source.' + kind)
                shutil.copyfile(source, temporary)
                converted = convert_office(temporary, target)
        return Path(converted), 'docx'
    raise ScopeUncertain(f'正文真实格式无法安全读取：{kind}')
