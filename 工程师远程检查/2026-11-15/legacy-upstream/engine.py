"""Thin desktop adapter over the user's unchanged cleaning-pipeline scripts."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from content_rules import PROCESSING_VERSION, valid_title

LEGACY_ROOT = Path(r"E:\dity\脱敏分类脚本\脱敏分类脚本\cleaning-pipeline")
EXTENSIONS = {".docx", ".doc", ".pdf", ".txt", ".md", ".rtf", ".wps", ".odt", ".zip", ".rar", ".7z"}
EXCLUDED = {".git", ".venv", "__pycache__", "node_modules", "output", "logs", "cleaning-pipeline"}
OFFICE_FORMATS = {".doc", ".rtf", ".wps", ".odt"}
MAX_SCAN_FILES = 20000
_OFFICE_MODULE_LOCK = threading.Lock()


def convert_office(source, output_dir):
    """Use installed WPS; retain the existing converter for hosts without WPS."""
    from material_conversion import current
    managed=current()
    if managed is not None:return managed.convert(source,output_dir,target_format='docx')
    from office_wps import available, convert
    if available():
        return convert(Path(source), Path(output_dir))
    import importlib.util
    import tempfile
    import zipfile
    module_name = "uv_upstream_libreoffice"
    # Publish-before-exec is required by dataclasses in this external module.
    # Keep every caller behind the lock until initialization has completed.
    with _OFFICE_MODULE_LOCK:
        if module_name not in sys.modules:
            spec = importlib.util.spec_from_file_location(module_name, r"E:\uv\src\paper_pipeline\pipeline\libreoffice.py")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                sys.modules.pop(module_name, None)
                raise
        module = sys.modules[module_name]
    env = dict(os.environ)
    resolution = module.resolve_libreoffice_path(env)
    executable = resolution.path if resolution.status == "found" else None
    if not executable:
        executable = next((p for p in [Path(r"C:\Program Files\LibreOffice\program\soffice.exe"), Path(r"C:\Program Files (x86)\LibreOffice\program\soffice.exe")] if p.is_file()), None)
    if not executable:
        raise RuntimeError("未找到 LibreOffice，旧 Office 文件需要转换后重试。")
    output_dir.mkdir(parents=True, exist_ok=True)
    command = module.build_libreoffice_conversion_command(executable, source, output_dir, target_format="docx")
    with tempfile.TemporaryDirectory(prefix="up_lo_") as profile_dir:
        profile = Path(profile_dir).resolve()
        if profile.parent != Path(tempfile.gettempdir()).resolve() or not profile.name.startswith("up_lo_"):
            raise RuntimeError("临时转换目录校验失败。")
        command.insert(1, "-env:UserInstallation=" + profile.as_uri())
        result = subprocess.run(command, capture_output=True, timeout=120, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    (output_dir.parent / (output_dir.name + "_conversion.log")).write_bytes(result.stdout + b"\n" + result.stderr + f"\nexit_code={result.returncode}".encode())
    target = output_dir / (source.stem + ".docx")
    if result.returncode or not target.is_file() or not zipfile.is_zipfile(target):
        raise RuntimeError("LibreOffice 转换未产生有效 DOCX，原文件已保留。")
    return target


def validate_zip(source, depth=0):
    """Protect the legacy archive extractor from absolute/traversal member names."""
    import io
    import zipfile
    from pathlib import PureWindowsPath
    try:
        with zipfile.ZipFile(source) as archive:
            if len(archive.infolist()) > 20000 or sum(i.file_size for i in archive.infolist()) > 2 * 1024**3:
                raise ValueError("压缩包过大，请拆分后处理。")
            for info in archive.infolist():
                path = PureWindowsPath(info.filename)
                if path.drive or path.root or ".." in path.parts:
                    raise ValueError("压缩包包含越界路径，已跳过。")
                if info.filename.lower().endswith(".zip") and depth < 2:
                    if info.file_size > 100 * 1024**2:
                        raise ValueError("嵌套压缩包过大，请先单独解压。")
                    validate_zip(io.BytesIO(archive.read(info)), depth + 1)
    except zipfile.BadZipFile:
        # The original script reports malformed ZIP files without extracting them.
        return


def read_json(path: Path, default=None):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    # A fixed ``.tmp`` name is unsafe when concurrent upload workers persist
    # receipts on Windows.  Unique sibling files retain atomic replacement
    # without allowing one writer to replace another writer's temporary file.
    temp = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        # Windows readers (including indexers/antivirus) can briefly prevent
        # rename/delete even when the file and directory are writable. Retry
        # the atomic replacement, never truncate the existing manifest.
        deadline = time.monotonic() + 2.0
        delay = 0.01
        while True:
            try:
                temp.replace(path)
                break
            except OSError as exc:
                if getattr(exc, "winerror", None) not in {5, 32, 33}:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.2)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            # Cleanup must not replace the original persistence error.
            pass


def csv_rows(path: Path):
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def digest(path: Path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_component(value: str):
    import re
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).strip(" .")[:100]
    if not cleaned or cleaned in {".", ".."}:
        return "未识别"
    if cleaned.upper().split(".")[0] in {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}:
        cleaned = "_" + cleaned
    return cleaned


def scan_sources(folder: Path, limit: int = 50, excluded_root: Path | None = None):
    folder = folder.resolve()
    if not folder.is_dir():
        raise ValueError("请选择存在的输入文件夹。")
    if not 1 <= limit <= MAX_SCAN_FILES:
        raise ValueError(f"单次扫描上限须为 1–{MAX_SCAN_FILES}。")
    if excluded_root and folder.is_relative_to(excluded_root.resolve()):
        raise ValueError("输入不能位于本程序输出目录内。")
    files = []
    from preextract_contract import is_preextract_artifact
    for current, dirs, names in os.walk(folder, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in EXCLUDED and not Path(current, d).is_symlink()
                         and not (excluded_root and Path(current, d).resolve().is_relative_to(excluded_root.resolve())))
        for name in sorted(names):
            path = Path(current, name)
            if not name.startswith(('~$','.')) and name!='_paper_pipeline_status.json' and not path.is_symlink():
                if is_preextract_artifact(path):continue
                files.append({"path": str(path), "name": name, "size": path.stat().st_size, "format": path.suffix.lower()})
                if len(files) >= limit:
                    return files
    return files


def load_legacy_taxonomy():
    import ast
    module = ast.parse((LEGACY_ROOT / "src/paper_cleaning/subject_taxonomy.py").read_text(encoding="utf-8"))
    for statement in module.body:
        if isinstance(statement, ast.AnnAssign) and getattr(statement.target, "id", None) == "SUBJECT_GROUPS":
            groups=ast.literal_eval(statement.value)
            # Local coverage additions, preserving the external legacy catalog.
            education=groups.setdefault('教育教学类',[])
            for major in ('人文教育','数学教育','汉语国际教育'):
                if major not in education:education.append(major)
            return groups
    raise RuntimeError("原分类词表不可读取。")


class Store:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs = self.root / "runs"
        self.runs.mkdir(exist_ok=True)
        self.lock = threading.RLock()

    def history(self):
        results = []
        for path in sorted(self.runs.glob("*/manifest.json"), reverse=True):
            try:
                # History scans run alongside upload receipt writers too.
                # Serialize each read with save/load so Windows can replace
                # the manifest after its reader has closed the file handle.
                with self.lock:
                    from preextract_delivery_queue import project_manifest
                    results.append(project_manifest(self.root,read_json(path)))
            except (OSError, ValueError):
                continue
        return results

    def manifest_path(self, run_id):
        if not run_id or Path(run_id).name != run_id or "/" in run_id or "\\" in run_id:
            raise ValueError("无效批次编号。")
        return self.runs / run_id / "manifest.json"

    def save(self, manifest):
        with self.lock:
            write_json(self.manifest_path(manifest["run_id"]), manifest)

    def load(self, run_id):
        with self.lock:
            from preextract_delivery_queue import project_manifest
            return project_manifest(self.root,read_json(self.manifest_path(run_id)))

    def refresh_view(self, run_id, progress=lambda message: None):
        from auto_view import build_view
        with self.lock:
            manifest = self.load(run_id)
            try:
                result = build_view(self, manifest, progress)
            except Exception as exc:
                result = {'status': 'failed', 'error': str(exc)}
            manifest['human_view'] = result
            self.save(manifest)
            return result

    def recover(self):
        for manifest in self.history():
            if manifest["status"] == "running":
                manifest["status"] = "interrupted"
                manifest["message"] = "上次运行中断。已完成结果保留，可重新选择未完成文件处理。"
                self.save(manifest)


class Processor:
    def __init__(self, store: Store, legacy_root=LEGACY_ROOT):
        self.store = store
        self.legacy_root = Path(legacy_root)
        self.stop = threading.Event()

    def _emit(self, manifest, callback, stage, filename="", message="", level="info"):
        event = {"time": datetime.now().strftime("%H:%M:%S"), "stage": stage, "file": filename, "message": str(message),
                 "level": level, "completed": manifest["processed_sources"], "total": manifest["source_count"]}
        with (self.store.runs / manifest["run_id"] / "progress.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps(event, ensure_ascii=False) + "\n")
        callback({"run_id": manifest["run_id"], "status": manifest["status"], "message": message or stage,
                  "processed_sources": manifest["processed_sources"], "source_count": manifest["source_count"], "progress": event})

    def _run_legacy(self, command, env, log_path, manifest, callback):
        process = subprocess.Popen(command, cwd=self.legacy_root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding="utf-8", errors="replace", creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        lines = queue.Queue()
        def read_output():
            try:
                for line in process.stdout:
                    lines.put(line)
            finally:
                lines.put(None)
        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()
        deadline = time.monotonic() + 7200
        try:
            with log_path.open("w", encoding="utf-8") as log:
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("原脚本运行超过 2 小时，已停止子进程。")
                    try:
                        line = lines.get(timeout=0.5)
                    except queue.Empty:
                        continue
                    if line is None:
                        break
                    log.write(line)
                    log.flush()
                    if line.startswith("UPSTREAM_PROGRESS "):
                        try:
                            event = json.loads(line.removeprefix("UPSTREAM_PROGRESS "))
                        except ValueError:
                            continue
                        self._emit(manifest, callback, event["stage"], event.get("file", ""), event.get("message", ""), event.get("level", "info"))
            return process.wait(timeout=max(1, deadline - time.monotonic()))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            reader.join(timeout=2)
            process.stdout.close()

    def run(self, selected, callback=lambda event: None):
        if not selected:
            raise ValueError("请先选择要处理的文件。")
        if not (self.legacy_root / "src/paper_cleaning/run_stage1.py").is_file():
            raise ValueError("原脱敏分类脚本不存在，请检查 E 盘。")
        self.stop.clear()
        run_id = datetime.now().strftime("UP_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]
        run = self.store.runs / run_id
        manifest = {"schema_version": 1, "run_id": run_id, "status": "running", "started_at": datetime.now().isoformat(timespec="seconds"),
                    "source_count": len(selected), "processed_sources": 0, "items": [], "events": [], "message": "准备本地副本"}
        self.store.save(manifest)
        self._emit(manifest, callback, "任务开始", message=f"共 {len(selected)} 个源文件，每批最多 50 个")
        prior_hashes = {item.get("source_hash") for history in self.store.history() for item in history["items"]
                        if item.get("status") in {"pass", "review"} and item.get("source_hash") and item.get("processing_version") == PROCESSING_VERSION}
        seen_clean = {item.get("evidence_source_hash") for history in self.store.history() for item in history["items"]
                      if item.get("status") in {"pass", "review"} and item.get("clean_hash") and item.get("processing_version") == PROCESSING_VERSION}
        try:
            for start in range(0, len(selected), 50):
                if self.stop.is_set():
                    manifest["status"] = "stopped"
                    break
                batch = run / f"batch_{start // 50 + 1:04d}"
                staging = batch / "input"
                staging.mkdir(parents=True, exist_ok=True)
                source_map = {}
                for index, record in enumerate(selected[start:start + 50]):
                    source = Path(record["path"])
                    self._emit(manifest, callback, "准备文件", source.name, f"第 {start // 50 + 1} 批，准备第 {index + 1} 个文件")
                    try:
                        source_hash = digest(source)
                        if source.suffix.lower() == ".zip":
                            validate_zip(source)
                        if source_hash in prior_hashes:
                            manifest["events"].append({"source": str(source), "status": "duplicate", "reason": "内容指纹与已处理文件重复"})
                            self._emit(manifest, callback, "重复跳过", source.name, "内容指纹与已处理文件重复", "warning")
                            continue
                        copy_root = batch / "originals" if source.suffix.lower() in OFFICE_FORMATS else staging
                        dest = copy_root / f"{index:04d}" / source.name
                        if source.suffix.lower() == ".md":
                            dest = dest.with_suffix(".txt")
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(source, dest)
                        if digest(dest) != source_hash:
                            raise ValueError("复制期间源文件变化，请重新扫描")
                        if source.suffix.lower() in OFFICE_FORMATS:
                            self._emit(manifest, callback, "格式转换", source.name, "Office 格式转换中，单文件超时上限 120 秒")
                            dest = convert_office(dest, staging / f"{index:04d}")
                            self._emit(manifest, callback, "转换完成", source.name, "已产生 DOCX")
                        source_map[dest.relative_to(staging).as_posix()] = {"path": str(source), "sha256": source_hash}
                    except Exception as exc:
                        manifest["events"].append({"source": str(source), "status": "failed", "reason": str(exc)})
                        self._emit(manifest, callback, "准备失败", source.name, str(exc), "error")
                write_json(batch / "source_map.json", source_map)
                if source_map:
                    manifest["message"] = f"正在调用原脚本，第 {start // 50 + 1} 批"
                    self.store.save(manifest)
                    callback(manifest)
                    output = batch / "legacy_output"
                    env = dict(os.environ, PYTHONPATH=str(self.legacy_root), PYTHONDONTWRITEBYTECODE="1", PYTHONIOENCODING="utf-8")
                    command = [sys.executable, "-B", "-u", str(Path(__file__).with_name("progress_runner.py")), "--input", str(staging), "--output", str(output), "--batch-size", "50", "--keep-intermediate"]
                    if self._run_legacy(command, env, batch / "process.log", manifest, callback):
                        raise RuntimeError(f"原脚本执行失败，详见 {batch / 'process.log'}")
                    self._emit(manifest, callback, "整理结果", message="汇总正文、筛选结果与失败记录")
                    self._collect(output, manifest, source_map, seen_clean, callback)
                    for item in manifest["items"]:
                        if item["status"] in {"pass", "review"} and item.get("source_hash"):
                            prior_hashes.add(item["source_hash"])
                manifest["processed_sources"] = min(start + 50, len(selected))
                self.store.save(manifest)
                self._emit(manifest, callback, "批次完成", message=f"已完成 {manifest['processed_sources']}/{len(selected)} 个源文件")
                callback(manifest)
            if manifest["status"] == "running":
                manifest["status"] = "completed"
            manifest["message"] = "已安全停止，当前批次结果已保存" if manifest["status"] == "stopped" else "处理完成，请检查结果后导入 RAGFlow"
        except Exception as exc:
            manifest["status"] = "failed"
            manifest["message"] = str(exc)
        finally:
            manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self.store.save(manifest)
            view = self.store.refresh_view(run_id, lambda message: self._emit(manifest, callback, '整理浏览目录', message=message))
            manifest['human_view'] = view
            if view['status'] == 'failed':
                self._emit(manifest, callback, '浏览目录未更新', message=view['error'], level='warning')
            self._emit(manifest, callback, "任务结束", message=manifest["message"], level="error" if manifest["status"] == "failed" else "info")
            callback(manifest)
        return manifest

    def _collect(self, output, manifest, source_map, seen_clean, callback=lambda event:None):
        from evidence import build_package
        from screening import assess
        quality = {row["file_id"]: row for row in csv_rows(output / "quality_report.csv")}
        extractions = {row['file_id']:row for row in csv_rows(output / 'extract_text_report.csv')}
        run = self.store.runs / manifest["run_id"]
        for row in csv_rows(output / "sanitized_mapping.csv"):
            source = Path(row.get("sanitized_text_path") or row.get("sanitized_path") or "")
            status = row.get("quality_status", "review")
            if row.get("status") != "success" or not source.is_file():
                manifest["events"].append({"source": row.get("original_relative_path"), "status": "failed", "reason": row.get("error_message") or "未产生正文"})
                continue
            extraction=extractions.get(Path(row.get('raw_text_path','')).stem,{})
            cleaned=source.read_text(encoding='utf-8')
            raw_path=Path(row.get('raw_text_path',''))
            raw=raw_path.read_text(encoding='utf-8') if raw_path.is_file() else cleaned
            screening=assess(raw,cleaned,status,quality.get(row['file_id'],{}).get('quality_reason',''))
            status=screening['status']
            native_source=Path(extraction.get('input_path',''))
            if not native_source.is_file():
                manifest['events'].append({'source':row.get('original_relative_path'),'status':'failed','reason':'原始结构来源缺失，未使用旧正文猜测恢复图表'})
                continue
            clean_hash = digest(native_source)
            if clean_hash in seen_clean:
                manifest["events"].append({"source": row.get("original_relative_path"), "status": "duplicate", "reason": "清洗后的正文重复"})
                continue
            category = row.get("detected_subject_group") or "未识别"
            major = row.get("detected_major_or_direction") or "未识别"
            if "未识别" in category or "未识别" in major:
                if status != "reject":
                    status = "review"
            paper_id = "UP_" + clean_hash[:20]
            title = row.get("detected_title", "")
            title_ok = valid_title(title)
            if not title_ok and status != "reject":
                status = "review"
            bucket = "excluded" if status == "reject" else "candidates"
            folder = run / bucket / safe_component(category) / safe_component(major)
            package = folder / safe_component(row['file_id'])
            if package.exists(): package=folder/(safe_component(row['file_id'])+'_'+clean_hash[:12])
            destination=package/'ingest.md'
            original = source_map.get(row.get("original_relative_path", "").replace("\\", "/"), {})
            if status=='reject':
                package.mkdir(parents=True,exist_ok=True)
                excluded_text=package/'excluded.md'
                excluded_text.write_text(cleaned,encoding='utf-8')
                item={'id':paper_id,'legacy_id':row['file_id'],'title':title if title_ok else native_source.stem,
                      'category':category,'major':major,'status':'reject','reason':screening['reason'],
                      'chars':len(cleaned),'source_path':original.get('path',row.get('original_relative_path','')),
                      'source_hash':original.get('sha256',''),'evidence_source_hash':clean_hash,'clean_hash':digest(excluded_text),
                      'path':str(excluded_text),'display_path':str(excluded_text),'reviewed_hash':'','uploads':{},
                      'academic_rating':None,'screening':screening,'processing_version':PROCESSING_VERSION,
                      'content_format':'excluded_text','evaluation_ready':False}
                manifest['items'].append(item)
                write_json(package/'metadata.json',{**item,'run_id':manifest['run_id']})
                self._emit(manifest,callback,'低质/非论文筛除',native_source.name,screening['reason'],'warning')
                continue
            try:
                self._emit(manifest,callback,'生成证据包',native_source.name,'从原文件重新读取正文、原生表格和图片')
                package_meta=build_package(native_source,package,{'id':paper_id,'legacy_id':row['file_id'],
                    'title':title if title_ok else '待确认题目','category':category,'major':major,'run_id':manifest['run_id'],
                    'original_source_sha256':original.get('sha256',''),'parsed_source_format':native_source.suffix.lower(),
                    'screening':screening,
                    'upstream_quality_status':status,'processing_version':PROCESSING_VERSION},render_screenshots=True,run_ocr=True,
                    progress=lambda stage,message:self._emit(manifest,callback,stage,native_source.name,message))
            except Exception as exc:
                manifest['events'].append({'source':row.get('original_relative_path'),'status':'failed','reason':'证据包生成失败：'+str(exc)})
                self._emit(manifest,callback,'证据包失败',native_source.name,str(exc),'error')
                continue
            item = {"id": paper_id, "legacy_id": row["file_id"], "title": title if title_ok else "待确认题目", "category": category, "major": major,
                    "status": status, "reason": screening['reason'], 'screening':screening,"chars": destination.stat().st_size,
                    "source_path": original.get("path", row.get("original_relative_path", "")), "source_hash": original.get("sha256", ""),
                    "clean_hash": digest(destination), "evidence_source_hash":clean_hash,"path": str(destination), "reviewed_hash": "", "uploads": {},
                    "redaction_status": "legacy_rules_applied", "academic_rating": None,
                    "processing_version": PROCESSING_VERSION, "title_status": "detected" if title_ok else "review",
                    "content_format": "evidence_package", "package_path":str(package),'display_path':str(package/'document.md'),
                    'review_status':'needs_review','evaluation_ready':False,'issue_count':len(package_meta['issues']),
                    "images_preserved": bool(package_meta['resources']), "table_structure_preserved": any(b['type']=='table' for b in package_meta['blocks'])}
            if not title_ok:
                item["reason"] = (item["reason"] + "；" if item["reason"] else "") + "题目未可靠识别，需复核"
            manifest["items"].append(item)
            if status in {"pass", "review"}:
                seen_clean.add(clean_hash)
            write_json(package/'record.json', {**item, "run_id": manifest["run_id"]})
        for row in csv_rows(output / "extract_text_report.csv"):
            if row.get("status") != "success":
                manifest["events"].append({"source": row.get("original_relative_path"), "status": "failed", "reason": row.get("error_message") or row.get("status")})
        for row in csv_rows(output / "archive_report.csv"):
            if row.get("status") not in {"success", "extracted"}:
                manifest["events"].append({"source": row.get("archive_path", ""), "status": "archive", "reason": row.get("error_message", "")})
        for row in csv_rows(output / "dedupe_report.csv"):
            manifest["events"].append({"source": row.get("original_relative_path", row.get("file_id", "")), "status": "duplicate", "reason": "原脚本去重记录"})


def approve_items(store, run_id, item_ids):
    manifest = store.load(run_id)
    if manifest["status"] == "running":
        raise ValueError("请等待本批处理结束。")
    for item in manifest["items"]:
        if item["id"] in item_ids:
            if item["status"] not in {"pass", "review"} or "未识别" in item["category"] or "未识别" in item["major"]:
                raise ValueError("筛除或未识别专业的文件不能确认导入。")
            item["reviewed_hash"] = digest(Path(item["path"]))
            if item.get('package_path'):
                from evidence import package_fingerprint
                item['reviewed_package_hash']=package_fingerprint(Path(item['package_path']))
            item["reviewed_at"] = datetime.now().isoformat(timespec="seconds")
    store.save(manifest)
    store.refresh_view(run_id)
    return store.load(run_id)
