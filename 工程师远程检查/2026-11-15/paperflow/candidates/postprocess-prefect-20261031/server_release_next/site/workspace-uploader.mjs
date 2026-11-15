import { Uppy, Dashboard, Tus } from './vendor/uppy/uppy.min.mjs';
import zhCN from './vendor/uppy/zh_CN.mjs';
import { bindFolderPreview } from './folder-preview.mjs?v=20261030-4';

// Uppy and tusd handle the picker and transfer. This adapter only binds their
// upload to the existing first-step task and its server-side file manifest.
export function createWorkspaceUploader({ taskId, apiBase, tusUrl, target, folderHost, isActive = () => true, onStatus, onBusy, onReady }) {
  // Resume only within this open dashboard while its File objects are retained.
  const noPersistentUrls = { findAllUploads: async () => [], findUploadsByFingerprint: async () => [],
    removeUpload: async () => {}, addUpload: async () => '' };
  const writerId = crypto.randomUUID();
  const uppy = new Uppy({ id: `first-step-${taskId}`, autoProceed: false,
    allowMultipleUploadBatches: false, locale: zhCN })
    .use(Dashboard, { inline: true, target, height: 210, width: '100%', showSelectedFiles: false,
      proudlyDisplayPoweredByUppy: false, disableThumbnailGenerator: true,
      fileManagerSelectionType: 'folders',
      doneButtonHandler: null, note: '请选择一个资料文件夹。上传后需点击“开始”才会处理。' })
    .use(Tus, { endpoint: tusUrl, addRequestId: true, parallelUploads: 1,
      urlStorage: noPersistentUrls, storeFingerprintForResuming: false,
      headers: (file) => ({ 'X-Task-ID': taskId, 'X-Folder-Upload-ID': file.meta.folder_upload_id,
        'X-Writer-ID': writerId }),
      limit: 4, allowedMetaFields: ['task_id', 'folder_upload_id', 'relativePath', 'name', 'type'] });

  async function request(path, body, timeoutMs = 15000) {
    const options = { cache: 'no-store' };
    if (timeoutMs) options.signal = AbortSignal.timeout(timeoutMs);
    if (body !== undefined) {
      options.method = 'POST';
      options.headers = { 'Content-Type': 'application/json' };
      options.body = JSON.stringify(body);
    }
    const response = await fetch(new URL(path, apiBase), options);
    let data = {};
    try { data = await response.json(); } catch (_) { /* 状态需以实际响应为准 */ }
    if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : '无法确认上传状态');
    return data;
  }

  async function readManifest(uploadId) {
    const items = [];
    let first;
    for (let page = 1; ; page += 1) {
      const data = await request(`/api/v1/first-step/uploads/${encodeURIComponent(uploadId)}?page=${page}&page_size=200`);
      if (data.task_id !== taskId || data.upload_id !== uploadId || !Number.isInteger(data.expected_files) || !Array.isArray(data.items)) {
        throw new Error('上传批次信息不完整');
      }
      first ||= data;
      items.push(...data.items);
      if (items.length >= data.expected_files) break;
      if (!data.items.length) throw new Error('上传清单尚未完整返回');
    }
    return { status: first.status, items };
  }

  let uploadId = null;
  let selectedCount = 0;
  let finished = false;
  let frozenRows = null;
  const originalFiles = new Map();
  const refreshFolders = bindFolderPreview(uppy, folderHost, isActive, () =>
    Boolean(frozenRows) || Object.keys(uppy.getState().currentUploads || {}).length > 0);
  uppy.addPreProcessor(async (fileIds) => {
    onBusy(true);
    refreshFolders();
    try {
      const files = uppy.getFiles();
      const sameWindow = Boolean(frozenRows);
      const rows = files.map((file) => ({ relative_path: file?.meta?.relativePath, size: file?.size }));
      if (!rows.length || rows.some((row) => typeof row.relative_path !== 'string' || !row.relative_path.includes('/') || !Number.isInteger(row.size))) {
        throw new Error('请选择一个完整的文件夹');
      }
      const root = rows[0].relative_path.split('/')[0];
      if (rows.some((row) => row.relative_path.split('/')[0] !== root)) throw new Error('一次只能选择一个根文件夹');
      const chosen = new Map(rows.map((row) => [row.relative_path, row.size]));
      if (chosen.size !== rows.length) throw new Error('文件夹内存在重复路径');
      if (frozenRows) {
        if (rows.length !== frozenRows.length || files.some((file) => originalFiles.get(file.meta.relativePath) !== file.data)) {
          throw new Error('文件选择已变化，请核对原资料');
        }
      }
      selectedCount = rows.length;
      onStatus('正在核对文件清单');
      const batch = await request(`/api/v1/first-step/tasks/${encodeURIComponent(taskId)}/uploads`, { root_name: root });
      if (batch.task_id !== taskId || !batch.upload_id) throw new Error('上传批次尚未确认');
      uploadId = batch.upload_id;
      const existing = await readManifest(uploadId);
      if (!['uploading', 'sealed'].includes(existing.status)) throw new Error('此任务的上传状态已变化，请重新打开任务');
      if (existing.items.some((item) => !chosen.has(item.relative_path) || chosen.get(item.relative_path) !== item.size)) {
        throw new Error('所选文件夹与之前的上传清单不一致');
      }
      if (!sameWindow && existing.items.some((item) => item.tus_url))
        throw new Error('旧上传需核对原文件，暂不能重新选择后续传');
      if (!sameWindow) {
        frozenRows = rows;
        for (const file of files) originalFiles.set(file.meta.relativePath, file.data);
      }
      if (existing.status === 'sealed' && existing.items.length !== rows.length) throw new Error('请选择原来上传的完整文件夹');
      if (existing.status === 'uploading') {
        for (let i = 0; i < rows.length; i += 500) {
          await request(`/api/v1/first-step/uploads/${encodeURIComponent(uploadId)}/manifest`, { files: rows.slice(i, i + 500) });
        }
        const sealed = await request(`/api/v1/first-step/uploads/${encodeURIComponent(uploadId)}/seal`, { expected_files: rows.length });
        if (sealed.task_id !== taskId || sealed.expected_files !== rows.length) throw new Error('上传清单数量未确认');
      }
      uppy.setMeta({ task_id: taskId, folder_upload_id: uploadId });
      onStatus('正在上传资料');
    } catch (error) {
      onBusy(false);
      onStatus(error.message || '上传准备未完成，请核对后重试');
      throw error;
    }
  });

  uppy.addPostProcessor(async () => {
    try {
      const files = uppy.getFiles();
      if (!uploadId || files.length !== selectedCount || files.some((file) => !file.progress?.uploadComplete)) {
        onStatus('部分文件未上传，可在当前窗口重试');
        return;
      }
      onStatus('正在确认全部文件，资料较多时需要一些时间');
      onBusy(false);
      const result = await request(`/api/v1/first-step/uploads/${encodeURIComponent(uploadId)}/finish`, {}, 0);
      if (result.task_id !== taskId || !['ready', 'needs_review'].includes(result.task_status)) throw new Error('上传结果尚未确认');
      finished = true;
      const hasUngrouped = Array.isArray(result.ungrouped_files) && result.ungrouped_files.length > 0;
      onStatus(result.task_status === 'needs_review' ? '上传完成，有资料待核对' :
        hasUngrouped ? '上传完成，部分资料待核对，任务等待开始' : '上传完成，任务等待开始');
      onReady(result);
    } catch (error) {
      onStatus(error.message || '上传结果未确认，请核对任务状态');
      throw error;
    } finally {
      onBusy(false);
    }
  });
  uppy.on('error', () => onBusy(false));
  uppy.on('upload-error', () => onStatus('部分文件上传未完成，请确认原文件及续传记录'));
  uppy.on('cancel-all', () => {
    onBusy(false);
    if (!finished) onStatus('上传未完成，原文件需核对后才能继续');
  });
  return uppy;
}
