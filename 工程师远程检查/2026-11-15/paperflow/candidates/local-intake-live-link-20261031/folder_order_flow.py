"""The waiting-stage deployment: it never performs preprocessing."""
from prefect import flow


@flow(name='资料文件夹工单-等待前处理', retries=0)
def waiting_folder_order(task_id: str, group_id: str, receipt_id: str,
                         attempt_id: str, folder_label: str):
    # A dedicated paused work pool is the primary barrier. This is a second,
    # fail-closed barrier should a worker be attached to it by mistake.
    raise RuntimeError('当前只接入等待前处理队列，尚未启用前处理执行')
