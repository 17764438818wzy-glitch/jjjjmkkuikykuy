"""Preparation-stage admission on the existing seal/correction boundaries."""
from .embedding_admission import AdmissionRejected, digest, repair_route
from .producer import SyntaxRepairExhausted


def seal_with_length_repair(workflow, task):
    from .phase_cache import CACHE_STAGES, input_fingerprint, phase_dependency
    task = dict(task)
    while True:
        try:
            if hasattr(workflow,'seal_managed'):
                sealed=workflow.seal_managed(task)
            else:
                sealed = workflow.actions.run('local', dict(operation='seal'),
                    lambda cancel: workflow.seal(task), total_timeout=300)
            task.update(sealed, stage='handoff')
            return task
        except AdmissionRejected as exc:
            evidence = exc.evidence
        failures = [row for row in evidence['records'] if row['issues']]
        owners = {repair_route(row) for row in failures}
        task['embedding_admission'] = evidence
        if (not evidence['counter_verified'] or not evidence['path_verified'] or not failures
                or None in owners):
            # Mixed structural and ordinary defects remain explicit together;
            # no model budget is spent while the structural contract is pending.
            task.update(stage='held', hold_reason='embedding_structure_pending' if None in owners
                        else 'embedding_admission_unverified')
            return task
        used = workflow.actions.correction_count()
        if used >= 2:
            task.update(stage='held', hold_reason='embedding_correction_budget_exhausted')
            return task
        owner = min(owners, key=CACHE_STAGES.index)
        marker = dict(owner=owner, rule_fingerprint=evidence['rule_fingerprint'],
                      defect_fingerprint=digest(failures), records=failures, correction_ordinal=used+1)
        task['preparation_length_repair'] = marker
        fingerprint = input_fingerprint(workflow.config, task, owner)
        try:
            # Existing source-keyed cumulative budget survives revision/re-entry.
            workflow.actions.reserve_correction(owner, fingerprint, used + 1)
        except SyntaxRepairExhausted:
            task.update(stage='held', hold_reason='embedding_correction_budget_exhausted')
            return task
        affected = CACHE_STAGES[CACHE_STAGES.index(owner):]
        outputs = {s: v for s, v in task.get('stage_outputs', {}).items() if s not in affected}
        dependencies = {s: v for s, v in task.get('stage_dependencies', {}).items() if s not in affected}
        for stage in affected:
            # Existing producer/cache execution is reused. Preserve failed_stage
            # and all earlier verified outputs; start from the complete draft.
            repair_task = dict(task, repaired_stage=stage, stage_outputs=outputs,
                               stage_dependencies=dependencies)
            result = workflow.producer.run_stage(repair_task, stage)
            task.update(result)
            outputs[stage] = dict(input_sha256=result['stage_input_sha256'], directory=result['draft_dir'])
            dependencies[stage] = phase_dependency(task, stage, stage, result)
        task.update(stage_outputs=outputs, stage_dependencies=dependencies)
