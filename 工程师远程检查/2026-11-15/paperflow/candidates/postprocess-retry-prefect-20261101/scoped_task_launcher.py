"""Bind one existing Prefect folder to its own independently verified roots.

This is inert composition: it has no task discovery, scheduler, network call,
fallback to local processing, or process-wide serialization lock. Callers
provide the original runtime, a fresh registered-folder policy reader and the
reviewed remote runtime builder.
"""
import copy

from remote_folder_config import derive_folder_config


class ScopedTaskLauncher:
    def __init__(self, runtime, template, expected_counter, *, read_policy, make_remote, run_exact):
        if (not callable(getattr(runtime, 'check_configuration', None))
                or runtime.execution_enabled is not True
                or not isinstance(template, dict) or not isinstance(expected_counter, dict)
                or not all(callable(item) for item in (read_policy, make_remote, run_exact))):
            raise ValueError('the original enabled Prefect runtime and explicit policy callbacks are required')
        self.runtime = runtime
        self.template = copy.deepcopy(template)
        self.expected_counter = copy.deepcopy(expected_counter)
        self.read_policy, self.make_remote, self.run_exact = read_policy, make_remote, run_exact

    def prepare(self, identity, generation, *, resume_job_id=None, retry_job_id=None):
        self.runtime.check_configuration()
        supplied = copy.deepcopy(identity)
        if retry_job_id is not None:
            record = self.read_policy(supplied, generation, retry_job_id=retry_job_id)
        elif resume_job_id is not None:
            record = self.read_policy(supplied, generation, resume_job_id=resume_job_id)
        else:
            record = self.read_policy(supplied, generation)
        if supplied != identity:
            raise ValueError('policy reader changed the original folder identity')
        record = copy.deepcopy(record)
        if (not isinstance(record, dict) or record.get('resume_job_id') != resume_job_id
                or record.get('retry_job_id') != retry_job_id):
            raise ValueError('policy reader changed the original stopped stage identity')
        config = derive_folder_config(self.template, record, identity=identity, generation=generation,
                                      expected_counter=self.expected_counter)
        prepared = dict(identity=copy.deepcopy(identity), generation=generation, record=record, config=config)
        if resume_job_id is not None:
            prepared['resume_job_id'] = resume_job_id
        if retry_job_id is not None:
            prepared['retry_job_id'] = retry_job_id
        return prepared

    async def launch(self, identity, generation, prepared):
        self.runtime.check_configuration()
        if (not isinstance(prepared, dict) or set(prepared) not in
                ({'identity', 'generation', 'record', 'config'},
                 {'identity', 'generation', 'record', 'config', 'resume_job_id'},
                 {'identity', 'generation', 'record', 'config', 'retry_job_id'})
                or prepared['identity'] != identity or prepared['generation'] != generation
                or not isinstance(prepared['record'], dict)
                or prepared['record'].get('resume_job_id') != prepared.get('resume_job_id')
                or prepared['record'].get('retry_job_id') != prepared.get('retry_job_id')):
            raise ValueError('the prepared policy belongs to another original folder')
        config = derive_folder_config(self.template, prepared['record'], identity=identity, generation=generation,
                                      expected_counter=self.expected_counter)
        if config != prepared['config']:
            raise ValueError('the independently derived folder policy changed')
        scoped_runtime = copy.copy(self.runtime)
        scoped_runtime.remote = self.make_remote(copy.deepcopy(config))
        if 'resume_job_id' in prepared:
            from remote_scoped_job import resume_stopped_folder
            result = await resume_stopped_folder(scoped_runtime, copy.deepcopy(identity), generation,
                                                 prepared['resume_job_id'])
        elif 'retry_job_id' in prepared:
            result = await self.run_exact(scoped_runtime, copy.deepcopy(identity), generation,
                                          retry_job_id=prepared['retry_job_id'])
        else:
            result = await self.run_exact(scoped_runtime, copy.deepcopy(identity), generation)
        if not isinstance(result, dict) or not isinstance(result.get('state'), str):
            raise ValueError('the original Prefect stage outcome is unavailable')
        return result['state']
