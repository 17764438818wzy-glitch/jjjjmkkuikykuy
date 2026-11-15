import unittest

from education_fast_pipeline import fast_dsl


class FastPipelineTest(unittest.TestCase):
    def test_compilers_are_bypassed_without_changing_annotation_or_index(self):
        ids = ['File', 'VariableAssigner:EducationIndexInput',
               'Compiler:PaperStructure', 'Compiler:PaperWiki',
               'CodeExec:ArtifactSources', 'Tokenizer:EducationIndex']
        components = {key: {'upstream': [], 'downstream': [],
                            'obj': {'params': {'arguments': {}}}} for key in ids}
        components['VariableAssigner:EducationIndexInput']['downstream'] = ['Compiler:PaperStructure']
        components['Compiler:PaperStructure'].update(
            upstream=['VariableAssigner:EducationIndexInput'], downstream=['Compiler:PaperWiki'])
        components['Compiler:PaperWiki'].update(
            upstream=['Compiler:PaperStructure'], downstream=['CodeExec:ArtifactSources'])
        components['CodeExec:ArtifactSources']['upstream'] = ['Compiler:PaperWiki']
        components['CodeExec:ArtifactSources']['obj']['params']['arguments'] = {
            'compiled': 'Compiler:PaperWiki@chunks', 'originals': 'CodeExec:EducationGate@json'}
        dsl = {'components': components, 'graph': {
            'nodes': [{'id': key} for key in ids],
            'edges': [{'id': f'{a}-{b}', 'source': a, 'target': b,
                       'sourceHandle': 'start', 'targetHandle': 'end'} for a, b in zip(ids, ids[1:])]}}
        result = fast_dsl(dsl)
        self.assertEqual(set(result['components']), set(ids) - {'Compiler:PaperStructure', 'Compiler:PaperWiki'})
        self.assertEqual(result['components']['CodeExec:ArtifactSources']['obj']['params']['arguments'], {
            'compiled': 'VariableAssigner:EducationIndexInput@chunks',
            'originals': 'CodeExec:EducationGate@json'})
        self.assertEqual(result['components']['VariableAssigner:EducationIndexInput']['downstream'],
                         ['CodeExec:ArtifactSources'])
        self.assertIn(('VariableAssigner:EducationIndexInput', 'CodeExec:ArtifactSources'),
                      {(row['source'], row['target']) for row in result['graph']['edges']})
        self.assertEqual(dsl['components']['CodeExec:ArtifactSources']['obj']['params']['arguments']['compiled'],
                         'Compiler:PaperWiki@chunks')


if __name__ == '__main__':
    unittest.main()
