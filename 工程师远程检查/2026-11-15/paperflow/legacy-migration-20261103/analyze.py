import collections,json,ntpath
from pathlib import Path

root=Path(__file__).parent
snapshot=json.loads((root/'source-inventory.json').read_text(encoding='utf-8'))
majors=json.loads((root.parent/'article-repository/majors.json').read_text(encoding='utf-8'))['majors']
known={m[key]:(m['code'],key) for m in majors for key in ('paper_dataset_id','image_dataset_id')}
docs=snapshot['documents'];kbs={k['id']:k for k in snapshot['knowledgebases']}
packages=json.loads((root.parent/'legacy-package-matches.json').read_text(encoding='utf-8'))
identities={(p['task'],p['source_sha256'],p['manifest_sha256']):p for p in packages['matched_packages']}
groups=collections.defaultdict(list)
for doc in docs:
    meta=doc['meta'];key=tuple(meta.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))
    groups[key].append(doc)
unmapped=collections.Counter(kbs[d['kb_id']]['name'] for d in docs if d['kb_id'] not in known)
print(json.dumps({'document_count':len(docs),'identity_groups':len(groups),'complete_identity_groups':sum(all(k) for k in groups),
                  'matched_package_groups':sum(k in identities for k in groups),'matched_document_count':sum(len(v) for k,v in groups.items() if k in identities),
                  'roles':dict(collections.Counter(d['meta'].get('paperflow_role','(missing)') for d in docs)),
                  'unknown_datasets':dict(unmapped),'network':snapshot['network']},ensure_ascii=False))
for key,values in list((k,v) for k,v in groups.items() if k in identities)[:3]:
    item=identities[key];package=Path(item['path'])
    material_path=package.parent/'material.json'
    material=json.loads(material_path.read_text(encoding='utf-8-sig')) if material_path.exists() else {}
    manifest=json.loads((package/'manifest.json').read_text(encoding='utf-8-sig'))
    print(json.dumps({'sample_package':str(package),'source':material.get('source'),
                      'manifest_keys':list(manifest),'body_docs':sum(d['meta'].get('paperflow_role')=='body' for d in values),
                      'package_files':[p.name for p in package.iterdir()],
                      'nearby_files':[p.name for p in package.parent.iterdir()]},ensure_ascii=False)[:3500])
