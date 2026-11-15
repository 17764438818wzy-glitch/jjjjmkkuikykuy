"""Compare the isolated page to the current frontend without editing it."""
from pathlib import Path


HERE = Path(__file__).resolve().parents[1]
LIVE = HERE.parents[1] / 'first-step-preview-8795'
SITE = HERE / 'site'


def test_current_frontend_manual_retry_and_other_assets_are_preserved():
    excluded = {'index.html', 'postprocessing-results-server-20261101.js',
                'prefect-ticket-counts-server-20261101.js'}
    for path in SITE.rglob('*'):
        if path.is_file() and path.name not in excluded:
            assert path.read_bytes() == (LIVE / path.relative_to(SITE)).read_bytes(), str(path.relative_to(SITE))
    candidate = (SITE / 'index.html').read_text(encoding='utf8')
    live = (LIVE / 'index.html').read_text(encoding='utf8')
    old_count = '  <script src="prefect-ticket-counts.js?v=20261101-postprocessing-4" defer></script>\n'
    old_results = '  <script src="postprocessing-results.js?v=20261101-results-2" defer></script>\n'
    new_results = ('  <script src="postprocessing-results-server-20261101.js?v=20261101-server-release-1" defer></script>\n'
                   '  <script src="prefect-ticket-counts-server-20261101.js?v=20261101-server-release-1" defer></script>\n')
    assert live.count(old_count) == live.count(old_results) == 1
    assert candidate == live.replace(old_count, '').replace(old_results, new_results)
    assert 'llm-results.js?v=20261101-manual-retry-1' in candidate
