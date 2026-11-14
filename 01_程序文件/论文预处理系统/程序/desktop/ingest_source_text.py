"""Recognize only verified, deterministic source-text parser transformations."""
import re


def nonblank_lines(text):
    return '\n'.join(line for line in text.splitlines() if line.strip())


def _literal_table_cells(line):
    # Deliberately exclude inline Markdown/HTML and escapes. Unknown rendering
    # stays a proof conflict instead of guessing how the server interpreted it.
    if not line.startswith('|') or not line.rstrip().endswith('|'):
        return None
    cells = [cell.strip() for cell in line.rstrip()[1:-1].split('|')]
    if len(cells) < 2 or any(re.search(r'[&<>\\`*_~\[\]]', cell) for cell in cells):
        return None
    return cells


def _render_literal_tables(source):
    """Reproduce the observed Markdown->HTML plain-table output, outside fences.

    Cell values, ordering and column counts must be preserved. No prose, HTML,
    inline formatting or unknown table dialect is normalized by this fallback.
    """
    lines = source.splitlines()
    output, index, fence = [], 0, None
    while index < len(lines):
        line = lines[index]
        if fence:
            if re.fullmatch(r' {0,3}' + re.escape(fence[0]) + '{' + str(fence[1]) + r',}\s*', line):
                fence = None
            output.append(line); index += 1
            continue
        opened = re.match(r' {0,3}(`{3,}|~{3,})', line)
        if opened:
            fence = (opened[1][0], len(opened[1]))
            output.append(line); index += 1
            continue
        header = _literal_table_cells(line)
        separator = _literal_table_cells(lines[index + 1]) if index + 1 < len(lines) else None
        if not (header and separator and len(header) == len(separator)
                and all(re.fullmatch(r':?-{3,}:?', cell) for cell in separator)):
            output.append(line); index += 1
            continue
        rows, end = [], index + 2
        while end < len(lines):
            cells = _literal_table_cells(lines[end])
            if cells is None or len(cells) != len(header): break
            rows.append(cells); end += 1
        if not rows:
            output.append(line); index += 1
            continue
        output.extend(['<table>', '<thead>', '<tr>'])
        output.extend('<th>' + cell + '</th>' for cell in header)
        output.extend(['</tr>', '</thead>', '<tbody>'])
        for row in rows:
            output.append('<tr>')
            output.extend('<td>' + cell + '</td>' for cell in row)
            output.append('</tr>')
        output.extend(['</tbody>', '</table>'])
        index = end
    return '\n'.join(output)


def matches_parsed_source(source, parsed):
    expected = nonblank_lines(parsed)
    return (nonblank_lines(source) == expected
            or nonblank_lines(_render_literal_tables(source)) == expected)
