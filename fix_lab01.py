import json
import re

path = "notebooks/day1/lab/guide/LAB_01_guide.ipynb"

with open(path, 'r') as f:
    nb = json.load(f)

fixed = 0
for cell in nb['cells']:
    if cell['cell_type'] == 'markdown':
        new_source = []
        for line in cell['source']:
            cleaned = re.sub(r'\n?<screen = [^>]+>\n?', '', line)
            if cleaned != line:
                fixed += 1
                cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
            new_source.append(cleaned)
        cell['source'] = new_source

with open(path, 'w') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print(f"Fixed {fixed} <screen> tags")
