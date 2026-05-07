import os
import re

template_dir = 'templates'
pattern = re.compile(r'\{\{.*?\}\}')

for root, dirs, files in os.walk(template_dir):
    for file in files:
        if file.endswith('.html'):
            path = os.path.join(root, file)
            # print(f"Scanning {path}...")
            if 'snacks_menu.html' in file:
                print(f"DEBUG: Scanning {path}")
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                matches = pattern.findall(content)
                for m in matches:
                    if ':' in m and 'csrf_token' not in m:
                        print(f"File: {path}")
                        print(f"  Match: {m}")
