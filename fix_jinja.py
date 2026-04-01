import os, re
from jinja2 import Environment

def wrap_scripts_in_raw(content):
    """Wrap all <script type='text/babel'> blocks in Jinja raw tags"""
    def replacer(m):
        body = m.group(2)
        if '{% raw %}' in body:
            return m.group(0)
        return m.group(1) + '{% raw %}' + body + '{% endraw %}' + m.group(3)

    pattern = r'(<script[^>]+type=["\']text/babel["\'][^>]*>)([\s\S]*?)(</script>)'
    return re.sub(pattern, replacer, content)

changed = []
for dirpath, _, filenames in os.walk('templates'):
    for filename in filenames:
        if not filename.endswith('.html'):
            continue
        filepath = os.path.join(dirpath, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            original = f.read()
        fixed = wrap_scripts_in_raw(original)
        if fixed != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(fixed)
            changed.append(filepath)

print(f'Wrapped script blocks in {len(changed)} files:')
for f in changed:
    print(f'  {f}')

env = Environment()
errors = []
print('\nJinja check:')
for dirpath, _, filenames in os.walk('templates'):
    for filename in filenames:
        if not filename.endswith('.html'):
            continue
        filepath = os.path.join(dirpath, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            src = f.read()
        try:
            env.parse(src)
        except Exception as e:
            errors.append((filepath, str(e)))
            print(f'  ERROR {filepath}: {e}')

if not errors:
    print('  All templates OK!')
