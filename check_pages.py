import requests, sys

session = requests.Session()
# Login first
resp = session.post('http://127.0.0.1:5000/login', data={'username':'admin','password':'admin123'}, allow_redirects=True)
print('Login status:', resp.status_code, resp.url)

# Pages to check
pages = [
    '/admin',
    '/dairy',
    '/dairy/accounts',
    '/dairy/attendance',
    '/dairy/billing',
    '/dairy/customers',
    '/dairy/products',
    '/dairy/staff',
    '/dairy/payroll',
    '/snacks',
    '/snacks/billing',
    '/snacks/accounts',
    '/snacks/inventory',
    '/snacks/stock',
]

for page in pages:
    try:
        r = session.get(f'http://127.0.0.1:5000{page}', allow_redirects=True)
        status = 'OK' if r.status_code == 200 else f'FAIL({r.status_code})'
        if 'TemplateSyntaxError' in r.text or 'Internal Server Error' in r.text or 'jinja2' in r.text.lower():
            status += ' [JINJA/SERVER ERROR]'
        print(f'{page}: {status}')
    except Exception as e:
        print(f'{page}: EXCEPTION - {e}')
