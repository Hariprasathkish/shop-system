import sys
import os

# Add the project root to sys.path so Flask app is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

# Vercel expects an 'app' or 'handler' callable at the module level
# The Flask 'app' object acts as the WSGI callable directly
