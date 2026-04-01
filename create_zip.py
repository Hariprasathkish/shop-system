import zipfile
import os

def create_git_ready_zip(output_filename, source_dir):
    # Exclude these folders/files
    exclude_dirs = {'.venv', 'venv', 'env', '__pycache__', '.vscode', '.idea', '.git'}
    exclude_files = {'.env'}
    exclude_extensions = {'.db', '.pyc', '.pyo'}

    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            # Prune directories from os.walk traversal
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if file in exclude_files:
                    continue
                if any(file.endswith(ext) for ext in exclude_extensions):
                    continue
                    
                full_path = os.path.join(root, file)
                # Create a relative path for the zip structure
                rel_path = os.path.relpath(full_path, source_dir)
                zipf.write(full_path, rel_path)

if __name__ == "__main__":
    output = "shop_system_git_ready.zip"
    source = "."
    # Delete if exists to avoid recursion or adding to old
    if os.path.exists(output):
        os.remove(output)
    
    print(f"Creating {output}...")
    
    # Custom create_git_ready_zip to ignore the output file
    exclude_dirs = {'.venv', 'venv', 'env', '__pycache__', '.vscode', '.idea', '.git'}
    exclude_files = {'.env', output, 'create_zip.py'} # Exclude self too
    exclude_extensions = {'.db', '.pyc', '.pyo'}

    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        for root, dirs, files in os.walk(source):
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            for file in files:
                if file in exclude_files or any(file.endswith(ext) for ext in exclude_extensions):
                    continue
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, source)
                zipf.write(full_path, rel_path)
    print("Done!")
