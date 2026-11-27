"""Install the exact official native signer ABI used by the Rust Lighter adapter."""
import hashlib
import io
import json
import zipfile
from pathlib import Path
import urllib.request

VERSION = "1.1.2"
SHA256 = "6e5ede71ecc3f74002752421f823d9493459ade7f6e01b37539b5be4c4299b66"

def main():
    destination = Path(__file__).resolve().parents[1] / "rust/vendor/lighter/lighter-signer-windows-amd64.dll"
    if destination.is_file() and hashlib.sha256(destination.read_bytes()).hexdigest() == SHA256:
        print(f"Verified installed official signer: {destination}")
        return
    with urllib.request.urlopen(f'https://pypi.org/pypi/lighter-sdk/{VERSION}/json', timeout=30) as response:
        metadata = json.load(response)
    wheel = next(row for row in metadata['urls'] if row['filename'].endswith('.whl'))
    url = wheel['url']
    with urllib.request.urlopen(url, timeout=60) as response:
        archive = response.read(150_000_001)
    if hashlib.sha256(archive).hexdigest() != wheel['digests']['sha256']:
        raise RuntimeError('Wheel download checksum mismatch')
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        data = zipped.read(f'lighter/signers/{destination.name}')
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise RuntimeError("Official signer checksum mismatch; existing file unchanged")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    print(f"Verified official signer: {destination}")

if __name__ == "__main__":
    main()
