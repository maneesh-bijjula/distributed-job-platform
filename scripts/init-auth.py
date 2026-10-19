"""Create a random local bootstrap credential in ignored .env, without printing it."""
import os
from pathlib import Path
import re
import secrets
import tempfile


def main():
    path = Path(__file__).resolve().parent.parent / ".env"
    if path.is_symlink():
        raise SystemExit("Refusing to write credentials through a .env symlink")
    content = path.read_text() if path.exists() else ""
    for line in content.splitlines():
        if line.strip().startswith("JOB_BOOTSTRAP_API_KEY="):
            token = line.strip().split("=", 1)[1].strip().strip("\"'")
            if not re.fullmatch(r"jpk_[A-Za-z0-9_-]{43}", token):
                raise SystemExit("Existing JOB_BOOTSTRAP_API_KEY is invalid; remove its line before initializing")
            os.chmod(path, 0o600)
            print("Existing local bootstrap key retained; .env permissions set to 0600.")
            return
    token = "jpk_" + secrets.token_urlsafe(32)
    if content and not content.endswith("\n"):
        content += "\n"
    content += "JOB_BOOTSTRAP_API_KEY=" + token + "\n"
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".auth-", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()
    print("Initialized a random local bootstrap key in ignored .env (0600). Credential was not printed.")


if __name__ == "__main__":
    main()
