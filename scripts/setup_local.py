"""Prepare local TLS and per-agent secrets. Existing files are never overwritten."""

import json, os, secrets, shutil, subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]


def write_new(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def main():
    os.chdir(root)
    if Path(".env.coordinator").exists():
        raise SystemExit(
            "Уже настроено: .env.coordinator существует. Файлы не изменены."
        )
    Path(".secrets").mkdir(mode=0o700, exist_ok=True)
    Path("certs").mkdir(mode=0o700, exist_ok=True)
    if not Path("config.yaml").exists():
        shutil.copyfile("config.example.yaml", "config.yaml")
    tokens = {f"worker-0{i}": secrets.token_urlsafe(32) for i in range(1, 4)}
    write_new(".secrets/agent_tokens.json", json.dumps(tokens, indent=2))
    write_new(
        ".env.coordinator",
        "\n".join(
            [
                "SECRET_KEY=" + secrets.token_urlsafe(32),
                "COORDINATOR_AGENT_TOKENS_FILE=.secrets/agent_tokens.json",
                "BOOTSTRAP_ADMIN_USERNAME=admin",
                "BOOTSTRAP_ADMIN_PASSWORD=" + secrets.token_urlsafe(18),
                "BOOTSTRAP_OPERATOR_PASSWORD=" + secrets.token_urlsafe(18),
                "BOOTSTRAP_VIEWER_PASSWORD=" + secrets.token_urlsafe(18),
            ]
        )
        + "\n",
    )
    for name, token in tokens.items():
        write_new(
            ".env." + name,
            f"AGENT_TOKEN={token}\nCOORDINATOR_URL=https://localhost:8000\nCA_CERT=./certs/cert.pem\nSTORAGE_PATH=./data/{name}\n",
        )
    if not Path("certs/cert.pem").exists():
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "30",
                "-keyout",
                "certs/key.pem",
                "-out",
                "certs/cert.pem",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True,
            capture_output=True,
        )
        os.chmod("certs/key.pem", 0o600)
    print(
        "Готово. Пароль admin находится в .env.coordinator. Токены сохранены раздельно; в Git они не входят."
    )


if __name__ == "__main__":
    main()
