"""Generate deterministic, offline SVG diagrams and source-code excerpts."""

from pathlib import Path
from html import escape
import ast, re

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/images"
OUT.mkdir(exist_ok=True)


def start(w, h):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img"><defs><marker id="a" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#334155"/></marker></defs><rect width="100%" height="100%" fill="#f8fafc"/><g font-family="Arial,sans-serif" font-size="15" fill="#172033">'
    ]


def box(parts, x, y, w, h, lines, color="#e6efff"):
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{color}" stroke="#a2b3cf"/>'
    )
    for i, line in enumerate(lines):
        parts.append(
            f'<text x="{x+w/2}" y="{y+27+i*22}" text-anchor="middle">{escape(line)}</text>'
        )


def arrow(parts, x, y, xx, yy, label="", lx=None, ly=None):
    parts.append(
        f'<path d="M{x},{y} L{xx},{yy}" fill="none" stroke="#334155" stroke-width="2" marker-end="url(#a)"/>'
    )
    if label:
        parts.append(
            f'<text x="{lx if lx is not None else (x+xx)/2}" y="{ly if ly is not None else (y+yy)/2-9}" text-anchor="middle" font-size="13">{escape(label)}</text>'
        )


def finish(parts, name):
    parts.append("</g></svg>")
    (OUT / name).write_text("".join(parts))


p = start(980, 520)
box(p, 35, 200, 180, 85, ["Браузер", "Формы и мониторинг"])
box(p, 335, 190, 230, 100, ["Координатор", "API · аренда · лимиты"])
box(p, 350, 35, 200, 78, ["SQLite", "Подтверждённое состояние"], "#f0eaff")
box(p, 700, 35, 240, 78, ["Агент 1", "HTTP · загрузчик · диск"])
box(p, 700, 200, 240, 78, ["Агент 2", "HTTP · загрузчик · диск"])
box(p, 700, 365, 240, 78, ["Агент 3", "HTTP · загрузчик · диск"])
box(p, 335, 390, 230, 78, ["Kinescope и CDN", "Страницы · HLS · MP4"], "#e5f5ef")
arrow(p, 215, 240, 335, 240, "HTTPS", 275, 222)
arrow(p, 450, 190, 450, 113, "SQL", 475, 153)
for yy in [75, 240, 405]:
    arrow(
        p,
        565,
        220 if yy == 75 else 240 if yy == 240 else 260,
        700,
        yy,
        "API",
        632,
        145 if yy == 75 else 222 if yy == 240 else 340,
    )
arrow(p, 565, 443, 700, 443, "медиа", 630, 465)
p.append(
    '<text x="35" y="500" font-size="13">Видео идёт на диск агента. Координатор хранит сведения о файле.</text>'
)
finish(p, "components.svg")
p = start(980, 570)
for x, name in [(100, "Агент"), (460, "Координатор"), (840, "SQLite")]:
    box(p, x - 75, 20, 150, 50, [name])
    p.append(
        f'<line x1="{x}" y1="80" x2="{x}" y2="535" stroke="#a5b4c8" stroke-dasharray="5 5"/>'
    )
arrow(p, 100, 110, 460, 110, "1. lease: прошу блок", 280, 100)
arrow(p, 460, 155, 840, 155, "2. BEGIN IMMEDIATE + назначение", 650, 145)
arrow(p, 840, 200, 460, 200, "3. commit", 650, 190)
arrow(p, 460, 245, 100, 245, "4. блок + lease_token + параметры", 280, 235)
arrow(p, 100, 305, 460, 305, "5. heartbeat продлевает аренду", 280, 295)
arrow(p, 100, 365, 460, 365, "6. progress: конечный результат ID", 280, 355)
arrow(p, 460, 420, 840, 420, "7. проверка токена + запись + checkpoint", 650, 410)
arrow(p, 460, 475, 100, 475, "8. подтверждение сохранения", 280, 465)
p.append(
    '<text x="35" y="555" font-size="14">Медиа скачивается между шагами управления; heartbeat не ждёт завершения файла.</text>'
)
finish(p, "sequence.svg")
p = start(980, 440)
box(p, 30, 170, 150, 65, ["created"])
box(p, 300, 170, 170, 65, ["running"], "#e5f5ef")
box(p, 690, 35, 190, 65, ["paused"])
box(p, 690, 170, 190, 65, ["stopped"])
box(p, 690, 310, 190, 65, ["completed"])
box(p, 300, 310, 170, 65, ["failed"], "#fff0e5")
arrow(p, 180, 202, 300, 202, "start", 240, 188)
arrow(p, 470, 182, 690, 75, "pause", 570, 110)
arrow(p, 690, 60, 470, 170, "resume", 560, 65)
arrow(p, 470, 202, 690, 202, "stop", 580, 188)
arrow(p, 690, 220, 470, 225, "resume", 580, 250)
arrow(p, 470, 230, 690, 340, "диапазон завершён", 560, 320)
arrow(p, 385, 235, 385, 310, "нет узлов", 430, 277)
p.append(
    '<path d="M785,375 L785,410 L210,410 L210,235 L300,235" stroke="#334155" stroke-width="2" fill="none" marker-end="url(#a)"/><text x="490" y="402" text-anchor="middle" font-size="13">retry_failed — явный повтор только ошибок</text>'
)
finish(p, "states.svg")
# Insert exact code from the final implementation, not a similar pseudocode example.
guide = ROOT / "docs/guide.md"
text = guide.read_text()


def code(match):
    file, name = match.group(1), match.group(2)
    source = (ROOT / file).read_text()
    tree = ast.parse(source)
    node = next(
        n
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
    )
    first = min([node.lineno] + [d.lineno for d in node.decorator_list])
    snippet = "\n".join(source.splitlines()[first - 1 : node.end_lineno])
    return f"Источник: [{file}](../{file}).\n\n```python\n{snippet}\n```"


text = re.sub(r"<!-- CODE:([^:]+):([^ ]+) -->", code, text)
guide.write_text(text)
print("Diagrams and exact code excerpts generated")
