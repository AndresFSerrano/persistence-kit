from __future__ import annotations

import argparse
import sys
import tempfile
import webbrowser
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from persistence_kit.mail.errors import MailTemplateError
from persistence_kit.mail.templates import IMAGES_DIRECTORY, MailTemplates

DEFAULT_OUTPUT = Path(tempfile.gettempdir()) / "mail-preview"


@dataclass(frozen=True, kw_only=True)
class MailPreview:
    name: str
    subject: str
    html: Path | None
    text: Path | None


def render_preview(templates: MailTemplates, name: str, output: Path) -> MailPreview:
    rendered = templates.render(name, templates.sample_context(name))
    output.mkdir(parents=True, exist_ok=True)
    html = text = None
    if rendered.html is not None:
        page = rendered.html
        for image in rendered.inline_images:
            images = output / IMAGES_DIRECTORY
            images.mkdir(exist_ok=True)
            (images / image.filename).write_bytes(image.content)
            page = page.replace(f"cid:{image.content_id}", f"{IMAGES_DIRECTORY}/{image.filename}")
        html = output / f"{name}.html"
        html.write_text(page, encoding="utf-8")
    if rendered.text is not None:
        text = output / f"{name}.txt"
        text.write_text(f"Subject: {rendered.subject}\n\n{rendered.text}", encoding="utf-8")
    return MailPreview(name=name, subject=rendered.subject, html=html, text=text)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m persistence_kit.mail.preview",
        description="Render mail templates with their <name>.sample.json, without sending.",
    )
    parser.add_argument("directory", help="The application's template directory.")
    parser.add_argument("names", nargs="*", help="Templates to render. All of them when omitted.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Where to write the files.")
    parser.add_argument("--no-open", action="store_true", help="Do not open the HTML in the browser.")
    args = parser.parse_args(argv)

    try:
        templates = MailTemplates(args.directory)
    except MailTemplateError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    names = args.names or templates.names()
    if not names:
        print(f"error: no templates in {args.directory}", file=sys.stderr)
        return 1

    failed = False
    for name in names:
        try:
            preview = render_preview(templates, name, args.output)
        except MailTemplateError as exc:
            print(f"error: {exc}", file=sys.stderr)
            failed = True
            continue
        print(f"{preview.name}: {preview.subject}")
        for path in (preview.html, preview.text):
            if path is not None:
                print(f"  {path}")
        if preview.html is not None and not args.no_open:
            webbrowser.open(preview.html.resolve().as_uri())
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
