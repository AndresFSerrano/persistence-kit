from __future__ import annotations

import json
import mimetypes
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import (
    Environment,
    FileSystemLoader,
    StrictUndefined,
    TemplateError,
    TemplateNotFound,
    select_autoescape,
)

from persistence_kit.mail.errors import MailTemplateError
from persistence_kit.mail.message import CONTENT_ID_PATTERN, MailAttachment

SUBJECT_SUFFIX = ".subject.txt"
HTML_SUFFIX = ".html"
TEXT_SUFFIX = ".txt"
SAMPLE_SUFFIX = ".sample.json"
FOLDER_SUBJECT = "subject.txt"
FOLDER_HTML = "body.html"
FOLDER_TEXT = "body.txt"
FOLDER_SAMPLE = "sample.json"
IMAGES_DIRECTORY = "images"
CID_SOURCE = re.compile(r"""\bsrc\s*=\s*["']cid:([^"']+)["']""", re.IGNORECASE)


@dataclass(frozen=True, kw_only=True)
class RenderedMail:
    subject: str
    html: str | None
    text: str | None
    inline_images: tuple[MailAttachment, ...] = ()


@dataclass(frozen=True, kw_only=True)
class _TemplateFiles:
    subject: str
    html: str
    text: str
    sample: str


class MailTemplates:
    """Renders the templates of the application that sends; the kit ships none of its own.

    A template `name` is a folder `name/` with `subject.txt` (required) and `body.html` or
    `body.txt` (at least one), or the same files loose in `directory` as `name.subject.txt`,
    `name.html` and `name.txt`. HTML is autoescaped, subject and text are not. An optional
    `sample.json` (`name.sample.json` when loose) holds an example context for previews and tests.
    Every `src="cid:file.png"` in the HTML embeds `images/file.png` in the message.
    """

    def __init__(self, directory: str | Path) -> None:
        path = Path(directory)
        if not path.is_dir():
            raise MailTemplateError(f"The template directory does not exist: {path}")
        self._directory = path
        self._environment = Environment(
            loader=FileSystemLoader(str(path)),
            autoescape=select_autoescape(enabled_extensions=("html",), default_for_string=False),
            undefined=StrictUndefined,
        )

    def render(self, name: str, context: Mapping[str, Any] | None = None) -> RenderedMail:
        values = dict(context or {})
        files = self._files(name)
        subject = self._render_file(files.subject, values)
        if subject is None:
            raise MailTemplateError(f"Template '{name}' has no {files.subject}.")
        html = self._render_file(files.html, values)
        text = self._render_file(files.text, values)
        if html is None and text is None:
            raise MailTemplateError(f"Template '{name}' needs {files.html} or {files.text}.")
        return RenderedMail(
            subject=" ".join(subject.split()),
            html=html,
            text=text,
            inline_images=self._inline_images(name, html),
        )

    def names(self) -> list[str]:
        """Every template in the directory, identified by its subject file."""
        loose = (
            path.name.removesuffix(SUBJECT_SUFFIX)
            for path in self._directory.glob(f"*{SUBJECT_SUFFIX}")
        )
        folders = (path.parent.name for path in self._directory.glob(f"*/{FOLDER_SUBJECT}"))
        return sorted({*loose, *folders})

    def sample_context(self, name: str) -> dict[str, Any]:
        relative = self._files(name).sample
        path = self._directory / relative
        if not path.is_file():
            raise MailTemplateError(f"Template '{name}' has no {relative}.")
        try:
            sample = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise MailTemplateError(f"{relative} is not valid JSON: {exc}") from exc
        if not isinstance(sample, dict):
            raise MailTemplateError(f"{relative} must hold a JSON object.")
        return sample

    def _files(self, name: str) -> _TemplateFiles:
        if (self._directory / name).is_dir():
            return _TemplateFiles(
                subject=f"{name}/{FOLDER_SUBJECT}",
                html=f"{name}/{FOLDER_HTML}",
                text=f"{name}/{FOLDER_TEXT}",
                sample=f"{name}/{FOLDER_SAMPLE}",
            )
        return _TemplateFiles(
            subject=f"{name}{SUBJECT_SUFFIX}",
            html=f"{name}{HTML_SUFFIX}",
            text=f"{name}{TEXT_SUFFIX}",
            sample=f"{name}{SAMPLE_SUFFIX}",
        )

    def _inline_images(self, name: str, html: str | None) -> tuple[MailAttachment, ...]:
        if html is None:
            return ()
        images: list[MailAttachment] = []
        for filename in dict.fromkeys(CID_SOURCE.findall(html)):
            images.append(self._inline_image(name, filename))
        return tuple(images)

    def _inline_image(self, name: str, filename: str) -> MailAttachment:
        reference = f"Template '{name}' references cid:{filename}"
        if not CONTENT_ID_PATTERN.fullmatch(filename):
            raise MailTemplateError(f"{reference}, which is not a plain file name.")
        path = self._directory / IMAGES_DIRECTORY / filename
        if not path.is_file():
            raise MailTemplateError(f"{reference}, but {IMAGES_DIRECTORY}/{filename} does not exist.")
        content_type, _ = mimetypes.guess_type(filename)
        if content_type is None or not content_type.startswith("image/"):
            raise MailTemplateError(f"{reference}, which is not an image.")
        return MailAttachment(
            filename=filename,
            content=path.read_bytes(),
            content_type=content_type,
            content_id=filename,
        )

    def _render_file(self, filename: str, values: dict[str, Any]) -> str | None:
        try:
            template = self._environment.get_template(filename)
        except TemplateNotFound as exc:
            if exc.name == filename:
                return None
            raise MailTemplateError(f"Could not load {filename}: {exc}") from exc
        except TemplateError as exc:
            raise MailTemplateError(f"Could not load {filename}: {exc}") from exc
        try:
            return template.render(values)
        except TemplateError as exc:
            raise MailTemplateError(f"Could not render {filename}: {exc}") from exc
