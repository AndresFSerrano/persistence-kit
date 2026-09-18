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
IMAGES_DIRECTORY = "images"
CID_SOURCE = re.compile(r"""\bsrc\s*=\s*["']cid:([^"']+)["']""", re.IGNORECASE)


@dataclass(frozen=True, kw_only=True)
class RenderedMail:
    subject: str
    html: str | None
    text: str | None
    inline_images: tuple[MailAttachment, ...] = ()


class MailTemplates:
    """Renders the templates of the application that sends; the kit ships none of its own.

    A template `name` is up to three files in `directory`: `name.subject.txt` (required) and
    `name.html` or `name.txt` (at least one). HTML is autoescaped, subject and text are not.
    An optional `name.sample.json` holds an example context for previews and tests.
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
        subject = self._render_file(f"{name}{SUBJECT_SUFFIX}", values)
        if subject is None:
            raise MailTemplateError(f"Template '{name}' has no {name}{SUBJECT_SUFFIX}.")
        html = self._render_file(f"{name}{HTML_SUFFIX}", values)
        text = self._render_file(f"{name}{TEXT_SUFFIX}", values)
        if html is None and text is None:
            raise MailTemplateError(
                f"Template '{name}' needs {name}{HTML_SUFFIX} or {name}{TEXT_SUFFIX}."
            )
        return RenderedMail(
            subject=" ".join(subject.split()),
            html=html,
            text=text,
            inline_images=self._inline_images(name, html),
        )

    def names(self) -> list[str]:
        """Every template in the directory, identified by its subject file."""
        return sorted(
            path.name.removesuffix(SUBJECT_SUFFIX)
            for path in self._directory.glob(f"*{SUBJECT_SUFFIX}")
        )

    def sample_context(self, name: str) -> dict[str, Any]:
        path = self._directory / f"{name}{SAMPLE_SUFFIX}"
        if not path.is_file():
            raise MailTemplateError(f"Template '{name}' has no {name}{SAMPLE_SUFFIX}.")
        try:
            sample = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise MailTemplateError(f"{path.name} is not valid JSON: {exc}") from exc
        if not isinstance(sample, dict):
            raise MailTemplateError(f"{path.name} must hold a JSON object.")
        return sample

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
