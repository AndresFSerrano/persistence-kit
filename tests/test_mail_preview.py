import json

import pytest

from persistence_kit.mail import MailTemplateError
from persistence_kit.mail import preview
from persistence_kit.mail.templates import MailTemplates


@pytest.fixture
def templates_dir(tmp_path):
    directory = tmp_path / "mail"
    directory.mkdir()
    (directory / "layout.html").write_text(
        "<html><body>{% block content %}{% endblock %}</body></html>", encoding="utf-8"
    )
    (directory / "user_created.subject.txt").write_text("Hola {{ name }}", encoding="utf-8")
    (directory / "user_created.html").write_text(
        '{% extends "layout.html" %}{% block content %}<p>{{ name }}</p>{% endblock %}',
        encoding="utf-8",
    )
    (directory / "user_created.txt").write_text("Hola {{ name }}", encoding="utf-8")
    (directory / "user_created.sample.json").write_text(
        json.dumps({"name": "<Juan>"}), encoding="utf-8"
    )
    (directory / "user_deleted.subject.txt").write_text("Chao", encoding="utf-8")
    (directory / "user_deleted.txt").write_text("Chao {{ name }}", encoding="utf-8")
    return directory


@pytest.fixture
def opened(monkeypatch):
    uris = []
    monkeypatch.setattr(preview.webbrowser, "open", uris.append)
    return uris


def test_names_lists_every_template_by_its_subject_file(templates_dir):
    assert MailTemplates(templates_dir).names() == ["user_created", "user_deleted"]


def test_sample_context_reads_the_json_next_to_the_template(templates_dir):
    assert MailTemplates(templates_dir).sample_context("user_created") == {"name": "<Juan>"}


def test_a_template_without_sample_has_no_sample_context(templates_dir):
    with pytest.raises(MailTemplateError, match="user_deleted.sample.json"):
        MailTemplates(templates_dir).sample_context("user_deleted")


@pytest.mark.parametrize("content", ["{not json", "[1, 2]"])
def test_a_sample_has_to_be_a_json_object(templates_dir, content):
    (templates_dir / "user_created.sample.json").write_text(content, encoding="utf-8")

    with pytest.raises(MailTemplateError, match="user_created.sample.json"):
        MailTemplates(templates_dir).sample_context("user_created")


def test_preview_writes_html_and_text_and_opens_the_html(templates_dir, tmp_path, opened, capsys):
    output = tmp_path / "out"

    exit_code = preview.main([str(templates_dir), "user_created", "--output", str(output)])

    assert exit_code == 0
    assert (output / "user_created.html").read_text(encoding="utf-8") == (
        "<html><body><p>&lt;Juan&gt;</p></body></html>"
    )
    assert (output / "user_created.txt").read_text(encoding="utf-8") == (
        "Subject: Hola <Juan>\n\nHola <Juan>"
    )
    assert opened == [(output / "user_created.html").resolve().as_uri()]
    assert "user_created: Hola <Juan>" in capsys.readouterr().out


def test_preview_copies_cid_images_next_to_the_html_and_points_to_them(templates_dir, tmp_path, opened):
    (templates_dir / "images").mkdir()
    (templates_dir / "images" / "key.png").write_bytes(b"\x89PNG-key")
    (templates_dir / "layout.html").write_text(
        '<html><body><img src="cid:key.png">{% block content %}{% endblock %}</body></html>',
        encoding="utf-8",
    )
    output = tmp_path / "out"

    exit_code = preview.main([str(templates_dir), "user_created", "--output", str(output), "--no-open"])

    assert exit_code == 0
    assert (output / "images" / "key.png").read_bytes() == b"\x89PNG-key"
    assert '<img src="images/key.png">' in (output / "user_created.html").read_text(encoding="utf-8")


def test_preview_does_not_open_the_browser_with_no_open(templates_dir, tmp_path, opened):
    exit_code = preview.main(
        [str(templates_dir), "user_created", "--output", str(tmp_path / "out"), "--no-open"]
    )

    assert exit_code == 0
    assert opened == []


def test_preview_renders_all_templates_and_fails_on_the_one_without_sample(
    templates_dir, tmp_path, opened, capsys
):
    output = tmp_path / "out"

    exit_code = preview.main([str(templates_dir), "--output", str(output), "--no-open"])

    assert exit_code == 1
    assert (output / "user_created.html").exists()
    assert not (output / "user_deleted.txt").exists()
    assert "user_deleted.sample.json" in capsys.readouterr().err


def test_preview_fails_on_a_missing_directory(tmp_path, capsys):
    assert preview.main([str(tmp_path / "missing"), "--no-open"]) == 1
    assert "does not exist" in capsys.readouterr().err


def test_preview_fails_on_a_directory_without_templates(tmp_path, capsys):
    assert preview.main([str(tmp_path), "--no-open"]) == 1
    assert "no templates" in capsys.readouterr().err
