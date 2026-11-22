import math
import re
import zipfile
from collections.abc import Sequence
from datetime import date, datetime, tzinfo
from io import BytesIO
from xml.sax.saxutils import escape, quoteattr

from persistence_kit.excel.sheet import CellValue, ExcelSheet


EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

_ILLEGAL_XML_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]")
_FORBIDDEN_SHEET_NAME_CHARS = re.compile(r"[\[\]:*?/\\]")
_MAX_SHEET_NAME_LENGTH = 31

_BODY_STYLE = 0
_HEADER_STYLE = 1
_DATE_STYLE = 2
_DATETIME_STYLE = 3
_WRAPPED_TEXT_STYLE = 4

_EXCEL_EPOCH = datetime(1899, 12, 30)
_FIRST_EXACT_EXCEL_DATE = date(1900, 3, 1)
_SECONDS_PER_DAY = 86400

_DATE_TEXT_LENGTH = 10
_DATETIME_TEXT_LENGTH = 16
_BOOL_DISPLAY_TEXTS = ("TRUE", "FALSE", "VERDADERO", "FALSO", "VERDADEIRO")
_BOOL_TEXT_LENGTH = max(len(text) for text in _BOOL_DISPLAY_TEXTS)
_COLUMN_PADDING = 2
_MIN_COLUMN_WIDTH = 8
_MAX_COLUMN_WIDTH = 60

_XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

_WORKSHEET_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
)
_WORKSHEET_RELATIONSHIP_TYPE = f"{_REL_NS}/worksheet"
_STYLES_RELATIONSHIP_TYPE = f"{_REL_NS}/styles"

_CONTENT_TYPES_START = (
    _XML_DECLARATION
    + '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension="rels" '
    'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
    '<Default Extension="xml" ContentType="application/xml"/>'
    '<Override PartName="/xl/workbook.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
    '<Override PartName="/xl/styles.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
)

_ROOT_RELS = (
    _XML_DECLARATION
    + f'<Relationships xmlns="{_PACKAGE_RELS_NS}">'
    f'<Relationship Id="rId1" Type="{_REL_NS}/officeDocument" Target="xl/workbook.xml"/>'
    "</Relationships>"
)

_STYLES = (
    _XML_DECLARATION
    + f'<styleSheet xmlns="{_MAIN_NS}">'
    '<numFmts count="2">'
    '<numFmt numFmtId="164" formatCode="dd/mm/yyyy"/>'
    '<numFmt numFmtId="165" formatCode="dd/mm/yyyy hh:mm"/>'
    "</numFmts>"
    '<fonts count="2">'
    '<font><sz val="11"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>'
    "</fonts>"
    '<fills count="3">'
    '<fill><patternFill patternType="none"/></fill>'
    '<fill><patternFill patternType="gray125"/></fill>'
    '<fill><patternFill patternType="solid">'
    '<fgColor rgb="FF15803D"/><bgColor indexed="64"/>'
    "</patternFill></fill>"
    "</fills>"
    '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
    '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
    '<cellXfs count="5">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
    '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"/>'
    '<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
    '<xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
    '<alignment wrapText="1"/></xf>'
    "</cellXfs>"
    '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
    "</styleSheet>"
)


def _column_letter(index: int) -> str:
    letters = ""
    position = index + 1
    while position:
        position, remainder = divmod(position - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _validate_sheets(sheets: Sequence[ExcelSheet]) -> None:
    if not sheets:
        raise ValueError("A workbook needs at least one sheet")
    seen_names: set[str] = set()
    for sheet in sheets:
        if not sheet.name or len(sheet.name) > _MAX_SHEET_NAME_LENGTH:
            raise ValueError(
                f"Sheet name {sheet.name!r} must have 1 to {_MAX_SHEET_NAME_LENGTH} characters"
            )
        if _FORBIDDEN_SHEET_NAME_CHARS.search(sheet.name):
            raise ValueError(f"Sheet name {sheet.name!r} cannot contain [ ] : * ? / \\")
        if sheet.name.startswith("'") or sheet.name.endswith("'"):
            raise ValueError(f"Sheet name {sheet.name!r} cannot start or end with an apostrophe")
        if sheet.name.lower() in seen_names:
            raise ValueError(f"Sheet name {sheet.name!r} is repeated")
        seen_names.add(sheet.name.lower())
        if not sheet.headers:
            raise ValueError(f"Sheet {sheet.name!r} needs at least one header")
        if any(len(row) > len(sheet.headers) for row in sheet.rows):
            raise ValueError(f"Sheet {sheet.name!r} has a row with more cells than headers")


def _to_local(value: datetime, timezone: tzinfo | None) -> datetime:
    if value.tzinfo is None:
        return value
    if timezone is None:
        raise ValueError(f"Datetime {value.isoformat()} has a timezone; pass timezone=")
    return value.astimezone(timezone).replace(tzinfo=None)


def _serial(value: date | datetime) -> float:
    day = value.date() if isinstance(value, datetime) else value
    if day < _FIRST_EXACT_EXCEL_DATE:
        raise ValueError(
            f"Date {value.isoformat()} is before {_FIRST_EXACT_EXCEL_DATE.isoformat()}, "
            "which Excel shows wrong"
        )
    if isinstance(value, datetime):
        return (value - _EXCEL_EPOCH).total_seconds() / _SECONDS_PER_DAY
    return (value - _EXCEL_EPOCH.date()).days


def _text_cell(reference: str, text: str, style: int) -> str:
    safe_text = escape(_ILLEGAL_XML_CHARS.sub("", text))
    return (
        f'<c r="{reference}" s="{style}" t="inlineStr">'
        f'<is><t xml:space="preserve">{safe_text}</t></is></c>'
    )


def _number_cell(reference: str, number: float, style: int) -> str:
    return f'<c r="{reference}" s="{style}"><v>{number}</v></c>'


def _cell(reference: str, value: CellValue, timezone: tzinfo | None, text_style: int) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return f'<c r="{reference}" s="{_BODY_STYLE}" t="b"><v>{int(value)}</v></c>'
    if isinstance(value, int | float):
        if not math.isfinite(value):
            return _text_cell(reference, str(value), _BODY_STYLE)
        return _number_cell(reference, value, _BODY_STYLE)
    if isinstance(value, datetime):
        return _number_cell(reference, _serial(_to_local(value, timezone)), _DATETIME_STYLE)
    if isinstance(value, date):
        return _number_cell(reference, _serial(value), _DATE_STYLE)
    return _text_cell(reference, str(value), text_style)


def _display_length(value: CellValue) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return _BOOL_TEXT_LENGTH
    if isinstance(value, datetime):
        return _DATETIME_TEXT_LENGTH
    if isinstance(value, date):
        return _DATE_TEXT_LENGTH
    return len(str(value))


def _column_widths(sheet: ExcelSheet) -> list[int]:
    widths: list[int] = []
    for index, header in enumerate(sheet.headers):
        longest_cell = max(
            (_display_length(row[index]) for row in sheet.rows if index < len(row)),
            default=0,
        )
        content_width = max(len(header), longest_cell) + _COLUMN_PADDING
        widths.append(min(max(content_width, _MIN_COLUMN_WIDTH), _MAX_COLUMN_WIDTH))
    return widths


def _text_styles(widths: Sequence[int]) -> list[int]:
    return [
        _WRAPPED_TEXT_STYLE if width == _MAX_COLUMN_WIDTH else _BODY_STYLE for width in widths
    ]


def _row(
    number: int,
    values: Sequence[CellValue],
    timezone: tzinfo | None,
    text_styles: Sequence[int],
) -> str:
    cells = "".join(
        _cell(f"{_column_letter(index)}{number}", value, timezone, text_styles[index])
        for index, value in enumerate(values)
    )
    return f'<row r="{number}">{cells}</row>'


def _header_row(headers: Sequence[str]) -> str:
    cells = "".join(
        _text_cell(f"{_column_letter(index)}1", header, _HEADER_STYLE)
        for index, header in enumerate(headers)
    )
    return f'<row r="1">{cells}</row>'


def _last_column(sheet: ExcelSheet) -> str:
    return _column_letter(len(sheet.headers) - 1)


def _last_row(sheet: ExcelSheet) -> int:
    return len(sheet.rows) + 1


def _sheet_xml(sheet: ExcelSheet, timezone: tzinfo | None) -> str:
    data_range = f"A1:{_last_column(sheet)}{_last_row(sheet)}"
    widths = _column_widths(sheet)
    text_styles = _text_styles(widths)
    columns = "".join(
        f'<col min="{index + 1}" max="{index + 1}" width="{width}" customWidth="1"/>'
        for index, width in enumerate(widths)
    )
    body = _header_row(sheet.headers) + "".join(
        _row(number, values, timezone, text_styles)
        for number, values in enumerate(sheet.rows, start=2)
    )
    return (
        _XML_DECLARATION
        + f'<worksheet xmlns="{_MAIN_NS}" xmlns:r="{_REL_NS}">'
        f'<dimension ref="{data_range}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>'
        "</sheetView></sheetViews>"
        f"<cols>{columns}</cols>"
        f"<sheetData>{body}</sheetData>"
        f'<autoFilter ref="{data_range}"/>'
        "</worksheet>"
    )


def _filter_database(sheet: ExcelSheet, sheet_index: int) -> str:
    quoted_name = sheet.name.replace("'", "''")
    filter_range = f"'{quoted_name}'!$A$1:${_last_column(sheet)}${_last_row(sheet)}"
    return (
        f'<definedName name="_xlnm._FilterDatabase" localSheetId="{sheet_index}" hidden="1">'
        f"{escape(filter_range)}</definedName>"
    )


def _workbook_xml(sheets: Sequence[ExcelSheet]) -> str:
    sheet_entries = "".join(
        f'<sheet name={quoteattr(sheet.name)} sheetId="{index + 1}" r:id="rId{index + 1}"/>'
        for index, sheet in enumerate(sheets)
    )
    filter_entries = "".join(
        _filter_database(sheet, index) for index, sheet in enumerate(sheets)
    )
    return (
        _XML_DECLARATION
        + f'<workbook xmlns="{_MAIN_NS}" xmlns:r="{_REL_NS}">'
        f"<sheets>{sheet_entries}</sheets>"
        f"<definedNames>{filter_entries}</definedNames>"
        "</workbook>"
    )


def _workbook_rels_xml(sheet_count: int) -> str:
    sheet_relationships = "".join(
        f'<Relationship Id="rId{number}" Type="{_WORKSHEET_RELATIONSHIP_TYPE}" '
        f'Target="worksheets/sheet{number}.xml"/>'
        for number in range(1, sheet_count + 1)
    )
    return (
        _XML_DECLARATION
        + f'<Relationships xmlns="{_PACKAGE_RELS_NS}">'
        f"{sheet_relationships}"
        f'<Relationship Id="rId{sheet_count + 1}" Type="{_STYLES_RELATIONSHIP_TYPE}" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )


def _content_types_xml(sheet_count: int) -> str:
    sheet_overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{number}.xml" '
        f'ContentType="{_WORKSHEET_CONTENT_TYPE}"/>'
        for number in range(1, sheet_count + 1)
    )
    return f"{_CONTENT_TYPES_START}{sheet_overrides}</Types>"


def build_xlsx(*, sheets: Sequence[ExcelSheet], timezone: tzinfo | None = None) -> bytes:
    _validate_sheets(sheets)
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", _content_types_xml(len(sheets)))
        archive.writestr("_rels/.rels", _ROOT_RELS)
        archive.writestr("xl/workbook.xml", _workbook_xml(sheets))
        archive.writestr("xl/_rels/workbook.xml.rels", _workbook_rels_xml(len(sheets)))
        archive.writestr("xl/styles.xml", _STYLES)
        for number, sheet in enumerate(sheets, start=1):
            archive.writestr(f"xl/worksheets/sheet{number}.xml", _sheet_xml(sheet, timezone))
    return buffer.getvalue()
