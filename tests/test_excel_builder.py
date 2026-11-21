import math
import zipfile
from datetime import date, datetime
from io import BytesIO
from zoneinfo import ZoneInfo

import pytest

from persistence_kit.excel import EXCEL_MEDIA_TYPE, CellValue, ExcelSheet, build_xlsx

BOGOTA = ZoneInfo("America/Bogota")


def _part(content: bytes, name: str) -> str:
    with zipfile.ZipFile(BytesIO(content)) as archive:
        return archive.read(name).decode()


def _single_sheet(rows: list[list[CellValue]]) -> ExcelSheet:
    return ExcelSheet(name="Datos", headers=("Valor",), rows=rows)


def _first_sheet_xml(rows: list[list[CellValue]], timezone: ZoneInfo | None = None) -> str:
    content = build_xlsx(sheets=[_single_sheet(rows)], timezone=timezone)
    return _part(content, "xl/worksheets/sheet1.xml")


def test_the_media_type_is_the_xlsx_one():
    assert EXCEL_MEDIA_TYPE == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def test_the_workbook_is_a_zip_with_every_part():
    content = build_xlsx(sheets=[_single_sheet([["Ana"]])])

    with zipfile.ZipFile(BytesIO(content)) as archive:
        names = archive.namelist()

    assert content.startswith(b"PK")
    assert names == [
        "[Content_Types].xml",
        "_rels/.rels",
        "xl/workbook.xml",
        "xl/_rels/workbook.xml.rels",
        "xl/styles.xml",
        "xl/worksheets/sheet1.xml",
    ]


def test_each_sheet_gets_its_own_part_relationship_and_filter():
    content = build_xlsx(
        sheets=[
            ExcelSheet(name="Física", headers=("Nombre",), rows=[["Ana"], ["Luis"]]),
            ExcelSheet(name="Química", headers=("Nombre", "Ciudad"), rows=[]),
        ]
    )

    workbook = _part(content, "xl/workbook.xml")
    relationships = _part(content, "xl/_rels/workbook.xml.rels")
    content_types = _part(content, "[Content_Types].xml")

    assert '<sheet name="Física" sheetId="1" r:id="rId1"/>' in workbook
    assert '<sheet name="Química" sheetId="2" r:id="rId2"/>' in workbook
    assert "localSheetId=\"0\" hidden=\"1\">'Física'!$A$1:$A$3</definedName>" in workbook
    assert "localSheetId=\"1\" hidden=\"1\">'Química'!$A$1:$B$1</definedName>" in workbook
    assert 'Id="rId1"' in relationships and 'Target="worksheets/sheet1.xml"' in relationships
    assert 'Id="rId2"' in relationships and 'Target="worksheets/sheet2.xml"' in relationships
    assert 'Id="rId3"' in relationships and 'Target="styles.xml"' in relationships
    assert 'PartName="/xl/worksheets/sheet1.xml"' in content_types
    assert 'PartName="/xl/worksheets/sheet2.xml"' in content_types
    assert "<v>" not in _part(content, "xl/worksheets/sheet2.xml")


def test_the_sheet_name_is_escaped_in_the_workbook():
    content = build_xlsx(sheets=[ExcelSheet(name="O'Brien & \"Co\"", headers=("A",), rows=[])])

    workbook = _part(content, "xl/workbook.xml")

    assert '<sheet name="O\'Brien &amp; &quot;Co&quot;" sheetId="1"' in workbook
    assert "'O''Brien &amp; \"Co\"'!$A$1:$A$1" in workbook


def test_the_header_row_uses_the_header_style():
    sheet_xml = _first_sheet_xml([])

    assert '<row r="1"><c r="A1" s="1" t="inlineStr">' in sheet_xml
    assert ">Valor</t>" in sheet_xml


def test_a_date_is_an_excel_serial_with_the_date_style():
    sheet_xml = _first_sheet_xml([[date(2026, 9, 29)]])

    assert '<c r="A2" s="2"><v>46294</v></c>' in sheet_xml


def test_a_naive_datetime_keeps_its_clock_time():
    sheet_xml = _first_sheet_xml([[datetime(2026, 9, 29, 12, 0)]])

    assert '<c r="A2" s="3"><v>46294.5</v></c>' in sheet_xml


def test_an_aware_datetime_moves_to_the_given_timezone():
    utc_value = datetime.fromisoformat("2026-09-30T02:00:00+00:00")

    sheet_xml = _first_sheet_xml([[utc_value]], timezone=BOGOTA)

    assert '<c r="A2" s="3"><v>46294.875</v></c>' in sheet_xml


def test_an_aware_datetime_without_timezone_is_rejected():
    sheet = _single_sheet([[datetime.fromisoformat("2026-09-29T14:12:31+00:00")]])

    with pytest.raises(ValueError, match="pass timezone="):
        build_xlsx(sheets=[sheet])


def test_a_bool_is_a_boolean_cell():
    sheet_xml = _first_sheet_xml([[True], [False]])

    assert '<c r="A2" s="0" t="b"><v>1</v></c>' in sheet_xml
    assert '<c r="A3" s="0" t="b"><v>0</v></c>' in sheet_xml


def test_numbers_are_numeric_cells():
    sheet_xml = _first_sheet_xml([[12], [3.5]])

    assert '<c r="A2" s="0"><v>12</v></c>' in sheet_xml
    assert '<c r="A3" s="0"><v>3.5</v></c>' in sheet_xml


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_a_number_excel_cannot_store_is_written_as_text(value: float):
    sheet_xml = _first_sheet_xml([[value]])

    assert '<c r="A2" s="0" t="inlineStr">' in sheet_xml
    assert "<v>" not in sheet_xml


def test_none_leaves_the_cell_out():
    sheet_xml = _first_sheet_xml([[None]])

    assert '<row r="2"></row>' in sheet_xml


def test_text_is_escaped_and_loses_the_characters_xml_rejects():
    sheet_xml = _first_sheet_xml([["Ana\x0bMaría <R&D>\tdos"]])

    assert ">AnaMaría &lt;R&amp;D&gt;\tdos</t>" in sheet_xml


def test_a_row_shorter_than_the_headers_is_accepted():
    sheet = ExcelSheet(name="Datos", headers=("Nombre", "Ciudad"), rows=[["Ana"]])

    sheet_xml = _part(build_xlsx(sheets=[sheet]), "xl/worksheets/sheet1.xml")

    assert '<row r="2"><c r="A2" s="0" t="inlineStr">' in sheet_xml
    assert 'r="B2"' not in sheet_xml


def test_the_column_width_follows_the_longest_content():
    sheet = ExcelSheet(
        name="Datos",
        headers=("Identificador SIGA anterior", "Id", "Lugar", "Inicio", "Solicitud"),
        rows=[[1, 2, "x" * 200, date(2026, 9, 29), datetime(2026, 9, 29, 9, 12)]],
    )

    sheet_xml = _part(build_xlsx(sheets=[sheet]), "xl/worksheets/sheet1.xml")

    assert '<col min="1" max="1" width="29" customWidth="1"/>' in sheet_xml
    assert '<col min="2" max="2" width="8" customWidth="1"/>' in sheet_xml
    assert '<col min="3" max="3" width="60" customWidth="1"/>' in sheet_xml
    assert '<col min="4" max="4" width="12" customWidth="1"/>' in sheet_xml
    assert '<col min="5" max="5" width="18" customWidth="1"/>' in sheet_xml


def test_the_header_row_is_frozen_and_filtered():
    sheet_xml = _first_sheet_xml([["Ana"], ["Luis"]])

    assert '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>' in sheet_xml
    assert '<dimension ref="A1:A3"/>' in sheet_xml
    assert '<autoFilter ref="A1:A3"/>' in sheet_xml


def test_the_styles_define_the_date_formats():
    styles = _part(build_xlsx(sheets=[_single_sheet([])]), "xl/styles.xml")

    assert '<numFmt numFmtId="164" formatCode="dd/mm/yyyy"/>' in styles
    assert '<numFmt numFmtId="165" formatCode="dd/mm/yyyy hh:mm"/>' in styles
    assert '<cellXfs count="4">' in styles


def test_a_workbook_needs_a_sheet():
    with pytest.raises(ValueError, match="at least one sheet"):
        build_xlsx(sheets=[])


@pytest.mark.parametrize(
    "name",
    ["", "x" * 32, "Física/Química", "Física: 2026", "Hoja?", "Hoja*", "[Hoja]", "Hoja\\1"],
)
def test_an_invalid_sheet_name_is_rejected(name: str):
    sheet = ExcelSheet(name=name, headers=("A",), rows=[])

    with pytest.raises(ValueError, match="Sheet name"):
        build_xlsx(sheets=[sheet])


def test_a_sheet_name_of_31_characters_is_accepted():
    content = build_xlsx(sheets=[ExcelSheet(name="x" * 31, headers=("A",), rows=[])])

    assert content.startswith(b"PK")


def test_sheet_names_are_compared_without_case():
    sheets = [
        ExcelSheet(name="Física", headers=("A",), rows=[]),
        ExcelSheet(name="física", headers=("A",), rows=[]),
    ]

    with pytest.raises(ValueError, match="is repeated"):
        build_xlsx(sheets=sheets)


def test_a_sheet_needs_a_header():
    sheet = ExcelSheet(name="Datos", headers=(), rows=[])

    with pytest.raises(ValueError, match="at least one header"):
        build_xlsx(sheets=[sheet])


def test_a_row_longer_than_the_headers_is_rejected():
    sheet = ExcelSheet(name="Datos", headers=("Nombre",), rows=[["Ana", "Medellín"]])

    with pytest.raises(ValueError, match="more cells than headers"):
        build_xlsx(sheets=[sheet])
