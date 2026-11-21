"""
Report DOCX Exporter
====================
Project: News Article Analysis and Topic Extraction (Big Data Architecture)
Purpose: Converts the master Markdown report (`PROJECT_REPORT.md`) into a
         submission-ready Word document (`PROJECT_REPORT.docx`).

Design notes
------------
- The original Markdown file is NEVER modified; this script only reads it and
  writes a *separate* .docx alongside it (a converted copy).
- A focused Markdown parser handles exactly the constructs used in the report:
  ATX headings, pipe tables, fenced code blocks, bullet/numbered lists,
  blockquotes, horizontal rules and inline **bold** / *italic* / `code`.
- All 12 publication charts are appended as an embedded "Visualization Gallery".

Run:
    python src/export_report_docx.py
"""

import os
import re
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
SOURCE_MD = "PROJECT_REPORT.md"
OUTPUT_DOCX = "PROJECT_REPORT.docx"
VISUALIZATION_DIR = "visualizations"

# Charts embedded, in presentation order, into the appendix gallery.
GALLERY_CHARTS = [
    ("eda_overview.png", "Figure 1 — Exploratory Data Analysis Overview"),
    ("topic_volume_distribution.png", "Figure 2 — Discovered Topic Volume Distribution"),
    ("lda_topic_top_terms.png", "Figure 3 — LDA per-Topic Top Term Weights"),
    ("publisher_topic_heatmap.png", "Figure 4 — Publisher x Topic Editorial Cross-Tabulation"),
    ("temporal_topic_trends.png", "Figure 5 — Monthly Topic Trajectory Trends"),
    ("topic_sentiment_distribution.png", "Figure 6 — VADER Sentiment Distribution by Topic"),
    ("sentiment_analysis.png", "Figure 7 — Corpus-Wide Sentiment Breakdown"),
    ("topic_wordclouds_grid.png", "Figure 8 — Topic Vocabulary Word Clouds"),
    ("mapreduce_term_analysis.png", "Figure 9 — MapReduce Unigram / Bigram Frequencies"),
    ("notebook_publisher_heatmap.png", "Figure 10 — Notebook Publisher Editorial Heatmap"),
    ("notebook_temporal_trends.png", "Figure 11 — Notebook Temporal Trend Analysis"),
    ("notebook_wordclouds.png", "Figure 12 — Notebook Semantic Word Clouds"),
]

INLINE_PATTERN = re.compile(r"(\*\*.+?\*\*|`[^`]+`|\*[^*]+?\*|\[[^\]]+\]\([^)]+\))")


# --------------------------------------------------------------------------
# Inline formatting
# --------------------------------------------------------------------------
def add_inline(paragraph, text):
    """Writes `text` into `paragraph`, applying bold / italic / code runs."""
    for token in INLINE_PATTERN.split(text):
        if not token:
            continue
        if token.startswith("**") and token.endswith("**"):
            run = paragraph.add_run(token[2:-2])
            run.bold = True
        elif token.startswith("`") and token.endswith("`"):
            run = paragraph.add_run(token[1:-1])
            run.font.name = "Consolas"
            run.font.size = Pt(9.5)
            run.font.color.rgb = RGBColor(0xB0, 0x30, 0x60)
        elif token.startswith("[") and "](" in token:
            label = token[1:token.index("](")]
            run = paragraph.add_run(label)
            run.italic = True
            run.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
        elif token.startswith("*") and token.endswith("*") and len(token) > 2:
            run = paragraph.add_run(token[1:-1])
            run.italic = True
        else:
            paragraph.add_run(token)


def add_code_block(doc, lines):
    """Adds a shaded monospace code block."""
    paragraph = doc.add_paragraph()
    paragraph.paragraph_format.left_indent = Inches(0.2)
    paragraph.paragraph_format.space_before = Pt(4)
    paragraph.paragraph_format.space_after = Pt(8)
    run = paragraph.add_run("\n".join(lines))
    run.font.name = "Consolas"
    run.font.size = Pt(8.5)
    run.font.color.rgb = RGBColor(0x24, 0x29, 0x2E)
    return paragraph


def add_table(doc, rows):
    """Renders a markdown pipe table (rows = list of cell-lists) as a Word table."""
    header, body = rows[0], rows[1:]
    table = doc.add_table(rows=1, cols=len(header))
    table.style = "Light Grid Accent 1"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    for index, cell_text in enumerate(header):
        cell = table.rows[0].cells[index]
        cell.text = ""
        add_inline(cell.paragraphs[0], cell_text)
        for run in cell.paragraphs[0].runs:
            run.bold = True
    for row in body:
        cells = table.add_row().cells
        for index, cell_text in enumerate(row[: len(header)]):
            cells[index].text = ""
            add_inline(cells[index].paragraphs[0], cell_text)
    doc.add_paragraph()
    return table


def split_table_row(line):
    """Splits a pipe-table row into trimmed cells (drops the outer pipes)."""
    stripped = line.strip().strip("|")
    return [cell.strip() for cell in stripped.split("|")]


def is_separator_row(line):
    """True for markdown table separator rows like | :--- | ---: |."""
    return bool(re.fullmatch(r"\|[\s:|-]+\|", line.strip())) and "-" in line


# --------------------------------------------------------------------------
# Main conversion
# --------------------------------------------------------------------------
def convert(md_path, docx_path):
    with open(md_path, "r", encoding="utf-8") as handle:
        lines = handle.read().splitlines()

    doc = Document()
    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(10.5)

    index = 0
    in_code = False
    code_buffer = []
    table_buffer = []

    def flush_table():
        if table_buffer:
            add_table(doc, [split_table_row(row) for row in table_buffer])
            table_buffer.clear()

    while index < len(lines):
        line = lines[index]
        stripped = line.strip()

        # --- Fenced code blocks -------------------------------------------
        if stripped.startswith("```"):
            if in_code:
                add_code_block(doc, code_buffer)
                code_buffer.clear()
                in_code = False
            else:
                flush_table()
                in_code = True
            index += 1
            continue
        if in_code:
            code_buffer.append(line)
            index += 1
            continue

        # --- Pipe tables ---------------------------------------------------
        if stripped.startswith("|") and stripped.endswith("|"):
            if is_separator_row(stripped):
                index += 1
                continue
            table_buffer.append(stripped)
            index += 1
            # Only flush once the table block ends.
            if index >= len(lines) or not lines[index].strip().startswith("|"):
                flush_table()
            continue
        flush_table()

        # --- Blank line ----------------------------------------------------
        if not stripped:
            index += 1
            continue

        # --- Headings ------------------------------------------------------
        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            level = len(heading.group(1))
            text = heading.group(2)
            if level == 1:
                paragraph = doc.add_heading("", level=0)
            else:
                paragraph = doc.add_heading("", level=min(level - 1, 4))
            add_inline(paragraph, text)
            index += 1
            continue

        # --- Horizontal rule ----------------------------------------------
        if re.fullmatch(r"-{3,}|\*{3,}|_{3,}", stripped):
            index += 1
            continue

        # --- Blockquotes ---------------------------------------------------
        if stripped.startswith(">"):
            text = stripped.lstrip(">").strip()
            if text.startswith("[!"):
                index += 1
                continue
            paragraph = doc.add_paragraph()
            paragraph.paragraph_format.left_indent = Inches(0.3)
            add_inline(paragraph, text)
            for run in paragraph.runs:
                run.italic = True
            index += 1
            continue

        # --- Bullet / numbered lists --------------------------------------
        bullet = re.match(r"^[-*]\s+(.*)$", stripped)
        numbered = re.match(r"^\d+\.\s+(.*)$", stripped)
        if bullet:
            add_inline(doc.add_paragraph(style="List Bullet"), bullet.group(1))
            index += 1
            continue
        if numbered:
            add_inline(doc.add_paragraph(style="List Number"), numbered.group(1))
            index += 1
            continue

        # --- Regular paragraph --------------------------------------------
        add_inline(doc.add_paragraph(), stripped)
        index += 1

    flush_table()

    # ----------------------------------------------------------------------
    # Appendix: embedded visualization gallery
    # ----------------------------------------------------------------------
    doc.add_page_break()
    doc.add_heading("Appendix: Visualization Gallery", level=1)
    add_inline(
        doc.add_paragraph(),
        "All twelve 300-DPI analysis charts are embedded below. The *interactive* "
        "version of these dashboards (hover, zoom, filter) is available as a "
        "self-contained HTML file (`dashboard/interactive_dashboard.html`) and as "
        "the live Streamlit app (`src/dashboard.py`).",
    )
    for filename, caption in GALLERY_CHARTS:
        image_path = os.path.join(VISUALIZATION_DIR, filename)
        if not os.path.exists(image_path):
            continue
        add_inline(doc.add_paragraph(), f"**{caption}**")
        doc.add_picture(image_path, width=Inches(6.2))
        doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
        doc.add_paragraph()

    doc.save(docx_path)
    return docx_path


if __name__ == "__main__":
    output = convert(SOURCE_MD, OUTPUT_DOCX)
    print(f"[OK] Converted '{SOURCE_MD}' -> '{output}' (original left untouched)")
