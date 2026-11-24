#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Figures of the observation usage dashboard (obs_usage_dashboard.py): one
stop-light figure per observation group (satellite radiance, ozone,
conventional) and dashboard page. A group too long for one page is split into
parts; conventional parts keep each variable (ps, t, q, uv, gps) together
where it fits.

Sizes follow the AMS journal figure guidelines:
- the width is a standard AMS width: 3.2, 4.5, 5.5 or 6.5 in (19, 27, 33 or 39 picas);
- text is drawn at its printed size, 7 pt by default with 1-2 pt larger titles,
  in the same font in every figure;
- a figure is at most 9 in deep.
Rasters default to 300 dpi. For a journal submission, use PDF (vector, fonts
embedded) or 600 dpi.

Branding follows CIRES first, then CU Boulder:
- the CIRES typeface Inter if it is installed or its static .ttf files are in
  --font-dir; otherwise Helvetica Neue, Helvetica or Arial (CU Boulder's
  typeface and its substitute, which are also AMS standard fonts);
- CIRES dark blue titles;
- CU Boulder grays for text and rules;
- CU Gold and CIRES turquoise source strips.
The status colors stay the Okabe & Ito set: brand colors such as CIRES
turquoise come too close to them under simulated color-vision deficiencies.
"""

import math
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

from obs_usage_status import CONVENTIONAL, CONVENTIONAL_VARIABLES, GROUPS, OZONE, RADIANCE

AMS_WIDTHS = (3.2, 4.5, 5.5, 6.5)  # in: one column, 2/3 page, two columns, full page
MAX_HEIGHT = 9.0  # in
FONTS = ('Inter', 'Helvetica Neue', 'Helvetica', 'Arial', 'Liberation Sans', 'DejaVu Sans')
FORMATS = ('png', 'pdf', 'svg', 'eps', 'tif')
TITLE_COLOR = '#333666'  # CIRES dark blue
TEXT_COLOR = '#565A5C'  # CU Boulder dark gray (7:1 contrast on white)
RULE_COLOR = '#A2A4A3'  # CU Boulder light gray
GRID_COLOR = '#D0D1D1'  # CU Boulder light gray, 50% tint
# source strips: none, CU Gold, CIRES turquoise (50% tint), mixed (CU light gray)
SOURCE_COLORS = ['#FFFFFF', '#CFB87C', '#99CCE5', '#A2A4A3']
GROUP_FILES = {RADIANCE: 'radiance', OZONE: 'ozone', CONVENTIONAL: 'conventional'}
MARK = '\u25c6'
LINE_SPACING = 1.2
MARGIN, LABEL_GAP, SWATCH, SWATCH_GAP = 0.08, 0.05, 0.1, 0.06  # in
GAP = 0.4  # rows between conventional variables
TITLE_GAP = 0.03  # in


class FigureSizeError(ValueError):
    """The text size leaves no room for the chart within MAX_HEIGHT."""


@dataclass
class FigureStyle:
    """Width (in), text size (pt), raster resolution, formats and fonts
    (preferred first; later ones supply missing glyphs)."""
    width: float = 6.5
    size: float = 7.0
    dpi: int = 300
    formats: tuple = ('png',)
    fonts: list = field(default_factory=lambda: ['DejaVu Sans'])
    _widths: dict = field(default_factory=dict, repr=False)
    _heights: dict = field(default_factory=dict, repr=False)

    @property
    def row_height(self):
        return self.size * 1.25 / 72

    def lines_height(self, count, size=None, weight='normal'):
        """Height in inches of ``count`` lines of text as matplotlib lays them
        out. The line pitch depends on the font's ascent and descent: at
        LINE_SPACING it is 1.2 em for DejaVu Sans but 1.45 em for Inter."""
        if not count:
            return 0.0
        key = (count, size or self.size, weight)
        if key not in self._heights:
            from matplotlib.backends.backend_agg import FigureCanvasAgg
            from matplotlib.figure import Figure
            from matplotlib.font_manager import FontProperties
            figure = Figure(dpi=self.dpi)
            text = figure.text(0, 0, '\n'.join(['lp(|)'] * count), linespacing=LINE_SPACING,
                               fontproperties=FontProperties(family=self.fonts, weight=weight,
                                                             size=key[1]))
            extent = text.get_window_extent(FigureCanvasAgg(figure).get_renderer())
            self._heights[key] = extent.height / self.dpi
        return self._heights[key]

    def text_width(self, text, size=None, weight='normal'):
        """Width of ``text`` in inches, from the FreeType metrics matplotlib
        aligns text with (20x faster than measuring glyph outlines)."""
        key = (text, size or self.size, weight)
        if key not in self._widths:
            from matplotlib.font_manager import FontProperties
            from matplotlib.textpath import text_to_path
            prop = FontProperties(family=self.fonts, weight=weight, size=key[1])
            self._widths[key] = text_to_path.get_text_width_height_descent(
                text, prop, ismath=False)[0] / 72 if text.strip() else 0.0
        return self._widths[key]

    def fit(self, text, width, size=None, keep='start', weight='normal'):
        """``text`` cut with an ellipsis to ``width`` inches ('' if nothing
        fits), keeping its start or its end."""
        if self.text_width(text, size, weight) <= width:
            return text
        for cut in range(1, len(text)):
            short = (text[:len(text) - cut] + '\u2026' if keep == 'start'
                     else '\u2026' + text[cut:])
            if self.text_width(short, size, weight) <= width:
                return short
        return ''

    def wrap(self, parts, width, size=None, sep=' ', weight='normal'):
        """Join ``parts`` with ``sep`` into lines of at most ``width`` inches.
        A part wider than a line is broken at its spaces, and a word wider
        than a line is cut with an ellipsis."""
        lines = []
        for part in parts:
            if lines and self.text_width(lines[-1] + sep + part, size, weight) <= width:
                lines[-1] += sep + part
            elif self.text_width(part, size, weight) <= width:
                lines.append(part)
            elif len(part.split()) > 1:
                lines += self.wrap(part.split(), width, size, ' ', weight)
            else:
                lines.append(self.fit(part, width, size, weight=weight))
        return lines


def figure_style(width=6.5, size=7.0, dpi=300, formats=('png',), font=None, font_dir=None):
    """Style that uses ``font`` if given and installed, else the first
    installed font of FONTS. Adds the .ttf files in ``font_dir`` (e.g. the
    static Inter fonts) first."""
    from matplotlib import font_manager
    if font_dir:
        for path in sorted(Path(font_dir).expanduser().rglob('*.[tT][tT][fF]')):
            font_manager.fontManager.addfont(str(path))
    installed = {entry.name for entry in font_manager.fontManager.ttflist}
    if font and font not in installed:
        print(f'warning: font {font!r} is not installed; using {FONTS}')
    fonts = [name for name in dict.fromkeys(([font] if font else []) + list(FONTS))
             if name in installed]
    return FigureStyle(width, size, dpi, tuple(formats), fonts or ['DejaVu Sans'])


def variable(row):
    """Conventional variable (ps, t, q, uv, gps) of a row; '' for other groups."""
    return row[1].split()[0] if row[0] == CONVENTIONAL else ''


def paginate(rows, capacity):
    """Split one group's rows into parts of at most ``capacity`` rows.

    A group, or a conventional variable (ps, t, q, uv, gps), that is too long
    is split into parts of equal size; otherwise conventional variables are
    kept whole and packed in order."""
    blocks = []
    for row in rows:
        if blocks and blocks[-1][0] == variable(row):
            blocks[-1][1].append(row)
        else:
            blocks.append((variable(row), [row]))
    parts = []
    for _, block in blocks:
        size = math.ceil(len(block) / math.ceil(len(block) / capacity))
        for start in range(0, len(block), size):
            chunk = block[start:start + size]
            if parts and len(parts[-1]) + len(chunk) <= capacity:
                parts[-1].extend(chunk)
            else:
                parts.append(list(chunk))
    return parts


def text_color(hex_color):
    red, green, blue = (int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5))
    return 'black' if 0.299 * red + 0.587 * green + 0.114 * blue > 0.6 else 'white'


def x_ticks(columns, resolution, style, cell_w):
    """Tick positions and labels of the time axis, as dense as the labels fit,
    and the columns where a day (cycles) or a year (months) starts."""
    spans = {}
    for j, column in enumerate(columns):
        key = column.date() if resolution == 'cycle' else column.year
        spans.setdefault(key, [j, j])[1] = j + 1
    starts = [first for first, _ in list(spans.values())[1:]]
    if resolution == 'cycle':  # days, centered on their cycles
        need = style.text_width('30') + 0.06
        steps = [[(str(day.day), (first + end) / 2) for day, (first, end) in spans.items()
                  if step == 1 or day.day == 1 or day.day % step == 0]
                 for step in (1, 5, 10, 15)]
    elif len(spans) > 3:  # years, centered on their months
        need = style.text_width('2000') + 0.08
        steps = [[(str(year), (first + end) / 2) for year, (first, end) in spans.items()
                  if year % step == 0] for step in (1, 2, 5, 10, 20, 50)]
    else:  # months
        need = style.text_width('2000-00') + 0.08
        steps = [[(f'{column:%Y-%m}', j + 0.5) for j, column in enumerate(columns)
                  if j % step == 0] for step in (1, 2, 3, 6, 12)]
    for marks in steps:
        if all((b - a) * cell_w >= need for (_, a), (_, b) in zip(marks, marks[1:])):
            break
    else:  # drop labels that would overlap the previous one
        kept = marks[:1]
        for mark in marks[1:]:
            if (mark[1] - kept[-1][1]) * cell_w >= need:
                kept.append(mark)
        marks = kept
    marks = [(text, position) for text, position in marks  # not off the right edge
             if (len(columns) - position) * cell_w + MARGIN >= style.text_width(text) / 2]
    return [position for _, position in marks], [text for text, _ in marks], starts


def source_strip(sources, columns, shorten, fit):
    """Values of a source strip (0 none, 1 and 2 alternating at every change
    of source, 3 mixed) and ``{column: label}`` at each change, where
    ``fit(label, columns)`` cuts the label to the columns of its source up to
    the next change."""
    values, changes, previous, tone = [], [], None, 1
    for j, column in enumerate(columns):
        label = sources.get(column, '')
        if not label:
            values.append(0)
        elif label == 'mixed':
            values.append(3)
        else:
            if label != previous:
                tone = 2 if previous is not None and tone == 1 else 1
                changes.append((j, shorten(label)))
            values.append(tone)
        previous = label or previous
    texts = {}
    for (j, text), end in zip(changes, [j for j, _ in changes[1:]] + [len(columns)]):
        last = max(k for k in range(j, end) if values[k] == values[j])
        text = fit(text, last + 1 - j)
        if text:
            texts[j] = text
    return values, texts


class Chart:
    """The figures of one dashboard page: one per group, or per part of it.

    ``cells`` is ``{(group, name): {column: palette key}}`` in display order,
    ``palette`` is ``{key: (label, color)}`` and ``row_labels`` is
    ``{row: text}``. ``strips`` are drawn above the chart as
    ``[(label, {column: source}, shorten)]``. ``cell_text`` is
    ``{(row, column): text}``; it is drawn in a group only if all of it fits,
    and then ``cell_note`` is added to the axis label.
    """

    def __init__(self, style, cells, columns, resolution, palette, row_labels, *,
                 title, subtitle, info, xlabel, footnote, marks=None, cell_text=None,
                 cell_note='', strips=None):
        self.style, self.cells, self.columns = style, cells, list(columns)
        self.resolution, self.palette, self.title, self.marks = resolution, palette, title, marks
        self.xlabel, self.cell_text, self.cell_note = xlabel, cell_text or {}, cell_note
        self.strips = strips or []
        self.row_labels = {row: row_labels[row] + (f' {MARK}' if marks and marks.get(row)
                                                   else '') for row in cells}
        self.text_w = style.width - 2 * MARGIN
        self.subtitle = style.wrap(subtitle.split(' '), self.text_w, style.size + 1,
                                   weight='bold') if subtitle else []
        self.info = style.wrap(info, self.text_w, sep='  |  ')
        self.footnote = style.wrap(footnote.split(' '), self.text_w)
        self.legend, self.legend_h = self._legend()

    def _legend(self):
        """Entries ``(x, y, indent, color, text)`` in inches from the legend's
        top left, filled column by column, and the legend's height."""
        entries = [(color, label) for label, color in self.palette.values()]
        if self.marks is not None:
            entries.append((None, f'{MARK} after a name: run usage flags differ '
                                  'from build_gsinfo'))
        columns = 2 if self.style.width >= 4.5 else 1
        column_w = self.text_w / columns
        per_column = math.ceil(len(entries) / columns)
        placed, height = [], 0.0
        for c in range(columns):
            y = 0.0
            for color, label in entries[c * per_column:(c + 1) * per_column]:
                indent = SWATCH + SWATCH_GAP if color else 0.0
                lines = self.style.wrap(label.split(' '), column_w - indent - 0.1)
                placed.append((c * column_w, y, indent, color, '\n'.join(lines)))
                y += self.style.lines_height(len(lines)) + 0.03
            height = max(height, y)
        return placed, height

    def _layout(self, rows):
        """Label column, cells and time axis of the figures of one group; its
        parts share them, so their columns line up."""
        style = self.style
        label_w = min(max([style.text_width(self.row_labels[row]) for row in rows]
                          + [style.text_width(label) for label, _, _ in self.strips]
                          + [0.3]), 0.4 * style.width)
        left = MARGIN + label_w + LABEL_GAP
        data_w = style.width - left - MARGIN
        cell_w = data_w / len(self.columns)
        ticks, tick_labels, starts = x_ticks(self.columns, self.resolution, style, cell_w)
        texts = {key: text for key, text in self.cell_text.items() if key[0] in rows}
        if not all(style.text_width(text, style.size - 0.5) <= cell_w - 0.02
                   for text in set(texts.values())):
            texts = {}
        return SimpleNamespace(
            labels={row: style.fit(self.row_labels[row], label_w) for row in rows},
            strip_labels=[style.fit(label, label_w) for label, _, _ in self.strips],
            left=left, data_w=data_w, cell_w=cell_w, ticks=ticks, tick_labels=tick_labels,
            starts=starts, cell_text=texts,
            xlabel=style.wrap((self.xlabel + (f'; {self.cell_note}' if texts and self.cell_note
                                              else '')).split(' '), data_w))

    def _ticks_height(self):
        return self.style.lines_height(1) + 4 / 72

    def _height(self, title_lines, rows, gaps, xlabel_lines):
        style = self.style
        return (MARGIN + style.lines_height(title_lines, style.size + 2, 'bold') + TITLE_GAP
                + style.lines_height(len(self.subtitle), style.size + 1, 'bold')
                + style.lines_height(len(self.info)) + 0.08 + self._ticks_height()
                + (len(self.strips) * style.row_height + 0.05 if self.strips else 0)
                + (rows + gaps * GAP) * style.row_height + self._ticks_height()
                + style.lines_height(xlabel_lines) + 3 / 72 + 0.12
                + self.legend_h + 0.06 + style.lines_height(len(self.footnote)) + MARGIN)

    def _title_lines(self, group, part=1, parts=1, variables=()):
        name = group + (f', part {part} of {parts}' if parts > 1 else '')
        name += f" ({', '.join(variables)})" if variables else ''
        return self.style.wrap(f'{name}: {self.title}'.split(' '), self.text_w,
                               self.style.size + 2, weight='bold')

    def save(self, out, stem, suffix=''):
        """Draw ``<stem>_<group>[_p<part>]<suffix>.<format>`` for every group
        with rows; return the paths written."""
        import matplotlib
        matplotlib.use('Agg')
        written = []
        with matplotlib.rc_context({'font.family': self.style.fonts, 'pdf.fonttype': 42,
                                    'ps.fonttype': 42, 'svg.fonttype': 'none'}):
            for group in GROUPS:
                rows = [row for row in self.cells if row[0] == group]
                if not rows:
                    continue
                conventional = group == CONVENTIONAL
                longest = self._title_lines(group, 99, 99, list(CONVENTIONAL_VARIABLES)
                                            if conventional else ())
                gaps = len(CONVENTIONAL_VARIABLES) - 1 if conventional else 0
                layout = self._layout(rows)
                fixed = self._height(len(longest), 0, gaps, len(layout.xlabel))
                capacity = int((MAX_HEIGHT - fixed) / self.style.row_height)
                if capacity < 1:
                    raise FigureSizeError(
                        f'{self.style.size:g} pt text is too large for a {self.style.width:g} '
                        f'in wide figure: no row fits within {MAX_HEIGHT:g} in; use a '
                        'smaller --font-size or a larger --width')
                parts = paginate(rows, capacity)
                for part, page in enumerate(parts, start=1):
                    variables = (list(dict.fromkeys(map(variable, page)))
                                 if conventional and len(parts) > 1 else ())
                    name = (f'{stem}_{GROUP_FILES[group]}'
                            + (f'_p{part}' if len(parts) > 1 else '') + suffix)
                    written += self._draw(Path(out), name, page, layout, self._title_lines(
                        group, part, len(parts), variables))
        return written

    def _draw(self, out, name, rows, layout, title_lines):
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.colors import ListedColormap
        from matplotlib.patches import Rectangle

        style, width = self.style, self.style.width
        row_h, ncols = style.row_height, len(self.columns)
        x_edges = np.arange(ncols + 1)
        # blocks of rows (conventional variables) separated by gaps
        blocks, top = [], 0.0
        for i, row in enumerate(rows):
            if i and variable(row) != variable(rows[i - 1]):
                top += GAP
            if not i or variable(row) != variable(rows[i - 1]):
                blocks.append([i, i, top + i])
            blocks[-1][1] = i + 1
        span = len(rows) + (len(blocks) - 1) * GAP
        height = self._height(len(title_lines), len(rows), len(blocks) - 1,
                              len(layout.xlabel))
        fig = plt.figure(figsize=(width, height))

        def fy(inches):  # inches from the top -> figure fraction
            return 1 - inches / height

        y = MARGIN
        for lines, size, weight, color in [
                (title_lines, style.size + 2, 'bold', TITLE_COLOR),
                (self.subtitle, style.size + 1, 'bold', TEXT_COLOR),
                (self.info, style.size, 'normal', TEXT_COLOR)]:
            if lines:
                fig.text(MARGIN / width, fy(y), '\n'.join(lines), ha='left', va='top',
                         fontsize=size, fontweight=weight, color=color,
                         linespacing=LINE_SPACING)
                y += style.lines_height(len(lines), size, weight)
            if lines is title_lines:
                y += TITLE_GAP
        y += 0.08 + self._ticks_height()

        def axes(top, count, labeltop):
            ax = fig.add_axes([layout.left / width, fy(top + count * row_h),
                               layout.data_w / width, count * row_h / height])
            ax.set_xlim(0, ncols)
            ax.set_ylim(count, 0)
            ax.set_xticks(layout.ticks)
            ax.set_xticklabels(layout.tick_labels)
            ax.tick_params(axis='x', labelsize=style.size, length=2, width=0.5,
                           color=TEXT_COLOR, pad=1.5, top=labeltop, labeltop=labeltop,
                           bottom=not labeltop, labelbottom=not labeltop)
            ax.tick_params(axis='y', labelsize=style.size, length=0, pad=2)
            for spine in ax.spines.values():
                spine.set_color(TEXT_COLOR)
                spine.set_linewidth(0.5)
            return ax

        if self.strips:
            ax = axes(y, len(self.strips), True)
            for i, (_, sources, shorten) in enumerate(self.strips):
                values, texts = source_strip(
                    sources, self.columns, shorten, lambda text, columns: style.fit(
                        text, columns * layout.cell_w - 0.05, style.size - 0.5,
                        keep='end'))
                ax.pcolormesh(x_edges, [i, i + 1], [values],
                              cmap=ListedColormap(SOURCE_COLORS), vmin=-0.5,
                              vmax=len(SOURCE_COLORS) - 0.5)
                for j, text in texts.items():
                    ax.text(j + 0.03 / layout.cell_w, i + 0.5, text,
                            fontsize=style.size - 0.5, ha='left', va='center',
                            color='black', clip_on=True)
            ax.hlines(np.arange(1, len(self.strips)), 0, ncols, color=TEXT_COLOR,
                      linewidth=0.4)
            ax.set_yticks(np.arange(len(self.strips)) + 0.5)
            ax.set_yticklabels(layout.strip_labels)
            y += len(self.strips) * row_h + 0.05
            ax = axes(y, span, False)
        else:  # time axis labels above and below the chart
            ax = axes(y, span, True)
            ax.tick_params(axis='x', bottom=True, labelbottom=True)
        if len(blocks) > 1:  # frame each block instead of the whole axes
            ax.spines[:].set_visible(False)
        keys = list(self.palette)
        cmap = ListedColormap([color for _, color in self.palette.values()])
        positions = []
        for first, end, top in blocks:
            count = end - first
            ax.pcolormesh(x_edges, top + np.arange(count + 1),
                          [[keys.index(self.cells[row][column]) for column in self.columns]
                           for row in rows[first:end]],
                          cmap=cmap, vmin=-0.5, vmax=len(keys) - 0.5)
            ax.hlines(top + np.arange(1, count), 0, ncols, color=GRID_COLOR, linewidth=0.3)
            ax.vlines(layout.starts, top, top + count, color=RULE_COLOR, linewidth=0.4)
            if len(blocks) > 1:
                ax.add_patch(Rectangle((0, top), ncols, count, fill=False,
                                       edgecolor=TEXT_COLOR, linewidth=0.5, clip_on=False))
            positions += list(top + np.arange(count) + 0.5)
        for row, position in zip(rows, positions):
            for j, column in enumerate(self.columns):
                text = layout.cell_text.get((row, column))
                if text:
                    ax.text(j + 0.5, position, text, ha='center', va='center',
                            fontsize=style.size - 0.5, color=text_color(
                                self.palette[self.cells[row][column]][1]))
        ax.set_yticks(positions)
        ax.set_yticklabels([layout.labels[row] for row in rows], color='black')
        ax.set_xlabel('\n'.join(layout.xlabel), fontsize=style.size, color=TEXT_COLOR,
                      labelpad=2, linespacing=LINE_SPACING)
        y += (span * row_h + self._ticks_height() + style.lines_height(len(layout.xlabel))
              + 3 / 72 + 0.12)

        line_h = style.lines_height(1)
        for x0, y0, indent, color, text in self.legend:
            if color:
                fig.add_artist(Rectangle(
                    ((MARGIN + x0) / width, fy(y + y0 + (line_h + SWATCH) / 2)),
                    SWATCH / width, SWATCH / height, transform=fig.transFigure,
                    facecolor=color, edgecolor=TEXT_COLOR, linewidth=0.4))
            fig.text((MARGIN + x0 + indent) / width, fy(y + y0), text, ha='left',
                     va='top', fontsize=style.size, color='black',
                     linespacing=LINE_SPACING)
        y += self.legend_h + 0.06
        fig.text(MARGIN / width, fy(y), '\n'.join(self.footnote), ha='left', va='top',
                 fontsize=style.size, color=TEXT_COLOR, linespacing=LINE_SPACING)

        paths = []
        for extension in style.formats:
            paths.append(out / f'{name}.{extension}')
            fig.savefig(paths[-1], dpi=style.dpi, facecolor='white')
        plt.close(fig)
        return paths
