"""
Shared Visualization Palette
============================
Single source of truth for topic colours and continuous colorbars across every
project artifact (Matplotlib PNGs, Plotly HTML dashboards, Streamlit views).

The base sequence preserves the original six-colour scheme used by the existing
dashboard so the update does not visually disrupt previously graded charts.

Usage:
    from viz_palette import TOPIC_PALETTE, topic_color, topic_colors
    fig = px.bar(..., color_discrete_map=topic_colors(labels))
"""

from matplotlib.colors import ListedColormap

# Original dashboard sequence, retained as the canonical topic ordering.
TOPIC_PALETTE = [
    "#2A9D8F",  # teal
    "#E76F51",  # burnt orange
    "#264653",  # deep navy
    "#E9C46A",  # gold
    "#8AB17D",  # sage
    "#6D597A",  # plum
]

# Extra colours are only used when a corpus reports more than six components.
TOPIC_PALETTE_EXTENDED = TOPIC_PALETTE + ["#B56576", "#457B9D", "#A8DADC", "#F4A261"]

# Stable label -> colour bindings. Both corpora are covered so a named topic
# keeps the same colour whether it is rendered from the synthetic benchmark or
# from the real Reuters-21578 wire archive.
TOPIC_LABEL_COLORS = {
    # Synthetic benchmark (post-hoc majority-category labels)
    "Clinical Medicine & Genomics": TOPIC_PALETTE[0],
    "Quantum Physics & Advanced Materials": TOPIC_PALETTE[1],
    "AI Infrastructure & Compute": TOPIC_PALETTE[2],
    "Markets, Politics & Regulation": TOPIC_PALETTE[3],
    "Grid Decarbonization & Clean Energy": TOPIC_PALETTE[5],
    "Unused topic (no dominant assignments)": "#BFC9D4",
    # Legacy labels retained for older artifacts / notebook demonstrations
    "Monetary Policy & Markets": TOPIC_PALETTE[3],
    "Antitrust & Regulatory Policy": TOPIC_PALETTE[4],
    "Enterprise Analytics & Benchmarks": TOPIC_PALETTE[5],
    "AI Infrastructure & Cloud Compute": TOPIC_PALETTE[2],
    # Real Reuters-21578 wire (auto-derived labels from PySpark LDA)
    "Mln / Dlrs / Net": TOPIC_PALETTE[0],
    "Company / Shares / Stock": TOPIC_PALETTE[1],
    "Pct / Bank / Rate": TOPIC_PALETTE[2],
    "Trade / Japan / Market": TOPIC_PALETTE[3],
    "Mln / Tonnes / Pct": TOPIC_PALETTE[4],
    "Nil / Prev / Usda": TOPIC_PALETTE[5],
}

# Continuous scales shared by heatmaps and weight-encoded charts.
CONTINUOUS_CMAP_PLOTLY = "Tealgrn"
CONTINUOUS_CMAP_MPL = "YlGnBu"
TOPIC_COLORMAP = ListedColormap(TOPIC_PALETTE)


def topic_color(label, index=None):
    """Return the canonical hex colour for a topic label (or positional fallback)."""
    if label in TOPIC_LABEL_COLORS:
        return TOPIC_LABEL_COLORS[label]
    if index is not None:
        return TOPIC_PALETTE_EXTENDED[index % len(TOPIC_PALETTE_EXTENDED)]
    return TOPIC_PALETTE[0]


def topic_colors(labels):
    """Map an ordered sequence of topic labels to their canonical colours."""
    return [topic_color(label, index) for index, label in enumerate(labels)]


def color_map(labels):
    """Return a Plotly-compatible ``{topic_label: hex_colour}`` mapping."""
    return {label: topic_color(label, index) for index, label in enumerate(labels)}
