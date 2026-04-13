"""Results dashboard metrics: tolerate merged/re-exported CSV rows with explicit nulls."""

import gui


def test_metrics_from_pages_accepts_none_cell_values():
    """csv.DictReader uses None when a column exists but the cell is empty in some exports."""
    rows = [
        {
            "domain": "example.com",
            "http_status": "200",
            "content_kind_guess": None,
            "lang": None,
            "word_count": "100",
            "img_count": None,
            "img_missing_alt_count": None,
            "training_related_flag": None,
        },
    ]
    m = gui._metrics_from_pages(rows)
    assert m["pages"] == 1
    assert m["training_pages"] == 0
