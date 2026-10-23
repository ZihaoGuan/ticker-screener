# Martin forward-test rotation extension

Mode: Operate. Scope: `/backtests/martin-forward`, new independent RS Expanded eight-slot rotation book only.

## Direction contract

THESIS: Explain the automatic rotation decision and its capital dependency without changing existing trading books or disguising experimental rules as guru rules.

OWN-WORLD: Inherit the existing dark dashboard, Hanken Grotesk/system typography, restrained green accent, semantic panels, definition lists, wrapped book buttons and horizontally scrollable tables. An amber dashed chart series distinguishes the new book.

STORY: Choose the new book, read its saved decision and distinct stop-risk/giveback measures, inspect outgoing/incoming event history, and compare matching sessions rather than assume isolated rotation outperformance.

FIRST VIEWPORT: Retain the page header and book switch. The new book gets a compact rule disclosure, then a readable rotation-status panel above the unchanged candidate lifecycle. No manual broker or approval controls.

FORM: Narrow extension of the incumbent Operate surface; no concept seed or new visual world. Saved data drives all status and empty-state copy.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance

Existing PRODUCT.md and DESIGN.md are absent. This extension does not invent either file or a replacement design system. Context launcher was unavailable; verification and remaining limitations are reported in the implementation handoff.

## Finish evidence

Disposition: ship within the reviewed Today-view screenshot and code scope. This is an inherited surface extension; no visual-system changes were approved. The generic FINISH reference to DESIGN.md does not require a global design-system extraction for this scoped extension.

The frontend diff retains the incumbent page shell, dark surface/border/text/accent tokens, typography, wrapped book buttons, panel and disclosure styles, definition-list grid, lifecycle controls and horizontally scrollable tables. The rotation status reuses `martin-panel`, `martin-details`, `martin-panel-copy` and `SimpleTable`. Existing responsive rules supply the two-column mobile details and lifecycle layout. The only added CSS is the amber dashed comparison series and its matching legend marker; no existing token or component style is changed.

Reviewed local fixture captures: `/tmp/martin-rotation-desktop.png` and `/tmp/martin-rotation-mobile.png`. Both show the new book selected in Today, the rotation decision and separate risk/exposure figures above the candidate lifecycle, and the inherited responsive layout. The mobile capture shows tables clipped within their intended horizontal scroll containers; a still image alone does not establish scroll interaction. The new chart series and Performance view were checked in code, not in these captures.

The implementation handoff reports passing local fixtures and frontend build. This documentation closeout inspected the frontend diff and both captures; it did not rerun those checks, exercise a browser, or verify production data, scheduled jobs, broker behavior or device interaction. No shipping raster assets were introduced. PRODUCT.md and DESIGN.md remain absent and no global design-system files were created or overwritten.
