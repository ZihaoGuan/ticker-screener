from __future__ import annotations

import unittest

from src.webapp.services.insider_fetcher import build_document_candidates, discover_xml_documents_from_index


class InsiderFetcherTests(unittest.TestCase):
    def test_build_document_candidates_prefers_xml_sibling_for_html_primary_document(self) -> None:
        self.assertEqual(
            build_document_candidates("wk-form4_1779142358.html"),
            ["wk-form4_1779142358.html", "wk-form4_1779142358.xml"],
        )

    def test_discover_xml_documents_from_index_extracts_xml_href(self) -> None:
        index_html = """
        <html><body>
          <a href="wk-form4_1779142358.html">FORM 4 html</a>
          <a href="wk-form4_1779142358.xml">FORM 4 xml</a>
          <a href="0002034857-26-000008.txt">txt</a>
        </body></html>
        """
        self.assertEqual(
            discover_xml_documents_from_index(index_html),
            ["wk-form4_1779142358.xml"],
        )


if __name__ == "__main__":
    unittest.main()
