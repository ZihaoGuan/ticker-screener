import { formatCount } from "../lib/format";

type PaginationControlsProps = {
  currentPage: number;
  totalItems: number;
  totalPages: number;
  pageSize: number;
  onPageChange: (page: number) => void;
};

const PAGE_BUTTON_WINDOW = 7;

export function PaginationControls({
  currentPage,
  totalItems,
  totalPages,
  pageSize,
  onPageChange,
}: PaginationControlsProps) {
  const startItem = totalItems === 0 ? 0 : (currentPage - 1) * pageSize + 1;
  const endItem = totalItems === 0 ? 0 : Math.min(currentPage * pageSize, totalItems);
  const pageNumbers = buildPageNumbers(currentPage, totalPages);

  return (
    <div className="scanner-result-pagination">
      <span className="scanner-result-pagination-status">
        Showing {formatCount(startItem)}-{formatCount(endItem)} of {formatCount(totalItems)}
      </span>
      <div className="scanner-result-pagination-actions">
        <button className="ghost-button" type="button" onClick={() => onPageChange(1)} disabled={currentPage <= 1}>
          First
        </button>
        <button className="ghost-button" type="button" onClick={() => onPageChange(Math.max(1, currentPage - 1))} disabled={currentPage <= 1}>
          Prev
        </button>
        <div className="scanner-result-pagination-pages">
          {pageNumbers.map((pageNumber) => (
            <button
              key={pageNumber}
              className={`ghost-button scanner-result-page-button${pageNumber === currentPage ? " is-active" : ""}`}
              type="button"
              onClick={() => onPageChange(pageNumber)}
              disabled={pageNumber === currentPage}
            >
              {pageNumber}
            </button>
          ))}
        </div>
        <button className="ghost-button" type="button" onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))} disabled={currentPage >= totalPages}>
          Next
        </button>
        <button className="ghost-button" type="button" onClick={() => onPageChange(totalPages)} disabled={currentPage >= totalPages}>
          Last
        </button>
      </div>
    </div>
  );
}

function buildPageNumbers(currentPage: number, totalPages: number) {
  if (totalPages <= PAGE_BUTTON_WINDOW) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }
  const halfWindow = Math.floor(PAGE_BUTTON_WINDOW / 2);
  let start = Math.max(1, currentPage - halfWindow);
  let end = Math.min(totalPages, start + PAGE_BUTTON_WINDOW - 1);
  if (end - start + 1 < PAGE_BUTTON_WINDOW) {
    start = Math.max(1, end - PAGE_BUTTON_WINDOW + 1);
  }
  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
}
