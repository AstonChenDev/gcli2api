// Log retention and cleanup can remove a page between two reads.
export function applyLogPage(data, page, pageSize, setPage, showPage) {
  const lastPage = Math.max(1, Math.ceil(data.total / pageSize));
  if (page > lastPage) {
    setPage(lastPage);
    return;
  }
  showPage(data);
}
