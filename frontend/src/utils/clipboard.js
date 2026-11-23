// Called from a click handler: clipboard access always belongs to the visitor's browser.
function copyWithSelection(text) {
  const previousFocus = document.activeElement;
  const selection = window.getSelection();
  const ranges = selection ? Array.from({ length: selection.rangeCount }, (_, i) => selection.getRangeAt(i).cloneRange()) : [];
  const field = document.createElement("textarea");
  field.value = text;
  field.readOnly = true;
  field.style.cssText = "position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;font-size:16px;";
  document.body.appendChild(field);
  try {
    field.focus({ preventScroll: true });
    field.select();
    field.setSelectionRange(0, field.value.length);
    if (!document.execCommand("copy")) throw new Error("Clipboard copy unavailable");
  } finally {
    field.remove();
    if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
    if (selection) {
      selection.removeAllRanges();
      ranges.forEach((range) => selection.addRange(range));
    }
  }
}

export async function copyText(text) {
  if (window.isSecureContext && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch {
      // Embedded browsers or permission settings may reject even on HTTPS.
    }
  }
  // On HTTP, run synchronously during the click so user activation is retained.
  copyWithSelection(text);
}
