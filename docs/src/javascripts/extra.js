// Swap the code copy button to a checkmark for a moment after a click.
// Delegated on document so it survives instant navigation page swaps.
document.addEventListener("click", function (ev) {
  var button = ev.target.closest('.md-code__button[data-md-type="copy"]')
  if (!button) return
  button.classList.add("ll-copied")
  clearTimeout(button.llCopiedTimer)
  button.llCopiedTimer = setTimeout(function () {
    button.classList.remove("ll-copied")
  }, 1500)
})
