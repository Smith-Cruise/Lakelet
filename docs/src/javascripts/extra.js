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

// Tab strips (`.ll-switch`) on the home page. Delegated on document so the
// handlers survive instant navigation page swaps.

function llSelectTab(tab) {
  var strip = tab.closest(".ll-switch")
  if (!strip) return
  var tabs = Array.prototype.slice.call(strip.querySelectorAll(".ll-switch__tab"))
  tabs.forEach(function (other) {
    var on = other === tab
    other.setAttribute("aria-selected", on ? "true" : "false")
    other.tabIndex = on ? 0 : -1
    var panel = document.getElementById(other.getAttribute("aria-controls"))
    if (panel) panel.hidden = !on
  })
}

document.addEventListener("click", function (ev) {
  var tab = ev.target.closest(".ll-switch__tab")
  if (tab) llSelectTab(tab)
})

document.addEventListener("keydown", function (ev) {
  var tab = ev.target.closest && ev.target.closest(".ll-switch__tab")
  if (!tab) return
  var step = { ArrowLeft: -1, ArrowRight: 1, Home: "first", End: "last" }[ev.key]
  if (step === undefined) return
  var tabs = Array.prototype.slice.call(tab.closest(".ll-switch").querySelectorAll(".ll-switch__tab"))
  var next =
    step === "first"
      ? tabs[0]
      : step === "last"
        ? tabs[tabs.length - 1]
        : tabs[(tabs.indexOf(tab) + step + tabs.length) % tabs.length]
  ev.preventDefault()
  llSelectTab(next)
  next.focus()
})
