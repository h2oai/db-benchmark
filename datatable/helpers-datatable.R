datatable.git = function() {
  dcf = read.dcf(system.file("DESCRIPTION", package="data.table"), fields="Revision")
  toString(dcf[, "Revision"])
}
