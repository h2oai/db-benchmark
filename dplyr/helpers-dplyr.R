dplyr.git = function() {
  dcf = read.dcf(system.file("DESCRIPTION", package="dplyr"), fields="RemoteSha")
  toString(dcf[, "RemoteSha"])
}
