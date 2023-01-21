##########################################################################
## Copyright (C) 2006-2023, Roger D. Peng <roger.peng @ austin.utexas.edu>
##     
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
## 02110-1301, USA
##########################################################################

.onLoad <- function(lib, pkg) {
        assign("defaultType", "DB1", .filehashOptions)

        for(type in c("DB1", "RDS")) {
                cname <- paste("create", type, sep = "")
                iname <- paste("initialize", type, sep = "")
                r <- list(create = get(cname, mode = "function"),
                          initialize = get(iname, mode="function"))
                assign(type, r, .filehashFormats)
        }
}

.onAttach <- function(lib, pkg) {
        dcf <- read.dcf(file.path(lib, pkg, "DESCRIPTION"))
        msg <- gettextf("%s: %s (%s)", dcf[, "Package"], dcf[, "Title"],
                        as.character(dcf[, "Version"]))
        packageStartupMessage(paste(strwrap(msg), collapse = "\n"))
}

.filehashOptions <- new.env()

.filehashFormats <- new.env()
