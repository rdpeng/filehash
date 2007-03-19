\name{filehashFormats}
\alias{filehashFormats}
\alias{registerFormatDB}

\title{List and register filehash formats}
\description{
  List and register filehash backend database formats.
}
\usage{
registerFormatDB(name, funlist)
filehashFormats(...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{name}{character, name of database format}
  \item{funlist}{list of functions for creating and initializing a
    database format}
  \item{\dots}{list of functions for registering a new database format}
}
\details{
  \code{registerFormatDB} can be used to register new filehash backend
  database formats.  \code{filehashFormats} called with no arguments
  lists information on available formats.
}
\value{
  \code{filehashFormats} returns a list containing information on the
  available filehash formats.
}

\keyword{utilities}% at least one, from doc/KEYWORDS
