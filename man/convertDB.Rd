\name{convertDB}
\alias{convertDB}

\title{Convert from old to new 'DB'}
\description{
  Convert 'DB' databases from pre-0.5 'filehash'
}
\usage{
convertDB(name)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{name}{character, database name}
}
\details{
  The format for \code{"DB"} database changed with version 0.5 of
  \code{filehash}. This function can be used to convert database created
  under older versions of the package.
}
\value{
  \code{TRUE} is returned upon success.
}

\keyword{utilities}% at least one, from doc/KEYWORDS
