\name{filehashOption}
\alias{filehashOption}

\title{Set filehash options}
\description{
  Set global filehash options
}
\usage{
filehashOption(...)
}

\arguments{
  \item{\dots}{name-value pairs for options}
}
\details{
  Currently, the only option that can be set is the default database
  type (\code{defaultType}) which can be "DB1", "RDS" or "DB". 
}
\value{
  \code{filehashOptions} returns a list of current settings for all
  options.
}

\author{Roger D. Peng}

\keyword{database}% at least one, from doc/KEYWORDS

