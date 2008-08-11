\name{createQ}
\alias{createQ}
\alias{initQ}

%- Also NEED an '\alias' for EACH other topic documented here.
\title{Create/Initialize Queue}
\description{
  Create or initialize a queue data structure using \code{filehash}
  databases
}
\usage{
createQ(filename)
initQ(filename)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{filename}{character, file name for storing the queue data
    structure}
}
\details{
  A new queue can be created using \code{createQ}, which creates a file
  for storing the queue information and returns an object of class
  \code{"queue"}.
}
\value{
  The \code{createQ} and \code{initQ} functions both return an object of
  class \code{"queue"}.
}
\author{Roger D. Peng \email{rpeng@jhsph.edu}}

\keyword{database}
