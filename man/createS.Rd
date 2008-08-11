\name{createS}
\alias{createS}
\alias{initS}

%- Also NEED an '\alias' for EACH other topic documented here.
\title{Create/Initialize Stack}
\description{
  Create or initialize a stack data structure using \code{filehash}
  databases
}
\usage{
createS(filename)
initS(filename)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{filename}{character, file name for storing the stack data
    structure}
}
\details{
  A new stack can be created using \code{createS}, which creates a file
  for storing the stack information and returns an object of class
  \code{"stack"}.
}
\value{
  The \code{createS} and \code{initS} functions both return an object of
  class \code{"stack"}.
}
\author{Roger D. Peng \email{rpeng@jhsph.edu}}

\keyword{database}
