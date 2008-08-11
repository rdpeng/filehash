\name{stack-class}
\docType{class}
\alias{stack-class}
\alias{isEmpty,stack-method}
\alias{mpush,stack-method}
\alias{pop,stack-method}
\alias{push,stack-method}
\alias{show,stack-method}
\alias{top,stack-method}

\title{Class "stack"}
\description{A stack implementation using a \code{filehash} database}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("stack", ...)} or
by calling \code{createS}.  Existing queues can be initialized with
\code{initS}.
}
\section{Slots}{
  \describe{
    \item{\code{stack}:}{Object of class \code{"filehashDB1"}}
    \item{\code{name}:}{Object of class \code{"character"}: the name of
      the stack (default is the file name in which the stack data are
      stored)}
  }
}
\section{Methods}{
  \describe{
    \item{isEmpty}{\code{signature(db = "stack")}: returns
      \code{TRUE}/\code{FALSE} depending on whether there are elements
      in the stack.}
    \item{pop}{\code{signature(db = "stack")}: returns the value of the
      top of the stack and subsequently removes that
      element from the stack; an error is signaled if the stack is empty}
    \item{push}{\code{signature(db = "stack")}: adds an element to the
      top of the stack}
    \item{show}{\code{signature(object = "stack")}: prints the name of
      the stack}
    \item{top}{\code{signature(db = "stack")}: returns the value of the
      top of the stack; an error is signaled if the stack
      is empty}
    \item{mpush}{\code{signature(db = "stack")}: works like \code{push}
      except it can push multiple objects in a list on to the stack}
  }
}
\author{Roger D. Peng \email{rpeng@jhsph.edu}}

\examples{
showClass("stack")
}
\keyword{classes}
