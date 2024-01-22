min_max <- function(x, threshold = 3) {
  x[x > threshold] <- threshold
  x[x < -threshold] <- -threshold
  x
}
