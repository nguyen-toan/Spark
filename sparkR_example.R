#
# This is an example of using SparkR
#
# Reference: http://www.r-bloggers.com/sparkr-preview-by-vincent-warmerdam/
#

library(magrittr)
library(SparkR)

sc <- sparkR.init(master="local")

# An example
sc %>% 
  parallelize(1:100000) %>%
  count

nums <- runif(100000) * 10

# A more elaborate example
sc %>% 
  parallelize(nums) %>% 
  map(function(x) round(x)) %>%
  filterRDD(function(x) x %% 2) %>% 
  map(function(x) list(x, 1)) %>%
  reduceByKey(function(x,y) x + y, 1L) %>% 
  collect

# Bootstrapping with Spark
sample_cw <- function(n, s){
  set.seed(s)
  ChickWeight[sample(nrow(ChickWeight), n), ]
}

data_rdd <- sc %>% 
  parallelize(1:200, 20) %>% 
  map(function(s) sample_cw(250, s))

# estimate the mean of the weight
data_rdd %>% 
  map(function(x) mean(x$weight)) %>% 
  collect %>% 
  as.numeric %>% 
  hist(20, main="mean weight, bootstrap samples")

# perform bootstrapped regressions
train_lm <- function(data_in){
  lm(data=data_in, weight ~ Time)
}

coef_rdd <- data_rdd %>% 
  map(train_lm) %>% 
  map(function(x) x$coefficients) 

get_coef <- function(k){
  coef_rdd %>% 
    map(function(x) x[k]) %>% 
    collect %>%
    as.numeric
}

df <- data.frame(intercept = get_coef(1), time_coef = get_coef(2))
df$intercept %>% hist(breaks = 30, main="beta coef for intercept")
df$time_coef %>% hist(breaks = 30, main="beta coef for time")
