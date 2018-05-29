
library(MTS)

phi=matrix(c(0.5,-0.25,-1.0,0.5),2,2)
theta=matrix(c(0.2,-0.1,-0.4,0.2),2,2) 
Sig=diag(2) 
mm=VARMAsim(300,arlags=c(1),malags=c(1),phi=phi,theta=theta,sigma=Sig) 
zt=mm$series[,c(2,1)]
beta=matrix(c(1,0.5),2,1) 
m1=ECMvar(zt,3,ibeta=beta) 

load("~/R/win-library/3.4/GVAR/data/pesaran26.rda")

library(urca)
df <- read.csv("c:/users/benmo/desktop/test1.csv", header=TRUE)


bob <- ca.jo(df, type = c("eigen", "trace"), ecdet = c("none"), K = 2,spec=c("longrun"), season = NULL, dumvar = NULL)
summary(bob)
jim <- cajools(bob, reg.number = NULL)
summary(jim)



Data$test <- ts(df)
colnames(Data$test) <- c('y','x','b','c')





