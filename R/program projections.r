# http://stackoverflow.com/q/7963393/914686
options(java.parameters = "-Xmx4g") # Free up 4G of memory
library(tidyverse) #avoid java installation on new laptop and instead using tidyverse

data.cip.cred.age <- read_csv("./tmp/input-data.csv")

CIPS <- data.cip.cred.age %>% pull(CIP) %>% unique()
CREDS <- data.cip.cred.age %>% pull(CRED) %>% unique()
AGES <- data.cip.cred.age %>% pull(AGE) %>% unique()

years <- 12 # To predict

cip.groups <- length(CIPS) # Number of CIPs
cred.groups <- length(CREDS) # Number of Credentials
age.groups <- length(AGES) # Number of Age groups

# Extract number of sample points from one of the input data sets
samples <- length(data.cip.cred.age) - 3 # Remove first 3 columns that contain identifiers

#lm.weight.function <- function(x) {10 / (1 + exp(-x))} # Sigmoidal
lm.weight.function <- function(x) {
  x
} # Linear
# lm.weights <- 1:samples # Linear

# Create linear model weights
lm.weights <- lapply(seq(1, samples, length.out = samples), lm.weight.function)

data.lmweights <- lm.weights

lm.model.cip.cred.age <- array(
  0,
  dim = c(cip.groups, cred.groups, age.groups, 2)
)

for (cips in 1:cip.groups) {
  cip <- CIPS[[cips]]

  for (creds in 1:cred.groups) {
    cred <- CREDS[[creds]]

    for (ages in 1:age.groups) {
      age <- AGES[[ages]]

      lm.input <- as.data.frame(t(data.cip.cred.age[
        # Extract time series
        data.cip.cred.age$CIP == cip &
          data.cip.cred.age$CRED == cred &
          data.cip.cred.age$AGE == age,
      ][-1:-3])) # Drop first 3 columns
      lm.input <- cbind(
        # as.numeric(lm.model.cip.cred[[cips]][[creds]]$fitted.values), # CRED
        1:samples, # x
        lm.input
      ) # y values
      colnames(lm.input) <- c(
        # 'cred',
        'x',
        'y'
      )

      lm.model <- lm(
        # Fit a linear, weighted model
        #formula = y ~ cred + x,
        formula = y ~ x,
        data = lm.input,
        weights = unlist(lm.weights)
      )

      lm.model.cip.cred.age[cips, creds, ages, ] <- lm.model$coefficients # Store model coefficients in AGE model list
    }
  }
}
# Clean up workspace
remove(lm.model, lm.input, lm.weights, cip, cred, age, cips, creds, ages)

# Predict future years
# Each prediction follows a hierarchy:
#  1) Total
#  2) CIP
#  3) Credential
#  4) Age
# Care is taken to make sure than application of the respective linear models at any of the above levels
# never imply a negative count (reference the use of max(0,?) below). After every level of the hierarchy
# is complete,
newdata <- (samples + 1):(samples + years)

# First predict the total
# predict.total <- round(
#   unlist(
#     lapply(
#       newdata,
#       function (x) {return (max(0,
#                                 lm.model.total[1] +
#                                   lm.model.total[2] * x))})))

# Now sequentially estimate the lower hierarchical levels.
# predict.cip <- array(0, dim = c(cip.groups, years))
# predict.cip.cred <- array(0, dim = c(cip.groups, cred.groups, years))
predict.cip.cred.age <- array(
  0,
  dim = c(cip.groups, cred.groups, age.groups, years)
)

for (cips in 1:cip.groups) {
  # CIPS

  # predict.cip[cips,] <- unlist(
  #   lapply(
  #     newdata,
  #     function (x) {return (max(0,
  #                               lm.model.cip[cips, 1] +
  #                                 lm.model.cip[cips, 2] * x))}))

  for (creds in 1:cred.groups) {
    # CREDENTIALS

    # predict.cip.cred[cips, creds,] <- unlist(
    #   lapply(
    #     newdata,
    #     function (x) {return (max(0,
    #                               lm.model.cip.cred[cips, creds, 1] +
    #                                 lm.model.cip.cred[cips, creds, 2] * x))}))

    for (ages in 1:age.groups) {
      # AGES

      predict.cip.cred.age[cips, creds, ages, ] <- unlist(
        lapply(
          newdata,
          function(x) {
            return(max(
              0,
              lm.model.cip.cred.age[cips, creds, ages, 1] +
                lm.model.cip.cred.age[cips, creds, ages, 2] * x
            ))
          }
        )
      )
    }
  }
}

# Clean up workspace
remove(newdata, cips, creds, ages)

data_output <- array(
  aperm(predict.cip.cred.age, c(3, 2, 1, 4)),
  c(cip.groups * cred.groups * age.groups, years)
)
write.table(
  data_output,
  "./tmp/output.csv",
  sep = "\t",
  row.names = FALSE
)
#view(data_output)

# Clean workspace
remove(years, samples, data_output, data.cip.cred.age, data.lmweights)
