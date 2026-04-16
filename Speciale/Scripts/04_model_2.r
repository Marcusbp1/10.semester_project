

library(rugarch)


# 1. Define the model specification
spec <- ugarchspec(
  variance.model = list(
    model = "gjrGARCH", 
    garchOrder = c(1, 1),
    external.regressors = matrix(v_t) # Impact on Volatility
  ),
  mean.model = list(
    armaOrder = c(1, 0),             # AR(1) to handle serial correlation
    include.mean = TRUE,
    external.regressors = matrix(v_t) # Impact on the Spread itself
  ),
  distribution.model = "nig"         # Normal-inverse Gaussian distribution
)
#  normal-inverse Gaussian distribution

length(u_t)
# 2. Estimate the model
garch_fit <- ugarchfit(spec = spec, data = u_t, solver = "hybrid")

# 3. View the results
print(garch_fit)



# Plot the diagnostic graphs
png(filename = paste0("Speciale/plots/04_model_2/", "model_2.png"), width = 800, height = 800)
plot(garch_fit, which = "all")
dev.off()


# Define the descriptive names for the plots to make your folders organized
plot_names <- c("Series_with_2SD", "Series_with_VaR", "Conditional_SD", 
                "ACF_Observations", "ACF_Squared_Observations", "ACF_Absolute_Observations", 
                "CrossCorr_Squared_vs_Actual", "Empirical_Density", "QQ_Plot", 
                "ACF_Std_Residuals", "ACF_Squared_Std_Residuals", "News_Impact_Curve")

# Loop through all 12 available plots
for (i in 1:12) {
    # Generate a clean filename using the index and the descriptive name
    file_name <- sprintf("Speciale/plots/04_model_2/%02d_%s.png", i, plot_names[i])
    
    png(filename = file_name, width = 800, height = 600, res = 120)
    
    # plot() with 'which' allows selecting the specific diagnostic
    plot(garch_fit, which = i)
    
    dev.off()
}

# Check for remaining ARCH effects (Should be > 0.05)
# This is the Weighted Ljung-Box Test on Standardized Squared Residuals
#infocriteria(garch_fit)


