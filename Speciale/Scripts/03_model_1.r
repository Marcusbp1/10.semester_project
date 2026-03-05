



"
The two function u_t and v_t together with the data frame reg_df are created in '02_...' 
We first try and create the function u_t = a + b * v_t + {varepsilon}_t
"

# Run the project's core regression
model_1 <- lm(u_t ~ v_t, data = reg_df)
summary(model_1)


# Plot the relationship
png(filename = paste0("Speciale/plots/01_model_1/", "model_1.png"), width = 800, height = 800)

plot(reg_df$v_t, reg_df$u_t, 
     xlab = "Total Wind Forecast Error (v_t)", 
     ylab = "Price Spread Equilibrium (u_t)",
     main = "Impact of Forecast Errors on Price Spread",
     pch = 20, col = rgb(0, 0, 1, 0.2)) # Using transparency for density
abline(model_1, col = "red", lwd = 2)
dev.off()


"
The plot shows a 'vertical explosion' around zero.

The Center: When the forecast error (v_t) is near zero, the price spread (u_t) is very stable.

The Wings: As the forecast error moves away from zero (even slightly), the variance of the price spread expands massively.

This is exactly what the 'second moment' refers to. The mean impact is small, but the risk (volatility) is huge.
"