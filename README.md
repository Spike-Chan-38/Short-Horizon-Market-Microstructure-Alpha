Spike's Project


# Short-Horizon-Market-Microstructure-Alpha

This project investigates whether L2 order book signals contain economically meaningful short-horizon alpha after realistic transaction costs and execution constraints.

Using full depth (Level 2) order book and trade data, the research constructs microstructure-driven features including microprice, depth-weighted imbalance, order flow imbalance (OFI), spread dynamics, and short-term realized volatility. Labels are defined on future mid-price movements (1–10 second horizon and next price-change event), with strict timestamp alignment to eliminate lookahead bias and event-time vs clock-time comparisons.

Models include:

* Regularized logistic regression (interpretable baseline)
* Gradient-boosted trees (XGBoost / LightGBM)
* Optional lightweight temporal models (LSTM)

Validation is performed using walk-forward splits (no random CV), with evaluation metrics including AUC, Brier score, calibration, information coefficient (IC), and out-of-sample stability.

A custom execution simulator evaluates both:

* Market order strategies (spread + taker fees)
* Passive limit strategies with fill probability and queue modeling

The framework incorporates inventory constraints, turnover tracking, and realistic cost modeling to compute net PnL, Sharpe ratio, drawdown, and capacity estimates.

The final output is structured as a research-grade study linking empirical findings to microstructure theory (order imbalance, adverse selection, queue dynamics), emphasizing economic significance rather than purely predictive accuracy.

**Goal:** Determine whether short-horizon order book signals survive transaction costs under realistic execution assumptions, and under what market regimes the alpha persists or decays.






**TO DO LIST**
Remove after
- Work on Signal_Engineering
- Implement logistic_regression
- 
